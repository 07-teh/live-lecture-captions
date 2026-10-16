import base64
import asyncio
import io
import ipaddress
import json
import os
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import wave
from collections import deque
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse


CURRENT_DIR = Path(__file__).resolve().parent
load_dotenv(CURRENT_DIR / ".env")

HOST = "127.0.0.1"
PORT = 8081
# Only the page this server serves may talk to it. Any website open in the same browser can send
# requests to 127.0.0.1, so CORS alone is not enough: Host stops DNS rebinding, Origin stops other sites.
ALLOWED_HOSTS = {f"127.0.0.1:{PORT}", f"localhost:{PORT}"}
ALLOWED_ORIGINS = {f"http://127.0.0.1:{PORT}", f"http://localhost:{PORT}"}
STT_API_KEY = os.getenv("STT_API_KEY", os.getenv("DEEPGRAM_API_KEY", "")).strip()
STT_MODEL = os.getenv("STT_MODEL", "nova-3").strip()
STT_API_URL = os.getenv("STT_API_URL", "https://api.deepgram.com/v1/listen").strip()
STT_LANGUAGE = os.getenv("STT_LANGUAGE", "en")
STT_SAMPLE_RATE = int(os.getenv("STT_SAMPLE_RATE", "16000"))
STT_STREAM_API_URL = os.getenv("STT_STREAM_API_URL", "").strip()
STT_ENDPOINTING_MS = int(os.getenv("STT_ENDPOINTING_MS", "300"))
STT_UTTERANCE_END_MS = int(os.getenv("STT_UTTERANCE_END_MS", "1000"))
STT_STREAM_CLOSE_WAIT_SECONDS = 5
STT_BUFFER_SECONDS = 600          # audio kept for replay while Deepgram is unreachable
STT_BURST_MAX_SECONDS = 3         # longer backlogs go through the pre-recorded API; the stream only replays at ~1x
STT_JOIN_OVERLAP_SECONDS = 3.0    # a new stream re-hears this much already-transcribed audio, for context
STT_JOIN_TRIM_SECONDS = 0.6       # words this close to the end of a backfill clip are left to the stream
STT_WATCHDOG_SECONDS = 12
STT_IDLE_CLOSE_SECONDS = 30
STT_BATCH_TIMEOUT_SECONDS = 60
STT_STOP_GIVEUP_SECONDS = 120     # after Stop, keep trying this long to transcribe what is still buffered
STT_SESSION_GRACE_SECONDS = 600
STT_BYTES_PER_SECOND = STT_SAMPLE_RATE * 2
STT_DEBUG_FAULTS = os.getenv("HAHAHA_DEBUG_FAULTS", "").strip().lower() in {"1", "true", "yes"}
STT_KEYTERMS = [
    item.strip()
    for item in re.split(r"[,;\n]+", os.getenv("STT_KEYTERMS", ""))
    if item.strip()
][:100]

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()
DEEPSEEK_API_URL = (os.getenv("DEEPSEEK_API_URL", "").strip() or "https://api.deepseek.com").rstrip("/")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "").strip() or "deepseek-v4-flash"
DEEPSEEK_TEMPERATURE = float(os.getenv("DEEPSEEK_TEMPERATURE", "1.3"))   # DeepSeek's own recommendation for translation
DEEPSEEK_TIMEOUT_SECONDS = 20
TRANSLATE_MAX_WORDS = 28          # a sentence longer than this is cut at a clause boundary
TRANSLATE_CONTEXT_SENTENCES = 3
TRANSLATE_CONCURRENCY = 3
BALANCE_CACHE_SECONDS = 60
COURSE_TERM_SOURCES = ["manual", "slides", "auto"]
COURSE_TERM_STORE_LIMIT = 80
COURSE_KEYTERM_LIMIT = 40
COURSE_KEYTERM_WORD_BUDGET = 90
COURSE_LEARN_MIN_WORDS = 150
COURSE_IMPORT_MAX_BYTES = 30 * 1024 * 1024
COURSE_IMPORT_MAX_PAGES = 400
COURSE_IMPORT_MAX_CHARS = 400_000

AUTH_TOKEN = os.getenv("HAHAHA_AUTH_TOKEN", "").strip()
AUTH_ENABLED = bool(AUTH_TOKEN)
HISTORY_DB_PATH = Path(os.getenv("HISTORY_DB_PATH", "").strip() or str(CURRENT_DIR / "hahahav2_history.sqlite3"))
HISTORY_DEFAULT_LIMIT = 50
HISTORY_MAX_LIMIT = 200

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_history_db()
    janitor = asyncio.create_task(expire_abandoned_stt_sessions())
    yield
    janitor.cancel()


class LocalPageOnlyMiddleware:
    """Rejects HTTP and WebSocket requests whose Host or Origin is not this server's own page.
    Requests without an Origin header (same-origin GETs, curl) are allowed."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] in ("http", "websocket"):
            headers = dict(scope["headers"])
            host = headers.get(b"host", b"").decode("latin-1").strip().lower()
            origin = headers.get(b"origin", b"").decode("latin-1").strip().lower()
            if host not in ALLOWED_HOSTS or (origin and origin not in ALLOWED_ORIGINS):
                if scope["type"] == "http":
                    await PlainTextResponse("Forbidden.", status_code=403)(scope, receive, send)
                else:
                    await send({"type": "websocket.close", "code": 1008})
                return
        await self.app(scope, receive, send)


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=sorted(ALLOWED_ORIGINS),
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(LocalPageOnlyMiddleware)   # added last, so it runs first


CJK_CHAR_RE = re.compile(r"[\u3400-\u9FFF\u3040-\u30FF]")


def get_client_id_from_websocket(websocket: WebSocket) -> str:
    query_client_id = (websocket.query_params.get("client_id") or "").strip()
    if query_client_id:
        return query_client_id
    header_client_id = (websocket.headers.get("x-client-id") or "").strip()
    if header_client_id:
        return header_client_id
    if websocket.client and websocket.client.host:
        return websocket.client.host
    return "anonymous"


def require_auth(request: Request):
    if not AUTH_ENABLED:
        return
    bearer = (request.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
    token = bearer or (request.headers.get("X-HAHAHA-Token") or "").strip()
    if token != AUTH_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized.")


def require_history_access(request: Request):
    require_auth(request)
    if AUTH_ENABLED:
        return
    host = request.client.host if request.client else ""
    try:
        is_local = ipaddress.ip_address(host).is_loopback
    except ValueError:
        is_local = host == "localhost"
    if not is_local:
        raise HTTPException(status_code=403, detail="History is available only on this Mac unless authentication is enabled.")


def history_connection():
    connection = sqlite3.connect(HISTORY_DB_PATH, timeout=5)
    connection.row_factory = sqlite3.Row
    return connection


def init_history_db():
    with history_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS history_sessions (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                preview TEXT NOT NULL DEFAULT '',
                transcript TEXT NOT NULL DEFAULT '',
                translation TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                src_lang TEXT NOT NULL DEFAULT '',
                target_lang TEXT NOT NULL DEFAULT '',
                scene TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS history_sessions_updated_idx ON history_sessions(updated_at DESC)"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS course_terms (
                course TEXT PRIMARY KEY,
                terms TEXT NOT NULL DEFAULT '[]',
                updated_at INTEGER NOT NULL
            )
            """
        )


def clean_history_item(data: dict, session_id: str):
    transcript = str(data.get("transcript") or "")
    return {
        "id": session_id[:160],
        "title": str(data.get("title") or "Untitled").strip()[:240] or "Untitled",
        "preview": transcript,
        "transcript": transcript,
        "src_lang": str(data.get("srcLang") or data.get("src_lang") or "")[:40],
        "scene": str(data.get("scene") or "")[:400],
        # Sentence pairs live in the old `translation` column as JSON, so no migration is needed.
        "translation": json.dumps([
            {"en": str(segment.get("en") or ""), "zh": str(segment.get("zh") or "")}
            for segment in (data.get("segments") or []) if isinstance(segment, dict)
        ], ensure_ascii=False),
    }


def upsert_history_item(data: dict, session_id: str):
    item = clean_history_item(data, session_id)
    now = int(time.time() * 1000)
    with history_connection() as connection:
        latest = connection.execute("SELECT MAX(updated_at) AS updated_at FROM history_sessions").fetchone()
        if latest and latest["updated_at"] is not None:
            now = max(now, latest["updated_at"] + 1)
        existing = connection.execute(
            "SELECT created_at, translation FROM history_sessions WHERE id = ?", (item["id"],)
        ).fetchone()
        created_at = existing["created_at"] if existing else now
        # Sessions from the old translation feature keep plain text in this column; a save
        # without sentence pairs (a rename, say) must not wipe it.
        if existing and item["translation"] == "[]" and not existing["translation"].lstrip().startswith("["):
            item["translation"] = existing["translation"]
        connection.execute(
            """
            INSERT INTO history_sessions (
                id, title, preview, transcript, translation, src_lang, scene, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                preview = excluded.preview,
                transcript = excluded.transcript,
                translation = excluded.translation,
                src_lang = excluded.src_lang,
                scene = excluded.scene,
                updated_at = excluded.updated_at
            """,
            (
                item["id"], item["title"], item["preview"], item["transcript"], item["translation"],
                item["src_lang"], item["scene"], created_at, now,
            ),
        )
    return history_item_by_id(item["id"])


def parse_history_segments(raw: str) -> list[dict]:
    # Rows saved before sentence pairs existed hold plain text here, not JSON.
    with suppress(ValueError, TypeError):
        segments = json.loads(raw or "[]")
        if isinstance(segments, list):
            return [segment for segment in segments if isinstance(segment, dict)]
    return []


def history_row_to_dict(row):
    if not row:
        return None
    return {
        "id": row["id"],
        "title": row["title"],
        "preview": row["preview"],
        "transcript": row["transcript"],
        "segments": parse_history_segments(row["translation"]),
        "srcLang": row["src_lang"],
        "scene": row["scene"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
    }


def history_item_by_id(session_id: str):
    with history_connection() as connection:
        row = connection.execute(
            """SELECT id, title, preview, transcript, translation, src_lang, scene, created_at, updated_at
               FROM history_sessions WHERE id = ?""",
            (session_id,),
        ).fetchone()
    return history_row_to_dict(row)


def decode_pcm_base64_bytes(encoded: str):
    if not encoded:
        return b""
    return base64.b64decode(encoded.encode("ascii"))


def contains_cjk(text: str):
    return bool(CJK_CHAR_RE.search(text or ""))


def should_insert_space(left_text: str, right_text: str):
    left = (left_text or "").rstrip()
    right = (right_text or "").lstrip()
    if not left or not right:
        return False

    left_char = left[-1]
    right_char = right[0]
    if left_char.isspace() or right_char.isspace():
        return False
    if contains_cjk(left_char) or contains_cjk(right_char):
        return False
    if left_char in "([{\\\"'“‘/" or right_char in ".,!?;:)]}\\\"'”’/%":
        return False
    return True


def join_text_segments(*segments: str):
    output = ""
    for segment in segments:
        piece = (segment or "").strip()
        if not piece:
            continue
        if not output:
            output = piece
        elif should_insert_space(output, piece):
            output = f"{output} {piece}"
        else:
            output = f"{output}{piece}"
    return output.strip()


def derive_deepgram_stream_base_url():
    base_url = STT_STREAM_API_URL or STT_API_URL or "https://api.deepgram.com/v1/listen"
    parsed = urllib.parse.urlparse(base_url)
    scheme = "wss" if parsed.scheme in {"http", "https", ""} else parsed.scheme
    netloc = parsed.netloc or "api.deepgram.com"
    path = parsed.path or "/v1/listen"
    return urllib.parse.urlunparse((scheme, netloc, path, "", "", ""))


def derive_deepgram_rest_url():
    parsed = urllib.parse.urlparse(STT_API_URL or "https://api.deepgram.com/v1/listen")
    scheme = "https" if parsed.scheme in {"ws", "wss", ""} else parsed.scheme
    return urllib.parse.urlunparse((scheme, parsed.netloc or "api.deepgram.com", parsed.path or "/v1/listen", "", "", ""))


def build_deepgram_stream_url(keyterms: list[str]):
    params = [
        ("model", STT_MODEL),
        ("language", STT_LANGUAGE),
        ("encoding", "linear16"),
        ("sample_rate", str(STT_SAMPLE_RATE)),
        ("channels", "1"),
        ("interim_results", "true"),
        ("smart_format", "true"),
        ("punctuate", "true"),
        ("endpointing", str(STT_ENDPOINTING_MS)),
        ("utterance_end_ms", str(STT_UTTERANCE_END_MS)),
    ]
    for keyterm in keyterms:
        params.append(("keyterm", keyterm))
    return f"{derive_deepgram_stream_base_url()}?{urllib.parse.urlencode(params)}"


class STTFatalError(RuntimeError):
    pass


def seconds_to_bytes(seconds: float) -> int:
    return int(round(seconds * STT_SAMPLE_RATE)) * 2


def words_to_text(words: list[dict]) -> str:
    return " ".join((word.get("punctuated_word") or word.get("word") or "").strip() for word in words).strip()


def raise_if_deepgram_rejected(exc: Exception):
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None) or getattr(exc, "status_code", None) or getattr(exc, "code", None)
    if status in (401, 402, 403):
        raise STTFatalError(
            f"Deepgram rejected the API key or the account has no credit (HTTP {status})."
        ) from exc


async def connect_deepgram_stream(keyterms: list[str]):
    url = build_deepgram_stream_url(keyterms)
    options = {"ping_interval": None, "close_timeout": 5, "max_size": 2_000_000}
    headers = {"Authorization": f"Token {STT_API_KEY}"}
    try:
        return await websockets.connect(url, additional_headers=headers, **options)
    except TypeError:
        return await websockets.connect(url, extra_headers=headers, **options)


def transcribe_prerecorded_sync(pcm: bytes, keyterms: list[str]) -> list[dict]:
    """Transcribe a finished clip with Deepgram's pre-recorded API; returns timestamped words."""
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(STT_SAMPLE_RATE)
        wav_file.writeframes(pcm)
    params = [
        ("model", STT_MODEL),
        ("language", STT_LANGUAGE),
        ("smart_format", "true"),
        ("punctuate", "true"),
    ]
    for keyterm in keyterms:
        params.append(("keyterm", keyterm))
    request = urllib.request.Request(
        f"{derive_deepgram_rest_url()}?{urllib.parse.urlencode(params)}",
        data=buffer.getvalue(),
        headers={"Authorization": f"Token {STT_API_KEY}", "Content-Type": "audio/wav"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=STT_BATCH_TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    channels = (payload.get("results") or {}).get("channels") or []
    alternatives = (channels[0].get("alternatives") if channels else None) or []
    return (alternatives[0].get("words") if alternatives else None) or []


SENTENCE_END_RE = re.compile(r"""(?<=[.!?])["')\]]*\s+(?=["'(\[]?[A-Z0-9])""")
ABBREVIATION_RE = re.compile(r"\b(?:Mr|Mrs|Ms|Dr|Prof|Sr|Jr|St|vs|etc|e\.g|i\.e|Fig|No|approx)\.$", re.IGNORECASE)
CLAUSE_END_RE = re.compile(r"[,;:]\s+")

TRANSLATION_SYSTEM_PROMPT = (
    "You are a simultaneous interpreter for a university lecture given in English. "
    "Translate the sentence into natural, concise Simplified Chinese that a student can read at a glance. "
    "Output only the translation, with no notes and no quotation marks. "
    "Translate technical terms into their standard Chinese terms (public key → 公钥, decrypt → 解密). "
    "Keep only acronyms, code, formulas and people's names in English. "
    "The text comes from live speech recognition: it may be a fragment or contain a misheard word. "
    "Translate what is there, fix an obvious mishearing silently if the context makes it clear, and never add content."
)


def split_sentences(text: str) -> tuple[list[str], str]:
    """Split into complete sentences plus the unfinished remainder."""
    sentences: list[str] = []
    start = 0
    for match in SENTENCE_END_RE.finditer(text):
        candidate = text[start:match.start()].strip()
        if not candidate or ABBREVIATION_RE.search(candidate):
            continue
        sentences.append(candidate)
        start = match.end()
    rest = text[start:].strip()
    if rest and rest[-1] in ".!?" and not ABBREVIATION_RE.search(rest):
        sentences.append(rest)
        rest = ""
    return sentences, rest


def normalize_sentence(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def deepseek_stream_sync(messages: list[dict], on_delta, max_tokens: int = 400):
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": messages,
        "temperature": DEEPSEEK_TEMPERATURE,
        "max_tokens": max_tokens,
        "stream": True,
    }
    request = urllib.request.Request(
        f"{DEEPSEEK_API_URL}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=DEEPSEEK_TIMEOUT_SECONDS) as response:
        for raw_line in response:
            line = raw_line.decode("utf-8", errors="ignore").strip()
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                break
            choices = json.loads(data).get("choices") or []
            delta = ((choices[0].get("delta") or {}).get("content") if choices else None) or ""
            if delta:
                on_delta(delta)


def describe_api_error(exc: Exception) -> str:
    if isinstance(exc, urllib.error.HTTPError):
        with suppress(Exception):
            detail = json.loads(exc.read().decode("utf-8", errors="ignore"))
            message = (detail.get("error") or {}).get("message") or detail.get("err_msg") or detail.get("message")
            if message:
                return f"HTTP {exc.code}: {message}"
        return f"HTTP {exc.code}"
    return str(exc) or exc.__class__.__name__


TERM_EXTRACTION_PROMPT = (
    "You help a speech recognizer follow a university course. From the text, list the domain-specific "
    "terms, acronyms and names (people, algorithms, protocols, products, places) that a general speech "
    "recognizer is likely to mishear. Rules: write each one with its correct standard spelling even if "
    "the text spells it wrong; 1 to 4 words each; no everyday words; no duplicates; at most 25 items, "
    'most important first. Reply with JSON only: {"terms": ["..."]}'
)


def normalize_course(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip())[:120]


def load_course_terms(course: str) -> list[dict]:
    with history_connection() as connection:
        row = connection.execute("SELECT terms FROM course_terms WHERE course = ?", (course,)).fetchone()
    with suppress(ValueError, TypeError):
        terms = json.loads(row["terms"]) if row else []
        return [term for term in terms if isinstance(term, dict) and term.get("term")]
    return []


def save_course_terms(course: str, terms: list[dict]):
    with history_connection() as connection:
        connection.execute(
            """INSERT INTO course_terms (course, terms, updated_at) VALUES (?, ?, ?)
               ON CONFLICT(course) DO UPDATE SET terms = excluded.terms, updated_at = excluded.updated_at""",
            (course, json.dumps(terms[:COURSE_TERM_STORE_LIMIT], ensure_ascii=False), int(time.time() * 1000)),
        )


def clean_term(term) -> str:
    term = re.sub(r"\s+", " ", str(term or "")).strip(" \t\n.,;:\"'")
    return term if 2 <= len(term) <= 60 and len(term.split()) <= 4 else ""


def merge_course_terms(existing: list[dict], found: list[str], source: str) -> list[dict]:
    """Terms the user typed or imported from slides outrank learned ones; learned ones rank by how often they recur."""
    by_key = {term["term"].lower(): dict(term) for term in existing}
    for raw in found:
        term = clean_term(raw)
        if not term:
            continue
        entry = by_key.setdefault(term.lower(), {"term": term, "source": source, "count": 0})
        entry["count"] = int(entry.get("count") or 0) + 1
        if COURSE_TERM_SOURCES.index(source) < COURSE_TERM_SOURCES.index(entry.get("source") or "auto"):
            entry["source"] = source
    return sorted(
        by_key.values(),
        key=lambda entry: (COURSE_TERM_SOURCES.index(entry.get("source") or "auto"), -int(entry.get("count") or 0)),
    )


def course_keyterms(course: str) -> list[str]:
    """Keyterms for Deepgram: it allows about 500 tokens in total, so stay well inside that."""
    terms, words = [], 0
    for term in [*STT_KEYTERMS, *(entry["term"] for entry in load_course_terms(course))]:
        # A run of Chinese has no spaces but costs about a token per character.
        size = len(re.findall(r"[\u3400-\u9FFF\u3040-\u30FF]|[^\s\u3400-\u9FFF\u3040-\u30FF]+", term))
        if term.lower() in {item.lower() for item in terms} or words + size > COURSE_KEYTERM_WORD_BUDGET:
            continue
        terms.append(term)
        words += size
        if len(terms) >= COURSE_KEYTERM_LIMIT:
            break
    return terms


def deepseek_json_sync(system: str, user: str, max_tokens: int = 900) -> dict:
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "temperature": 0.2,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
        "stream": False,
    }
    request = urllib.request.Request(
        f"{DEEPSEEK_API_URL}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        content = json.loads(response.read().decode("utf-8"))["choices"][0]["message"]["content"]
    return json.loads(content)


def learn_course_terms_sync(course: str, text: str, source: str) -> list[dict]:
    """Ask DeepSeek which terms in the text matter, and fold them into the course's list."""
    if len(text) > 12000:
        # Long lectures: the start, middle and end cover the vocabulary well enough.
        middle = len(text) // 2
        text = "\n...\n".join([text[:4000], text[middle - 2000:middle + 2000], text[-4000:]])
    found = deepseek_json_sync(TERM_EXTRACTION_PROMPT, text).get("terms") or []
    terms = merge_course_terms(load_course_terms(course), [str(term) for term in found if term], source)
    save_course_terms(course, terms)
    return terms


BACKGROUND_TASKS: set[asyncio.Task] = set()


def learn_course_terms_later(course: str, text: str):
    """After a recording, learn its vocabulary so the next class of this course is recognized better."""
    if not (DEEPSEEK_API_KEY and course and len(text.split()) >= COURSE_LEARN_MIN_WORDS):
        return

    async def learn():
        try:
            terms = await asyncio.to_thread(learn_course_terms_sync, course, text, "auto")
            print(f"Course terms for {course!r}: {len(terms)}", flush=True)
        except Exception as exc:
            print(f"Could not learn course terms for {course!r}: {describe_api_error(exc)}", flush=True)

    task = asyncio.create_task(learn())
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)


def extract_document_text(filename: str, data: bytes) -> str:
    if filename.lower().endswith(".pdf"):
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(data))
        pages, size = [], 0
        for page in reader.pages[:COURSE_IMPORT_MAX_PAGES]:
            pages.append(page.extract_text() or "")
            size += len(pages[-1])
            if size > COURSE_IMPORT_MAX_CHARS:
                break
        return "\n".join(pages)
    return data.decode("utf-8", errors="ignore")


class LiveTranslator:
    """Cuts the live transcript into sentences and translates each one as soon as it is complete.

    A sentence that is already complete in the interim transcript is translated early (a
    "speculative" unit) so the Chinese shows up before Deepgram finalizes the audio; when the
    final text arrives it either confirms the unit or replaces it.
    """

    def __init__(self, session: "STTSession"):
        self.session = session
        self.enabled = False                    # sentences are always cut; DeepSeek is only called when on
        self.units: list[dict] = []
        self.pending = ""                       # final text not yet turned into a confirmed unit
        self.speculative: deque[int] = deque()  # unit ids shown early, waiting for the final text
        self.semaphore = asyncio.Semaphore(TRANSLATE_CONCURRENCY)
        self.tasks: set[asyncio.Task] = set()
        self.error_reported = False

    def tail(self, preview: str = "") -> str:
        """Text that is on screen but not yet inside a unit."""
        sentences, rest = split_sentences(join_text_segments(self.pending, preview))
        return join_text_segments(*sentences[len(self.speculative):], rest)

    async def set_unit(self, unit_id: int | None, text: str, final: bool):
        if unit_id is None:
            unit_id = len(self.units)
            self.units.append({"en": "", "zh": "", "final": False, "generation": 0})
        unit = self.units[unit_id]
        unit["final"] = final
        changed = normalize_sentence(unit["en"]) != normalize_sentence(text)
        unit["en"] = text
        if changed:
            unit["generation"] += 1
            unit["zh"] = ""
        await self.session.emit({"event": "sentence", "id": unit_id, "en": text, "zh": unit["zh"], "final": final})
        if changed and text and self.enabled:
            task = asyncio.create_task(self.translate(unit_id, unit["generation"]))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)

    def snapshot(self) -> list[dict]:
        return [{"id": unit_id, "en": unit["en"], "zh": unit["zh"], "final": unit["final"]}
                for unit_id, unit in enumerate(self.units)]

    async def confirm(self, text: str):
        await self.set_unit(self.speculative.popleft() if self.speculative else None, text, True)

    async def on_final(self, text: str):
        self.pending = join_text_segments(self.pending, text)
        sentences, self.pending = split_sentences(self.pending)
        for sentence in sentences:
            await self.confirm(sentence)
        # A sentence that runs on without a full stop is cut at a clause boundary so the reader
        # is not left waiting for it to end.
        if len(self.pending.split()) >= TRANSLATE_MAX_WORDS:
            cut = max((match.end() for match in CLAUSE_END_RE.finditer(self.pending)), default=len(self.pending))
            head, self.pending = self.pending[:cut].strip(), self.pending[cut:].strip()
            await self.confirm(head)

    async def on_partial(self, preview: str):
        if not self.enabled:
            return
        sentences, rest = split_sentences(join_text_segments(self.pending, preview))
        # Deepgram may reword or merge sentences between interim results; keep the early rows in step.
        for unit_id, sentence in zip(list(self.speculative), sentences):
            if normalize_sentence(self.units[unit_id]["en"]) != normalize_sentence(sentence):
                await self.set_unit(unit_id, sentence, False)
        for index in range(len(self.speculative), len(sentences)):
            # Only trust a sentence end once more speech follows it; a trailing full stop in an
            # interim result often disappears when the speaker carries on.
            if index == len(sentences) - 1 and len(rest.split()) < 2:
                break
            unit_id = len(self.units)
            self.speculative.append(unit_id)
            await self.set_unit(None, sentences[index], False)

    async def on_pause(self):
        """The speaker stopped: whatever is left is a finished thought even without punctuation."""
        if self.pending:
            text, self.pending = self.pending, ""
            await self.confirm(text)
        while self.speculative:
            await self.set_unit(self.speculative.popleft(), "", True)

    async def finish(self):
        await self.on_pause()
        if self.tasks:
            await asyncio.wait(self.tasks, timeout=DEEPSEEK_TIMEOUT_SECONDS)

    async def translate(self, unit_id: int, generation: int):
        unit = self.units[unit_id]
        context = [item["en"] for item in self.units[max(0, unit_id - TRANSLATE_CONTEXT_SENTENCES):unit_id] if item["en"]]
        system = TRANSLATION_SYSTEM_PROMPT
        if self.session.keyterms:
            system += ("\n\nTerms used in this course (the recognizer may have misheard them; use their standard "
                       "Chinese terms, acronyms stay in English): " + ", ".join(self.session.keyterms))
        if context:
            system += "\n\nWhat was said just before (context only, do not translate it):\n" + " ".join(context)
        messages = [{"role": "system", "content": system}, {"role": "user", "content": unit["en"]}]
        loop = asyncio.get_running_loop()
        deltas: asyncio.Queue = asyncio.Queue()
        async with self.semaphore:
            if unit["generation"] != generation:
                return
            worker = asyncio.create_task(asyncio.to_thread(
                deepseek_stream_sync, messages, lambda delta: loop.call_soon_threadsafe(deltas.put_nowait, delta)
            ))
            worker.add_done_callback(lambda _: deltas.put_nowait(None))
            text = ""
            while (delta := await deltas.get()) is not None:
                text += delta
                if unit["generation"] == generation:
                    unit["zh"] = text.strip()
                    await self.session.emit({"event": "translation", "id": unit_id, "zh": unit["zh"], "done": False})
            if unit["generation"] != generation:
                return
            error = worker.exception()
            if error:
                print(f"Translation failed for {self.session.rec_id}: {describe_api_error(error)}", flush=True)
                payload = {"event": "translation", "id": unit_id, "zh": unit["zh"], "done": True, "failed": True}
                if not self.error_reported:
                    self.error_reported = True
                    payload["message"] = describe_api_error(error)
                await self.session.emit(payload)
                return
            self.error_reported = False
            await self.session.emit({"event": "translation", "id": unit_id, "zh": unit["zh"], "done": True})


class STTSession:
    """One recording. Owns the audio buffer and the Deepgram connection, and outlives browser reconnects.

    Audio positions are absolute byte offsets since the recording started. Everything before
    `finalized` is transcribed and committed; the rest is kept so it can be replayed after a drop.
    """

    def __init__(self, rec_id: str, committed_text: str = ""):
        self.rec_id = rec_id
        self.committed_text = committed_text
        self.learn_from = len(committed_text)   # only text recorded in this session teaches the course list
        self.course = ""
        self.keyterms: list[str] = list(STT_KEYTERMS)
        self.translator = LiveTranslator(self)
        self.audio = bytearray()
        self.audio_start = 0
        self.audio_end = 0
        self.finalized = 0
        self.lost_bytes = 0
        self.next_seq = 0
        self.acked_seq: int | None = None
        self.chunk_ends: deque[tuple[int, int]] = deque()
        self.websocket: WebSocket | None = None
        self.send_lock = asyncio.Lock()
        self.audio_event = asyncio.Event()
        self.stop_requested = False
        self.stop_deadline = 0.0
        self.stopped = asyncio.Event()
        self.closed = False
        self.status = ""
        self.detached_at: float | None = None
        self.deepgram_ws = None
        self.outage_until = 0.0
        self.manager = asyncio.create_task(self.run())

    def attach(self, websocket: WebSocket):
        self.websocket = websocket
        self.detached_at = None

    def detach(self, websocket: WebSocket):
        if self.websocket is websocket:
            self.websocket = None
            self.detached_at = time.monotonic()

    async def emit(self, payload: dict):
        websocket = self.websocket
        if websocket is None:
            return
        async with self.send_lock:
            with suppress(Exception):
                await websocket.send_json(payload)

    async def set_status(self, state: str):
        if state == self.status == "live":
            return
        self.status = state
        await self.emit({
            "event": "status",
            "state": state,
            "buffered_seconds": round((self.audio_end - self.finalized) / STT_BYTES_PER_SECOND, 1),
        })

    def add_audio(self, pcm: bytes, seq: int | None):
        self.audio += pcm
        self.audio_end += len(pcm)
        if seq is not None:
            self.chunk_ends.append((seq, self.audio_end))
        overflow = len(self.audio) - STT_BUFFER_SECONDS * STT_BYTES_PER_SECOND
        if overflow > 0:
            del self.audio[:overflow]
            self.audio_start += overflow
            if self.finalized < self.audio_start:
                self.lost_bytes += self.audio_start - self.finalized
                self.finalized = self.audio_start
        self.audio_event.set()

    def audio_slice(self, start: int, end: int) -> bytes:
        start = max(start, self.audio_start)
        return bytes(self.audio[start - self.audio_start:end - self.audio_start])

    def mark_finalized(self, offset: int):
        offset = min(offset, self.audio_end)
        if offset <= self.finalized:
            return
        self.finalized = offset
        # Keep a short tail of finalized audio so the next stream can re-send it for context.
        drop = self.finalized - seconds_to_bytes(STT_JOIN_OVERLAP_SECONDS) - self.audio_start
        if drop >= 5 * STT_BYTES_PER_SECOND:
            del self.audio[:drop]
            self.audio_start += drop

    def finalized_seq(self) -> int | None:
        seq = None
        while self.chunk_ends and self.chunk_ends[0][1] <= self.finalized:
            seq = self.acked_seq = self.chunk_ends.popleft()[0]
        return seq

    async def commit(self, text: str):
        text = (text or "").strip()
        if self.lost_bytes:
            text = join_text_segments(f"[… {self.lost_bytes // STT_BYTES_PER_SECOND}s of audio lost …]", text)
            self.lost_bytes = 0
        seq = self.finalized_seq()
        if not text:
            if seq is not None:
                await self.emit({"event": "ack", "ack": seq})
            return
        self.committed_text = join_text_segments(self.committed_text, text)
        await self.translator.on_final(text)
        event = {
            "event": "final",
            "committed_text": self.committed_text,
            "preview_text": "",
            "display_text": self.committed_text,
            "delta_text": text,
        }
        event["tail_text"] = self.translator.tail()
        if seq is not None:
            event["ack"] = seq
        await self.emit(event)

    async def run(self):
        backoff = 0.5
        backfilled = False
        try:
            while not self.closed:
                backlog = self.audio_end - self.finalized
                if self.stop_requested and backlog < seconds_to_bytes(0.2):
                    break
                if backlog == 0:
                    self.audio_event.clear()
                    await self.audio_event.wait()
                    continue
                try:
                    # After one backfill go live even if audio piled up meanwhile; chaining
                    # backfills on a slow link would never get back to live captions.
                    if backlog > seconds_to_bytes(STT_BURST_MAX_SECONDS) and not backfilled:
                        await self.backfill()
                        backfilled = True
                    else:
                        backfilled = False
                        await self.stream()
                    backoff = 0.5
                except asyncio.CancelledError:
                    raise
                except STTFatalError as exc:
                    print(f"STT fatal error for {self.rec_id}: {exc}", flush=True)
                    await self.emit({"event": "error", "message": str(exc), "committed_text": self.committed_text})
                    break
                except Exception as exc:
                    print(f"Deepgram connection lost for {self.rec_id}: {exc!r}", flush=True)
                    backfilled = False
                    if self.stop_requested and time.monotonic() > self.stop_deadline:
                        self.lost_bytes += self.audio_end - self.finalized
                        self.mark_finalized(self.audio_end)
                        await self.commit("")
                        break
                    await self.set_status("reconnecting")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 5)
        finally:
            self.stopped.set()

    async def backfill(self):
        """Catch up on a long backlog through the pre-recorded API, then let the live stream take over."""
        start, end = self.finalized, self.audio_end
        if time.monotonic() < self.outage_until:
            raise ConnectionError("simulated outage")
        try:
            words = await asyncio.to_thread(transcribe_prerecorded_sync, self.audio_slice(start, end), self.keyterms)
        except Exception as exc:
            raise_if_deepgram_rejected(exc)
            raise
        cut = end
        if not (self.stop_requested and end == self.audio_end):
            # The clip ends mid-speech, so its last words are cut off or lack context. Keep the
            # words before them and leave the rest unfinalized for the live stream to transcribe.
            clip_seconds = (end - start) / STT_BYTES_PER_SECOND
            cut_seconds = clip_seconds - STT_JOIN_TRIM_SECONDS
            while words and float(words[-1].get("end") or 0) > clip_seconds - STT_JOIN_TRIM_SECONDS:
                cut_seconds = min(cut_seconds, float(words.pop().get("start") or 0))
            if words:
                cut_seconds = max(cut_seconds, float(words[-1].get("end") or 0))
            if cut_seconds > 0:
                cut = start + seconds_to_bytes(cut_seconds)
        self.mark_finalized(cut)
        await self.commit(words_to_text(words))

    async def stream(self):
        # A stream that starts cold in the middle of a sentence tends to miss its first word, so
        # re-send a few seconds of already-transcribed audio for context and drop the repeated words.
        base = max(self.audio_start, self.finalized - seconds_to_bytes(STT_JOIN_OVERLAP_SECONDS))
        base_seconds = base / STT_BYTES_PER_SECOND
        resume_after = self.finalized / STT_BYTES_PER_SECOND if base < self.finalized else None
        if time.monotonic() < self.outage_until:
            raise ConnectionError("simulated outage")
        try:
            deepgram_ws = await asyncio.wait_for(connect_deepgram_stream(self.keyterms), 10)
        except Exception as exc:
            raise_if_deepgram_rejected(exc)
            raise
        self.deepgram_ws = deepgram_ws
        send_pos = base
        last_sent_at = last_message_at = time.monotonic()
        closing = asyncio.Event()
        drained = asyncio.Event()

        async def sender():
            nonlocal send_pos, last_sent_at
            while True:
                if send_pos < self.audio_end:
                    chunk = self.audio_slice(send_pos, self.audio_end)
                    send_pos = self.audio_end
                    last_sent_at = time.monotonic()
                    await deepgram_ws.send(chunk)
                    continue
                if self.stop_requested or time.monotonic() - last_sent_at > STT_IDLE_CLOSE_SECONDS:
                    # CloseStream makes Deepgram transcribe whatever it still holds, send the last
                    # finals, then Metadata. It works through a replayed backlog at about real-time
                    # speed, so the wait scales with what is still unconfirmed.
                    closing.set()
                    await deepgram_ws.send(json.dumps({"type": "CloseStream"}))
                    wait_seconds = STT_STREAM_CLOSE_WAIT_SECONDS + (send_pos - self.finalized) / STT_BYTES_PER_SECOND
                    try:
                        await asyncio.wait_for(drained.wait(), wait_seconds)
                    except asyncio.TimeoutError:
                        raise TimeoutError("Deepgram did not finish the stream") from None
                    self.mark_finalized(send_pos)
                    await self.commit("")
                    return
                self.audio_event.clear()
                try:
                    await asyncio.wait_for(self.audio_event.wait(), 3)
                except asyncio.TimeoutError:
                    await deepgram_ws.send(json.dumps({"type": "KeepAlive"}))

        async def receiver():
            nonlocal last_message_at, resume_after
            async for raw_message in deepgram_ws:
                last_message_at = time.monotonic()
                if isinstance(raw_message, bytes):
                    raw_message = raw_message.decode("utf-8", errors="ignore")
                try:
                    response = json.loads(raw_message)
                except json.JSONDecodeError:
                    continue
                if response.get("type") == "Metadata" and closing.is_set():
                    drained.set()
                if response.get("type") == "UtteranceEnd":
                    await self.translator.on_pause()
                    await self.emit({"event": "tail", "tail_text": self.translator.tail()})
                if response.get("type") != "Results":
                    continue
                alternatives = (response.get("channel") or {}).get("alternatives") or []
                alternative = alternatives[0] if alternatives else {}
                transcript = (alternative.get("transcript") or "").strip()
                start = float(response.get("start") or 0)
                end = start + float(response.get("duration") or 0)
                if resume_after is not None:
                    transcript = words_to_text([
                        word for word in alternative.get("words") or []
                        if base_seconds + (float(word.get("start") or 0) + float(word.get("end") or 0)) / 2 > resume_after
                    ])

                if not response.get("is_final"):
                    if transcript:
                        event = {
                            "event": "partial",
                            "committed_text": self.committed_text,
                            "preview_text": transcript,
                            "display_text": join_text_segments(self.committed_text, transcript),
                            "delta_text": transcript,
                        }
                        if resume_after is None:
                            await self.translator.on_partial(transcript)
                        event["tail_text"] = self.translator.tail(transcript)
                        await self.emit(event)
                    continue

                if resume_after is not None and base_seconds + end > resume_after:
                    resume_after = None
                self.mark_finalized(base + seconds_to_bytes(end))
                await self.commit(transcript)

        async def watchdog():
            while True:
                await asyncio.sleep(2)
                now = time.monotonic()
                if now < self.outage_until:
                    raise ConnectionError("simulated outage")
                if now - last_sent_at < 3 and now - last_message_at > STT_WATCHDOG_SECONDS:
                    raise TimeoutError("Deepgram stopped responding")

        tasks = [asyncio.create_task(sender()), asyncio.create_task(receiver()), asyncio.create_task(watchdog())]
        try:
            await self.set_status("live")
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task.exception():
                    raise task.exception()
            if tasks[0] not in done:
                if not closing.is_set():
                    raise ConnectionError("Deepgram closed the stream")
                await asyncio.wait_for(tasks[0], 5)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.deepgram_ws = None
            with suppress(Exception):
                await deepgram_ws.close()

    async def stop(self):
        if not self.stop_requested:
            self.stop_requested = True
            self.stop_deadline = time.monotonic() + STT_STOP_GIVEUP_SECONDS
            self.audio_event.set()
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.stopped.wait(), STT_STOP_GIVEUP_SECONDS + 30)
        await self.translator.finish()
        learn_course_terms_later(self.course, self.committed_text[self.learn_from:])
        await self.emit({
            "event": "stopped",
            "committed_text": self.committed_text,
            "preview_text": "",
            "display_text": self.committed_text,
            "delta_text": "",
        })
        await self.close()

    async def close(self):
        self.closed = True
        for task in list(self.translator.tasks):
            task.cancel()
        if STT_SESSIONS.get(self.rec_id) is self:
            del STT_SESSIONS[self.rec_id]
        if not self.manager.done():
            self.manager.cancel()
            with suppress(asyncio.CancelledError):
                await self.manager


STT_SESSIONS: dict[str, STTSession] = {}


async def expire_abandoned_stt_sessions():
    while True:
        await asyncio.sleep(60)
        now = time.monotonic()
        for session in list(STT_SESSIONS.values()):
            if session.detached_at is not None and now - session.detached_at > STT_SESSION_GRACE_SECONDS:
                print(f"STT session expired: {session.rec_id}", flush=True)
                await session.close()


def fetch_json_sync(url: str, headers: dict, timeout: int = 15):
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def deepgram_balance_sync() -> dict:
    if not STT_API_KEY:
        return {"ok": False, "reason": "no_key"}
    headers = {"Authorization": f"Token {STT_API_KEY}"}
    try:
        projects = fetch_json_sync("https://api.deepgram.com/v1/projects", headers).get("projects") or []
        if not projects:
            return {"ok": False, "reason": "error", "detail": "no project"}
        payload = fetch_json_sync(f"https://api.deepgram.com/v1/projects/{projects[0]['project_id']}/balances", headers)
        balances = payload.get("balances") or []
        return {
            "ok": True,
            "amount": round(sum(float(item.get("amount") or 0) for item in balances), 2),
            "currency": str((balances[0].get("units") if balances else "") or "usd").upper(),
        }
    except urllib.error.HTTPError as exc:
        # A key without the billing:read scope can transcribe but cannot see the balance.
        return {"ok": False, "reason": "no_permission" if exc.code in (401, 403) else "error", "detail": describe_api_error(exc)}
    except Exception as exc:
        return {"ok": False, "reason": "error", "detail": describe_api_error(exc)}


def deepseek_balance_sync() -> dict:
    if not DEEPSEEK_API_KEY:
        return {"ok": False, "reason": "no_key"}
    try:
        payload = fetch_json_sync(f"{DEEPSEEK_API_URL}/user/balance", {"Authorization": f"Bearer {DEEPSEEK_API_KEY}"})
        infos = payload.get("balance_infos") or []
        if not infos:
            return {"ok": False, "reason": "error", "detail": "no balance info"}
        return {
            "ok": True,
            "amount": round(float(infos[0].get("total_balance") or 0), 2),
            "currency": str(infos[0].get("currency") or "CNY").upper(),
        }
    except urllib.error.HTTPError as exc:
        return {"ok": False, "reason": "no_permission" if exc.code in (401, 403) else "error", "detail": describe_api_error(exc)}
    except Exception as exc:
        return {"ok": False, "reason": "error", "detail": describe_api_error(exc)}


BALANCE_CACHE = {"at": 0.0, "data": None}


@app.get("/balances")
async def balances():
    if BALANCE_CACHE["data"] is None or time.monotonic() - BALANCE_CACHE["at"] > BALANCE_CACHE_SECONDS:
        deepgram, deepseek = await asyncio.gather(
            asyncio.to_thread(deepgram_balance_sync), asyncio.to_thread(deepseek_balance_sync)
        )
        BALANCE_CACHE.update(at=time.monotonic(), data={"deepgram": deepgram, "deepseek": deepseek})
    return BALANCE_CACHE["data"]


@app.get("/courses")
async def list_courses(request: Request):
    require_history_access(request)
    with history_connection() as connection:
        rows = connection.execute("SELECT course FROM course_terms ORDER BY updated_at DESC").fetchall()
        scenes = connection.execute(
            "SELECT scene FROM history_sessions WHERE scene != '' GROUP BY scene ORDER BY MAX(updated_at) DESC LIMIT 30"
        ).fetchall()
    names = list(dict.fromkeys([row["course"] for row in rows] + [normalize_course(row["scene"]) for row in scenes]))
    return {"courses": [name for name in names if name]}


@app.get("/course-terms")
async def get_course_terms(request: Request, course: str = ""):
    require_history_access(request)
    return {"course": normalize_course(course), "terms": load_course_terms(normalize_course(course))}


@app.put("/course-terms")
async def put_course_terms(request: Request):
    require_history_access(request)
    data = await request.json()
    course = normalize_course(data.get("course"))
    if not course:
        raise HTTPException(status_code=400, detail="Course name is required.")
    # The edited list is the truth: keep what the user kept, add what they typed, drop the rest.
    existing = {term["term"].lower(): term for term in load_course_terms(course)}
    terms = []
    for raw in data.get("terms") or []:
        term = clean_term(raw)
        if term and term.lower() not in {item["term"].lower() for item in terms}:
            terms.append(existing.get(term.lower()) or {"term": term, "source": "manual", "count": 1})
    terms.sort(key=lambda entry: COURSE_TERM_SOURCES.index(entry.get("source") or "auto"))
    save_course_terms(course, terms)
    return {"course": course, "terms": load_course_terms(course)}


@app.post("/course-terms/import")
async def import_course_terms(request: Request, course: str = "", filename: str = ""):
    require_history_access(request)
    course = normalize_course(course)
    if not course:
        raise HTTPException(status_code=400, detail="Course name is required.")
    if not DEEPSEEK_API_KEY:
        raise HTTPException(status_code=400, detail="DEEPSEEK_API_KEY is not set.")
    if int(request.headers.get("content-length") or 0) > COURSE_IMPORT_MAX_BYTES:
        raise HTTPException(status_code=413, detail="File is too large.")
    data = await request.body()
    if len(data) > COURSE_IMPORT_MAX_BYTES:
        raise HTTPException(status_code=413, detail="File is too large.")
    try:
        text = await asyncio.to_thread(extract_document_text, filename, data)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not read the file: {exc}") from exc
    if len(text.split()) < 20:
        raise HTTPException(status_code=400, detail="No readable text in the file (scanned slides have none).")
    try:
        terms = await asyncio.to_thread(learn_course_terms_sync, course, text, "slides")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"DeepSeek: {describe_api_error(exc)}") from exc
    return {"course": course, "terms": terms}


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "port": PORT,
        "sttProvider": "deepgram-streaming",
        "sttModel": STT_MODEL,
        "sttReady": bool(STT_API_KEY),
        "sttSampleRate": STT_SAMPLE_RATE,
        "sttEndpointingMs": STT_ENDPOINTING_MS,
        "translationReady": bool(DEEPSEEK_API_KEY),
        "translationModel": DEEPSEEK_MODEL,
    }


@app.get("/")
async def serve_index():
    # no-cache so UI changes always reach the browser on refresh
    return FileResponse(
        CURRENT_DIR / "index.html",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.get("/history")
async def list_history(request: Request, q: str = "", limit: int = HISTORY_DEFAULT_LIMIT):
    require_history_access(request)
    safe_limit = max(1, min(HISTORY_MAX_LIMIT, limit))
    query = (q or "").strip()
    params = []
    where = ""
    if query:
        where = "WHERE title LIKE ? OR preview LIKE ? OR transcript LIKE ? ESCAPE '\\'"
        escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        pattern = f"%{escaped}%"
        params = [pattern] * 3
    with history_connection() as connection:
        rows = connection.execute(
            f"""SELECT id, title, substr(preview, 1, 200) AS preview, scene, created_at, updated_at
                FROM history_sessions {where} ORDER BY updated_at DESC LIMIT ?""",
            (*params, safe_limit),
        ).fetchall()
        total = connection.execute(
            f"SELECT COUNT(*) AS count FROM history_sessions {where}", params
        ).fetchone()["count"]
        titles = connection.execute(
            "SELECT title FROM history_sessions WHERE title LIKE 'Session %'"
        ).fetchall()
    next_number = 1
    for row in titles:
        match = re.match(r"^Session\s+(\d+)$", row["title"], re.IGNORECASE)
        if match:
            next_number = max(next_number, int(match.group(1)) + 1)
    return {
        # The list is refreshed often, so it carries no transcript; the full session is fetched on open.
        "items": [
            {
                "id": row["id"], "title": row["title"], "preview": row["preview"], "scene": row["scene"],
                "createdAt": row["created_at"], "updatedAt": row["updated_at"],
            }
            for row in rows
        ],
        "total": total,
        "nextNumber": next_number,
    }


@app.get("/history/{session_id}")
async def get_history_item(session_id: str, request: Request):
    require_history_access(request)
    item = history_item_by_id(session_id)
    if not item:
        raise HTTPException(status_code=404, detail="History item not found.")
    return item


@app.put("/history/{session_id}")
async def save_history_item(session_id: str, request: Request):
    require_history_access(request)
    data = await request.json()
    return upsert_history_item(data, session_id)


@app.delete("/history/{session_id}")
async def delete_history_item(session_id: str, request: Request):
    require_history_access(request)
    with history_connection() as connection:
        connection.execute("DELETE FROM history_sessions WHERE id = ?", (session_id,))
    return {"status": "success"}


@app.delete("/history")
async def delete_all_history(request: Request):
    require_history_access(request)
    with history_connection() as connection:
        connection.execute("DELETE FROM history_sessions")
    return {"status": "success"}


@app.post("/history/import")
async def import_history(request: Request):
    require_history_access(request)
    data = await request.json()
    items = data.get("items") if isinstance(data, dict) else []
    if not isinstance(items, list):
        raise HTTPException(status_code=400, detail="Invalid history import.")
    imported = 0
    for item in reversed(items[:HISTORY_MAX_LIMIT]):
        if not isinstance(item, dict):
            continue
        session_id = str(item.get("id") or f"import-{int(time.time() * 1000)}-{imported}")
        upsert_history_item(item, session_id)
        imported += 1
    return {"status": "success", "imported": imported}


@app.websocket("/ws-stt")
async def websocket_stt_endpoint(websocket: WebSocket):
    await websocket.accept()
    client_id = get_client_id_from_websocket(websocket)
    session: STTSession | None = None
    print(f"STT frontend connected: {client_id}", flush=True)

    async def send(payload: dict):
        if session:
            await session.emit(payload)
        else:
            await websocket.send_json(payload)

    def open_session(rec_id: str, committed_text: str = ""):
        existing = STT_SESSIONS.get(rec_id)
        if existing and not existing.closed:
            return existing, True
        STT_SESSIONS[rec_id] = STTSession(rec_id, committed_text)
        return STT_SESSIONS[rec_id], False

    try:
        await send({
            "event": "ready",
            "sample_rate": STT_SAMPLE_RATE,
            "provider": "deepgram-streaming",
            "model": STT_MODEL,
            "stt_ready": bool(STT_API_KEY),
        })

        while True:
            data = await websocket.receive_json()
            event_type = (data.get("type") or "audio").strip().lower()

            if event_type == "ping":
                await send({"type": "pong"})
                continue

            if event_type == "config":
                rec_id = str(data.get("rec_id") or "").strip()[:80] or f"anon-{uuid.uuid4().hex}"
                if session and session.rec_id != rec_id:
                    session.detach(websocket)
                session, resumed = open_session(rec_id, (data.get("committed_text") or "").strip())
                session.attach(websocket)
                session.translator.enabled = bool(data.get("translate") and DEEPSEEK_API_KEY)
                course = normalize_course(data.get("scene"))
                if course != session.course or not resumed:
                    session.course = course
                    session.keyterms = await asyncio.to_thread(course_keyterms, course)
                await send({
                    "event": "configured",
                    "committed_text": session.committed_text,
                    "resumed": resumed,
                    "next_seq": session.next_seq,
                    "ack": session.acked_seq,
                    # Sentence events sent while the browser was away were lost; resync the lot.
                    "units": session.translator.snapshot() if resumed else [],
                    "tail_text": session.translator.tail(),
                })
                if resumed and session.status:
                    status, session.status = session.status, ""
                    await session.set_status(status)
                continue

            if event_type == "audio":
                if not STT_API_KEY:
                    await send({
                        "event": "error",
                        "message": "STT API key missing. Set STT_API_KEY for HAHAHAv2.",
                    })
                    continue
                if session is None:
                    session, _ = open_session(f"anon-{uuid.uuid4().hex}")
                    session.attach(websocket)
                seq = data.get("seq")
                if isinstance(seq, int):
                    if seq < session.next_seq:
                        continue
                    session.next_seq = seq + 1
                else:
                    seq = None
                pcm_bytes = decode_pcm_base64_bytes(data.get("samples", ""))
                pcm_bytes = pcm_bytes[:len(pcm_bytes) - len(pcm_bytes) % 2]
                if pcm_bytes:
                    session.add_audio(pcm_bytes, seq)
                continue

            if event_type == "stop":
                if session:
                    await session.stop()
                    session = None
                else:
                    await send({"event": "stopped", "committed_text": "", "preview_text": "", "display_text": "", "delta_text": ""})
                continue

            if event_type == "debug" and STT_DEBUG_FAULTS and session:
                if data.get("action") == "outage":
                    session.outage_until = time.monotonic() + float(data.get("seconds") or 0)
                if session.deepgram_ws:
                    with suppress(Exception):
                        await session.deepgram_ws.close()
                continue
    except WebSocketDisconnect:
        print(f"STT frontend disconnected: {client_id}", flush=True)
    except Exception as exc:
        print(f"STT websocket error for {client_id}: {exc}", flush=True)
    finally:
        if session:
            session.detach(websocket)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT, ws_ping_interval=30, ws_ping_timeout=60)
