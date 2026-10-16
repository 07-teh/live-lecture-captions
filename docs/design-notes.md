# Design notes

A condensed log of how the interface changed, based on my feedback during development.

## Iterations

1. **Glass overlay (June 2026).** The first idea was a translucent, frosted-glass window floating
   over other apps. It took several rounds: the first versions were too pale, then too transparent
   to read, then not draggable. The lesson: only the background layer should be translucent; text
   and controls must stay opaque and readable.
2. **Translation workstation.** A redesign modelled on mature translation tools: a prominent
   language pair, an original/translation split workspace, and one obvious record button. My
   feedback was that it was still too close to the previous layout, so it was rebuilt as a
   logo-free subtitle stage with light and dark themes.
3. **Warm, minimal style.** I moved the whole interface to a warm paper look: ivory and charcoal
   themes, a clay accent, a collapsible sidebar with recent sessions, a centered reading column and a
   bottom composer with the record button. A lesson from this step: matching a style means matching
   the layout, not just recoloring the old one.
4. **Lecture captions (October 2026).** Once the app became a tool for my own lectures, the layout
   changed to one English sentence with its Chinese line directly underneath, new sentences fading
   in, the Chinese typing out at an even pace, and the page following the latest sentence.

## Current rules

- Apple-like minimalism; no red accent colors.
- Light theme: background `#FAF9F5`, ink `#1F1E1D`, primary `#C2613E`.
- Dark theme: background `#262624`, ink `#F5F4EE`, primary `#D97757`.
- Idle primary action uses the clay accent; the recording state uses a hotter terracotta so it reads
  as a distinct alert.
- DM Sans for all functional and Chinese text; a serif (Fraunces) only for display headings, never
  for live Chinese text.
- No logo in the interface; a text title is enough.
