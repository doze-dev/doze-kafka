# The design's boards

These files are the console's design, as the design gate (`e2e/design`) renders it.
They are the `project/` files of the design artifact
<https://claude.ai/artifact/DFbFCyDEEbRsyFP7DRQuvx>, read with
the Artifact tool's `read` action and copied here unchanged. They were read at
version 1791810435-5010 and read again at 1792149264-d921 on 2026-10-16; every file
here is byte-identical in both (the hashes are in `SHA256SUMS`).

- `con.css`: the shared stylesheet every board links;
- `canvas.json`: the artifact's index of boards (names, sizes, order);
- `<Board>.dc.html`: one board each. Only the boards a committed spec compares are
  here: Sy-01 to Sy-08, the section homes (Tp-Home, Gr-Home, Sc-Home, Tc-Home,
  Ac-Home, Cl-Home), Tp-24, and the Access boards the Access home builds from
  (Ac-03 Create, Ac-05 Check).

A board is a static page: it links `./support.js` (the design runtime, which these
boards do not need: they have no holes to fill; the gate serves an empty file in its
place) and asks Google Fonts for IBM Plex (the gate serves the console's own vendored
Plex files instead, `console/static/fonts`, so a difference in type is never a
difference in font files). Nothing else is loaded.

To take a newer version of a board, read the artifact again, replace the file, and
run `go tool task test:design`: every difference the new board makes is reported,
and the allow-lists say which are known.

SHA-256 of each file as committed is in `SHA256SUMS`; `go tool task test:design`
does not check it, a reviewer can (`shasum -a 256 -c SHA256SUMS`).
