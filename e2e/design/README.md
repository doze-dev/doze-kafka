# The design gate

The design is the specification of the console (plan/01-protocol.md, "Design
conformance"). This gate holds the console to it: each board is rendered from its
own HTML and CSS (`boards/`, see its README for where they came from), the live
console is brought to the state the board draws, and the two are measured element by
element with the same function. Every property of every probed element must be
equal, or the difference must be on the board's allow-list with the reason it is
allowed. An allow-list entry that no longer matches anything fails too, so the list
says only what is true now.

```sh
go tool task test:design                  # every board
cd e2e && E2E_PORT=14192 bunx playwright test -c design/playwright.config.ts homes   # one folder
DESIGN_SHOTS=/tmp/shots …                 # also keep a picture of both sides of each comparison
```

It boots its own broker (the browser suite's command, on `E2E_PORT`; the task uses
14192 so it can run beside `test:e2e`), seeds it with `e2e/seed`, and reads back what
the broker holds with `truth/` (Kafka's own requests and the registry's REST API, never
the console): the numbers a page shows are held to those.

## Layout

| Path | What it is |
|---|---|
| `lib/render.ts` | `openBoard(browser, file, size, theme)` renders a board (dark: the board's own `.app.dark` tokens); `openLive(browser, url, size, theme)` opens a console page in a theme before its first paint. |
| `lib/compare.ts` | `Probe` (one element on each side), `measure`, `diffProbes`, `explain` (sorts differences against an allow-list), `stale`, `report`. |
| `lib/gate.ts` | `runBoard(browser, run)`: one board in each of its places (default 1440 light and dark); returns what nothing explains and the stale entries. `truth()` is the seeded world. |
| `lib/probes.ts` | Probes every page shares: the top bar, the rail, the keys strip; a section home's list, id band, summary, table and notes. |
| `lib/topic.ts` | A topic page's probes (list, id band, facts, neighbours, tabs, records, composer, wire, inspector), the record Sy-02 opens, and `dropMarkers`. |
| `shell/` | Sy-01 to Sy-08, and `common-allow.ts` (what every board's top bar may differ in). |
| `homes/` | Tp-Home, Gr-Home, Sc-Home, Tc-Home, Ac-Home, Cl-Home, the homes at 960 and 390, and Tp-24. |
| `truth/` | Prints what the broker and the registry hold, as JSON. |
| `boards/` | The boards, unchanged. |

## What is compared

For each probe, on both sides: x, y, width and height (CSS px, to a tenth); the font
(family, weight, size, line height), letter spacing, text transform; colour and
background; padding, margin, gap; each border's width and colour; radius; display;
grid tracks; opacity; text decoration; and, where the probe says so, the text.

A probe may leave a property out only with a reason written beside it (`free`): a
width that is its text when the text is the seed's and not the board's sample, or a
position after such a text. Those reasons are printed with the allow-lists in
plan/00-foundation.md. Everything else that differs is an allow-list entry: board,
element, property, both values as measured, the places it holds, and the reason.

The boards do not always agree with one another (Sy-01 draws a specimen topic page;
Tp-05 and Tp-06, the topic page's own boards, draw it a little differently). Where
the console draws another board's value, the entry says so with `per: { board, sel?,
nth? }`, and the gate opens that board in the same theme, measures the element there
and fails the entry unless it gives the live value: "as Tp-05 draws it" is measured,
not taken on trust. A value that depends on the page's width (a `1fr` track) cannot
be checked that way and is written out in the reason instead.

A board drawn with numbered markers (Sy-01's "the numbers on the left mark each
part") has them taken out before it is measured (`lib/topic.ts`, `dropMarkers`):
they are annotations, and the console has none.

## Reading a failure

A failing board prints one line per difference in the allow-list's own form:

```
{ board: 'Tp-Home', el: 'id band', prop: 'gap', board_value: "12px 12px", live_value: "16px 16px", at: ['1440-light'], reason: '' },
```

Fix the console, or, if the difference is true and the board cannot be built
truthfully, paste the line into the allow-list with the reason. A stale entry prints
as `allow-list entry explains nothing: …`: delete it.

## It bites

`homes/bite.spec.ts` runs with every `task test:design`: it plants a one-pixel change
in a home (a style added to the live page after it loads: the list's rows 31 px, not
30) and a count one more than the broker holds in a home's summary, and passes only if
the gate reports the first as a difference nothing explains and the data check fails
on the second; the same home with nothing planted has nothing unexplained. A change to
`lib/` that blunts the comparison fails it.

The same was done once in the console's own source when the gate was written
(`.list .li` 31 px in app.css; the topics home's partitions plus one in
handlers_topics.go): homes.spec reported `list item h "30" -> "31"` in light and dark,
and data.spec "19 partitions" where the broker holds 18; both were put back by hand
and the gate was green again.

The comparison and data checks are:

| Spec | Boards and places | Data held to the broker |
|---|---|---|
| `homes/homes.spec.ts` | Tp-Home, Gr-Home, Sc-Home, Tc-Home, Ac-Home, Cl-Home at 1440 light and dark | |
| `homes/narrow.spec.ts` | each home at 960 (Sy-03's shell) and 390 (Sy-04's), light and dark | |
| `homes/data.spec.ts` | | every count, lag, version, id, ACL and size on the homes |
| `homes/confirms.spec.ts` | Tp-24's purge and delete | what each confirm says |
| `shell/anatomy.spec.ts` | Sy-01 (light, dark), Sy-02 (dark) on a topic | the table's cells, the schema callout |
| `shell/narrow.spec.ts` | Sy-03 at 960, Sy-04 at 390, light and dark | the sheet's scrim and grab handle |
| `shell/dialogs.spec.ts` | Sy-05's palette, Sy-06's shortcuts, light and dark | |
| `shell/themes.spec.ts` | Sy-07's switch; its specimens in light, dark and high contrast | every token, every printed ratio, the selection outline |
| `shell/firstrun.spec.ts` | Sy-08 on an empty broker of its own, light and dark | |
