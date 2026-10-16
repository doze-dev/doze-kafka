# The design gate

The design (https://claude.ai/artifact/DFbFCyDEEbRsyFP7DRQuvx) is the specification of
the console (plan/01-protocol.md, "Design conformance"). This gate holds the console to
it: each board is rendered from its own HTML and CSS (`boards/`, see its README for where
they came from), the live console is brought to the state the board draws, and the two
are measured element by element with the same function. Every property of every compared
element must be equal, or the difference must be on the board's allow-list with the
reason it is allowed. An allow-list entry that no longer matches anything fails too, so
the list says only what is true now.

```sh
go tool task design                       # every board: the shell, the section homes and Traffic
cd e2e && E2E_PORT=14192 bunx playwright test -c design/playwright.config.ts homes    # one folder
DESIGN_SHOTS=/tmp/shots …                 # also keep a picture of both sides of each comparison
go tool task test:e2e                     # the browser suite, and the gate once
```

It boots its own broker (the browser suite's command, on `E2E_PORT`; the task uses 14192
so it can run beside `test:e2e`), seeds it with `e2e/seed`, and reads back what the
broker holds with `truth/` (Kafka's own requests and the registry's REST API, never the
console): the numbers a page shows are held to those. The Traffic gate starts a broker of
its own at `E2E_PORT` + 40 and real clients (`gen/`), because its boards are drawn from
traffic and the numbers are held to what those clients say they sent.

## Layout

One library (`lib/`), three gates (`shell/`, `homes/`, `traffic/`).

| Path | What it is |
|---|---|
| `lib/measure.ts` | The one measuring function, run in the page: position, size, font, colours, padding, margin, gap, borders, radius, display, grid tracks, opacity, decoration, wrapping, alignment and text of every match of every selector, in one read (a page that polls cannot change between two of them). Two views of it: `snapshot` (the drawn matches, as `El`: the Traffic gate's property names) and `measure` in `compare.ts` (the nth visible match: the shell gate's). |
| `lib/render.ts` | `openBoard(browser, file, size, theme)` renders a board from its own file (dark: the board's `.app.dark` tokens) with the console's vendored fonts; `openLive(browser, url, size, theme)` opens a console page in a theme before its first paint; `ROOT` is the repository. |
| `lib/compare.ts`, `lib/gate.ts`, `lib/probes.ts`, `lib/topic.ts` | The probe form of the comparison (shell and homes): a `Probe` is one element on each side, `explain` sorts the differences against a TypeScript allow-list (entries carry a reason, and `per` where the boards disagree), `runBoard` runs one board in each of its places. `probes.ts` and `topic.ts` hold the probes every page shares (top bar, rail, keys strip, a home's list and table; a topic page). |
| `lib/diff.ts`, `lib/ledger.ts` | The pair form of the comparison (Traffic): a `Pair` is a list of elements on each side, compared index by index; `traffic/allow.json` entries name board, theme, width, element, index, property and both values, and quote the words of `plan/00-foundation.md` that record why (an entry whose words the plan no longer has fails). A pair whose text is data names the truth check that compares it with the clients' ledger (`lib/ledger.ts` reads it back). |
| `lib/goprog.ts` | Builds and runs a Go program of the repository (`truth/`, `gen/`). |
| `shell/` | Sy-01 to Sy-08, and `common-allow.ts` (what every board's top bar may differ in). |
| `homes/` | Tp-Home, Gr-Home, Sc-Home, Tc-Home, Ac-Home, Cl-Home, the homes at 960 and 390, and Tp-24; `bite.spec.ts`. |
| `traffic/` | Main and Traf-02 to Traf-12: `traffic.spec.ts` (the gate), `boards.ts` (each board, the state the page is put in, the pairs), `truth.ts` (the truth checks: counts, clients, times, sizes, offsets and metrics against the ledger, with the exact bounds a client's own timestamps allow), `world.ts` (a broker of its own and the scenario: joins, a leave, a member killed and expired), `up.ts` (keeps the world running for working on the pages; `DESIGN_WORLD=1` measures it), `allow.json`. |
| `gen/` | The traffic: real franz-go clients (classic and KIP-848 groups, a transactional producer, a refused Produce, a denied group, a client that reports its metrics and one that does not), each writing its ledger. |
| `truth/` | Prints what the broker and the registry hold, as JSON (the shell and homes gates' truth; the Traffic gate's is its ledger). |
| `boards/` | The boards, unchanged, with one README and one `SHA256SUMS`. |

Two forms of comparison remain on purpose: the shell and homes gates compare named
probes and carry a reason on every allow-list entry, and the Traffic gate compares whole
lists of elements and carries a quote of the plan on every entry. They share the
renderer, the measuring function and the Go programs, and nothing else of one is used by
the other.

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

## Refreshing the boards from the artifact

The board files are copied, not fetched, so the gate is the same on every machine and
offline. When the design changes, read the artifact's files (the `Artifact` tool's
`read` action with `path` `project/<board>.dc.html`, or the design tool's export of
`project/`) and replace the files here with them unchanged; then run
`go tool task design`, and for every new difference fix the page or record the
deviation in `plan/00-foundation.md` and its entry in the allow-list.

## Reading a failure

A failing board prints one line per difference in the allow-list's own form:

```
{ board: 'Tp-Home', el: 'id band', prop: 'gap', board_value: "12px 12px", live_value: "16px 16px", at: ['1440-light'], reason: '' },
```

Fix the console, or, if the difference is true and the board cannot be built
truthfully, paste the line into the allow-list with the reason. A stale entry prints
as `allow-list entry explains nothing: …`: delete it.

## It bites

`homes/bite.spec.ts` runs with every `task design`: it plants a one-pixel change
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
| `traffic/traffic.spec.ts` | Main and Traf-02 to Traf-12 at 1440, light and dark, in one run of the world (an empty broker, the scenario's traffic, then the broker stopped) | every count, client, time, size, offset and metric, against the clients' ledger |

## Checking that the Traffic gate bites

Change one padding of a Traffic rule in `console/static/app.css` by one pixel, or make
a count of the summary line one too many in `console/traffic.go`, and run
`go tool task design`: each must fail, naming the element, the property and both
values. Revert afterwards. Both were tried by the Traffic gate's author, before the shell's gate was merged:

- `.tf-filter .inp { padding-left: 10px }` failed Main and Traf-02 in both themes:
  `filter field[0].pad: board "0px 9px", live "0px 9px 0px 10px"`, and the
  sub-pixel moves of the kinds control it pushed.
- The clients count one too many failed Main in both themes: `summary: "10 clients
  connected": the ledger has 9 clients with a connection open`.

What it cannot catch: a requests-in-the-last-minute count off by a few. The ledger has
only the client's times (when it began writing and read the answer), and the broker
stamps a request when it begins handling it, somewhere in between, so a busy minute's
count is a range (about 8 wide in the Traffic world: long-poll Fetches straddle the
edges). One too many inside that range passes. Tightening the bound to when the client
finished writing was tried, and failed real rows: the broker can begin a request a few
milliseconds after that. Traf-02's "X of Y requests match" is checked only for X ≤ Y
and for Y agreeing with the line under the table, so a Y one too many in both places
passes too (it was tried alongside the padding change and was not caught).
