# The design gate

The design (137 boards, https://claude.ai/artifact/DFbFCyDEEbRsyFP7DRQuvx) is the
specification for the console. This directory measures a page against its boards,
element by element, and fails on any difference the allow-list does not name.

    go tool task design            # the Traffic boards, alone
    go tool task test:e2e          # everything, the gate included (tests/design-*.spec.ts)

## What is here

| Path | What it is |
|---|---|
| `boards/` | The boards' own files, unmodified, as the artifact holds them under `project/`: `Main.dc.html`, `Traf-02-Filter.dc.html` … `Traf-12-Rebalance-Story.dc.html`, `Sy-03-Narrow`, `Sy-04-Phone`, `Sy-05-Palette`, `Sy-06-Keys`, and `con.css`. |
| `lib/board.ts` | The renderer: a board's `<style>` and its `<div class="app">`, without the design tool's wrapper, with the console's own font files, in the light theme or the board's own `.app.dark`. |
| `lib/measure.ts` | What is measured of an element: position, size, font (weight, size, line height, family), colour, background, padding, border, radius, gap, letter spacing, text transform, opacity, grid tracks, wrapping, alignment, text. Every selector of a page in one read, so a page that polls cannot change between two of them. Elements that are not drawn are not measured. |
| `lib/diff.ts` | The differ and the allow-list: exact equality (no tolerance); an entry names the board, theme, width, element, index, property and both values, and quotes the words of `plan/00-foundation.md` that record why; an entry whose words the plan no longer has, or that no difference used, fails. |
| `lib/ledger.ts` | The generator's ledger, read back: what each client sent, when, over which connection, and what it reported. |
| `gen/` | The traffic: real franz-go clients (classic and KIP-848 groups, a transactional producer, a refused Produce, a denied group, a client that reports its metrics and one that does not), each writing its ledger. |
| `traffic/world.ts` | The Traffic boards' world: a broker of its own and the scenario (joins, a leave, a member killed and expired). |
| `traffic/boards.ts` | Each board, the state the page is put in, and the pairs compared. A pair whose text is data names the truth check that compares it with the ledger. |
| `traffic/truth.ts` | The truth checks: counts, clients, times, sizes, offsets and metrics against the ledger, with the exact bounds a client's own timestamps allow. |
| `traffic/allow.json` | The differences that are recorded deviations, each with its record. |
| `traffic/up.ts` | Starts the world and keeps it running, for working on the pages (`DESIGN_WORLD=1` measures it). |

## Refreshing the boards from the artifact

The board files are copied, not fetched, so the gate is the same on every machine and
offline. When the design changes, read the artifact's files (the `Artifact` tool's
`read` action with `path` `project/<board>.dc.html`, or the design tool's export of
`project/`) and replace the files here with them unchanged; then run
`go tool task design`, and for every new difference fix the page or record the
deviation in `plan/00-foundation.md` and its entry in the allow-list.

## Checking that it bites

Change one padding of a Traffic rule in `console/static/app.css` by one pixel, or make
a count of the summary line one too many in `console/traffic.go`, and run
`go tool task design`: each must fail, naming the element, the property and both
values. Revert afterwards.
