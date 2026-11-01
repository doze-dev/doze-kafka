import type { Probe } from '../lib/compare';
import { SHELL, TEXT_W, TEXT_H } from '../lib/probes';

// The Groups boards and the pairs the gate compares on each. What is data (the group's
// name, its counts, its members) is held to the clients' ledger in groups.spec.ts; the
// probes compare the page's shape, type and colour with the board's.

const DATA_ROWS = 'its height is its rows, and the rows are the broker\'s: the world\'s clients, not the board\'s sample';

/** The list pane, the id band, the facts line, the neighbours and the tabs of a group. */
export const GROUP_HEAD: Probe[] = [
  { el: 'list', board: '.list', live: 'aside.list, nav.list' },
  { el: 'list cap', board: '.list .cap', live: '.list .cap', text: true },
  { el: 'list item', board: '.list .li:not(.on)', live: '.list .li:not(.on)' },
  { el: 'list item on', board: '.list .li.on', live: '.list .li.on' },
  { el: 'main', board: 'main.main', live: 'main.main' },
  { el: 'id band', board: 'main > .row.sb', live: '#group-live > .idband' },
  { el: 'h1', board: 'main h1', live: '#group-live h1', free: { w: TEXT_W } },
  { el: 'protocol pill', board: 'main > .row.sb .pill:not(.ok)', live: '#group-live > .idband .pill:not(.ok)', free: { w: TEXT_W } },
  { el: 'state pill', board: 'main > .row.sb .pill.ok', live: '#group-live > .idband .pill.ok', free: { w: TEXT_W } },
  { el: 'reset button', board: 'main > .row.sb .btn.pri', live: '#group-live > .idband .btn.pri' },
  { el: 'more button', board: 'main > .row.sb button[aria-label^="More actions"]', live: '#group-live > .idband button[aria-label^="More actions"]' },
  { el: 'facts line', board: 'main > .note', live: '#group-facts', free: { h: TEXT_H } },
  { el: 'neighbours', board: 'main .well.wrap', live: '#neighbours' },
  { el: 'neighbours cap', board: 'main .well.wrap > .cap', live: '#neighbours > .cap', text: true },
  { el: 'neighbour', board: 'main .well.wrap > a', live: '#neighbours > a' },
  { el: 'tabs', board: 'main .tabs', live: '#group-live nav.tabs' },
  { el: 'tab on', board: 'main .tabs .tab.on', live: '#group-live nav.tabs .tab.on', text: true },
  { el: 'tab', board: 'main .tabs .tab:not(.on)', live: '#group-live nav.tabs .tab:not(.on)' },
];

export const NONE: Probe[] = [
  { el: 'list', board: '.list', live: 'aside.list' },
  { el: 'list head', board: '.list > :first-child', live: '.list > .list-h' },
  { el: 'list none', board: '.list > .col.g4', live: '.list .list-none' },
  { el: 'list none title', board: '.list > .col.g4 > .b', live: '.list .list-none > .b', text: true },
  { el: 'list none note', board: '.list > .col.g4 > .note', live: '.list .list-none > .note', text: true },
  { el: 'main', board: 'main.main', live: 'main.main' },
  { el: 'id band', board: 'main > .row.sb', live: '#groups-none-root' },
  { el: 'h1', board: 'main h1', live: 'main h1', text: true },
  { el: 'h1 note', board: 'main h1 + .note', live: 'main h1 + .note', text: true },
  { el: 'live pill', board: 'main > .row.sb .pill', live: '#groups-none-root .pill' },
  { el: 'card', board: 'main > .card', live: 'main > .none-card.card' },
  { el: 'card title', board: 'main > .card > .h2', live: 'main > .none-card.card > .h2', text: true },
  { el: 'card note', board: 'main > .card > .note', live: 'main > .none-card.card > .note', text: true },
  { el: 'card cap', board: 'main > .card .cap', live: 'main > .none-card.card .cap', text: true },
  { el: 'card cap 2', board: 'main > .card .cap', live: 'main > .none-card.card .cap', nth: 1, text: true },
  { el: 'code', board: 'main > .card pre.code', live: 'main > .none-card.card pre.code', free: { text: 'the commands name this broker\'s address and one of its topics; the board names 127.0.0.1:9092 and harbour.checkout.orders' } },
  { el: 'code 2', board: 'main > .card pre.code', live: 'main > .none-card.card pre.code', nth: 1, free: { text: 'the command names this broker\'s address and one of its topics; the board names 127.0.0.1:9092 and harbour.checkout.orders' } },
  { el: 'connect button', board: 'main > .card .btn.pri', live: 'main > .none-card.card .btn.pri', text: true },
  { el: 'topics button', board: 'main > .card .btn:not(.pri)', live: 'main > .none-card.card .btn:not(.pri)', text: true },
  { el: 'button note', board: 'main > .card > .row .note', live: 'main > .none-card.card > .row .note', text: true },
  { el: 'callouts', board: 'main > .row.g12', live: 'main > .none-card.row' },
  { el: 'callout', board: 'main > .row.g12 .callout', live: 'main > .none-card.row .callout' },
  { el: 'callout 2', board: 'main > .row.g12 .callout', live: 'main > .none-card.row .callout', nth: 1 },
  { el: 'callout title', board: 'main > .row.g12 .callout .b', live: 'main > .none-card.row .callout .b', text: true },
  { el: 'callout note', board: 'main > .row.g12 .callout .note', live: 'main > .none-card.row .callout .note', text: true },
];

export const LAG: Probe[] = [
  { el: 'lag card', board: 'main .card', live: '#lag-table', free: { h: DATA_ROWS } },
  { el: 'lag head', board: 'main .card .r.h', live: '#lag-table .r.h' },
  { el: 'lag head cell', board: 'main .card .r.h > span', live: '#lag-table .r.h > span', text: true },
  { el: 'lag row', board: 'main .card .r:not(.h)', live: '#lag-table .r:not(.h)' },
  { el: 'lag total', board: 'main .card .r:last-child', live: '#lag-table .r.totalrow' },
  { el: 'live note', board: 'main > .row.ce.g8', live: '#group-live > .row.ce.g8' },
];

export const MEMBERS: Probe[] = [
  { el: 'members card', board: 'main .card', live: '#members-table', free: { h: DATA_ROWS } },
  { el: 'members head', board: 'main .card .r.h', live: '#members-table .r.h' },
  { el: 'members row', board: 'main .card .r:not(.h)', live: '#members-table .r:not(.h)' },
  { el: 'inspector', board: 'aside.insp', live: '#inspector-panel aside.insp, aside.insp' },
];

export { SHELL };
