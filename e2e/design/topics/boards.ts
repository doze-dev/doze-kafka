import type { Probe } from '../lib/compare';
import { SHELL, HOME, TEXT_W, TEXT_H } from '../lib/probes';

// The Topics stage's boards: what each is compared on, and with which probes.

/** The list pane every topic board draws (a subset of a home's probes). */
export const LIST: Probe[] = HOME.filter((p) => p.el.startsWith('list ') || p.el === 'list');

export const NO_BODY_TEXT = 'its width is its text, and the text is the broker\'s own wording, not the board\'s';

const MAIN = 'main > .row.g16';
const LEFT = `${MAIN} > .col.g12:first-child`;
const RIGHT = `${MAIN} > .col.g12.grow`;
const L = 'form.tp-create > .tp-left';
const R = 'form.tp-create > .col.grow';

/** The id band, the left column's cards and the shell card: Tp-02, Tp-03. */
export const CREATE: Probe[] = [
  ...SHELL, ...LIST,
  { el: 'main', board: 'main.main', live: 'main.main' },
  { el: 'id band', board: 'main > .row.sb.ce', live: 'main > .idband' },
  { el: 'h1', board: 'main h1', live: 'main h1', text: true },
  { el: 'band cancel', board: 'main > .row.sb.ce .btn:not(.pri)', live: 'main > .idband .btn:not(.pri)', text: true, free: { x: 'right-aligned: its x is its neighbour\'s width' } },
  { el: 'band create', board: 'main > .row.sb.ce .btn.pri', live: 'main > .idband .btn.pri', text: true },
  { el: 'columns', board: MAIN, live: 'form.tp-create', free: { h: 'its height is its left column\'s, whose notes are the broker\'s teaching lines' } },
  { el: 'left', board: LEFT, live: L, free: { h: 'its height is its cards, which hold the broker\'s teaching lines' } },
  { el: 'card 1', board: `${LEFT} > .card`, live: `${L} > .card`, free: { h: TEXT_H } },
  { el: 'card 2', board: `${LEFT} > .card`, live: `${L} > .card`, nth: 1, free: { h: TEXT_H, y: 'it follows card 1' } },
  { el: 'name label', board: `${LEFT} label[for="f-name"]`, live: `${L} label[for="f-name"]` },
  { el: 'name field', board: `${LEFT} #f-name`, live: `${L} #f-name`, free: { y: 'it follows the label, whose rule is the broker\'s wording' } },
  { el: 'partitions label', board: `${LEFT} label[for="f-parts"]`, live: `${L} label[for="f-partitions"]`, text: true },
  { el: 'partitions field', board: `${LEFT} #f-parts`, live: `${L} #f-partitions` },
  { el: 'replication label', board: `${LEFT} label[for="f-rf"]`, live: `${L} label[for="f-replication"]` },
  { el: 'replication field', board: `${LEFT} #f-rf`, live: `${L} #f-replication` },
  { el: 'foot buttons', board: `${LEFT} > .row.g8.ce:last-child`, live: `${L} > .row.g8.ce:last-child`, free: { y: 'it follows the settings card, whose hints are the broker\'s teaching lines' } },
  { el: 'foot create', board: `${LEFT} > .row.g8.ce:last-child .btn.pri`, live: `${L} > .row.g8.ce:last-child .btn.pri`, free: { y: 'it follows the settings card, whose hints are the broker\'s teaching lines' }, text: true },
  { el: 'foot cancel', board: `${LEFT} > .row.g8.ce:last-child .btn.ghost`, live: `${L} > .row.g8.ce:last-child .btn.ghost`, free: { y: 'it follows the settings card, whose hints are the broker\'s teaching lines' }, text: true },
  { el: 'right', board: RIGHT, live: R, free: { h: 'it is as tall as the left column, whose notes are the broker\'s teaching lines' } },
  { el: 'shell card', board: `${RIGHT} > .card`, live: `${R} > .card`, free: { h: 'its height is the command, whose lines carry this broker\'s address' } },
  { el: 'shell cap', board: `${RIGHT} > .card > .cap`, live: `${R} > .card > .cap`, text: true },
  { el: 'shell code', board: `${RIGHT} > .card pre.code`, live: `${R} > .card pre.code`, free: { h: 'its lines carry this broker\'s address and the client.properties flag', w: NO_BODY_TEXT } },
  { el: 'shell note', board: `${RIGHT} > .card > .note`, live: `${R} > .card > .note`, text: true, free: { y: 'it follows the command' } },
  { el: 'callout', board: `${RIGHT} > .callout`, live: `${R} > .callout:not([x-cloak]):not([style*="none"])`, free: { w: NO_BODY_TEXT, h: TEXT_H, y: 'it follows the command' } },
  { el: 'callout title', board: `${RIGHT} > .callout .b`, live: `${R} > .callout:not([style*="none"]) .b`, free: { w: NO_BODY_TEXT, y: 'it follows the command' } },
];

const CFG = ['retention.ms', 'retention.bytes', 'segment.bytes', 'max.message.bytes', 'cleanup.policy', 'message.timestamp.type'];
const TEXTY = 'it is a line of the broker\'s own teaching text, not the board\'s';

/** Tp-03: the settings card, every field of it, and the warning for a compacted topic. */
export const SETTINGS: Probe[] = [
  ...CREATE,
  { el: 'general row', board: `${LEFT} > .card.row`, live: `${L} > .card.row` },
  { el: 'settings head', board: `${LEFT} > .card:nth-child(2) > .row.sb.ce`, live: `${L} > .tp-settings > .row.sb.ce` },
  { el: 'settings title', board: `${LEFT} > .card:nth-child(2) .h2`, live: `${L} > .tp-settings .h2`, text: true },
  { el: 'settings hide', board: `${LEFT} > .card:nth-child(2) > .row.sb.ce > .note`, live: `${L} > .tp-settings > .row.sb.ce > .note`, text: true },
  ...[0, 1, 2].map((n): Probe => ({ el: `cfg row ${n}`, board: `${LEFT} > .card:nth-child(2) > .row.g12`, live: `${L} > .tp-settings > .row.g12`, nth: n, free: { h: TEXTY, y: TEXTY } })),
  ...CFG.flatMap((k, n): Probe[] => [
    { el: `cfg label ${k}`, board: `${LEFT} > .card:nth-child(2) label.b.m`, live: `${L} > .tp-settings label.b.m`, nth: n, text: true, free: { y: TEXTY } },
    { el: `cfg field ${k}`, board: `${LEFT} > .card:nth-child(2) .inp.m`, live: `${L} > .tp-settings .inp.m`, nth: n, free: { y: TEXTY } },
  ]),
  { el: 'settings foot note', board: `${LEFT} > .card:nth-child(2) > .note`, live: `${L} > .tp-settings > .note`, free: { w: NO_BODY_TEXT, h: TEXTY, y: TEXTY } },
  { el: 'warn callout', board: `${RIGHT} > .callout.warn`, live: `${R} > .callout.warn`, free: { w: NO_BODY_TEXT, h: TEXTY, y: TEXTY } },
];

/** Tp-04: the refusal under the General card, and the name field marked. */
const WHERE = 'the board draws a compact form (one row of fields, 560 px) and a column of other refusals beside it; the console keeps the one form of Tp-02 and puts the refusal under its General card';
export const REFUSED: Probe[] = [
  ...SHELL, ...LIST,
  { el: 'main', board: 'main.main', live: 'main.main' },
  { el: 'id band', board: 'main > .row.sb.ce', live: 'main > .idband' },
  { el: 'h1', board: 'main h1', live: 'main h1', text: true, free: { w: 'its width is the width of the note under it, which is this page\'s own sentence' } },
  { el: 'name field', board: '#f-name', live: '#f-name', free: { x: WHERE, y: WHERE, w: WHERE, h: WHERE } },
  { el: 'refusal', board: `${LEFT} > .callout`, live: 'form.tp-create [data-err-slot] .callout', free: { x: WHERE, y: WHERE, w: WHERE, h: 'its height is the broker\'s message and rule, which wrap by their own length' } },
  { el: 'refusal kind', board: `${LEFT} > .callout .b`, live: 'form.tp-create [data-err-slot] .callout .refusal-kind', text: true, free: { x: WHERE, y: WHERE } },
  { el: 'refusal name', board: `${LEFT} > .callout .row .m`, live: 'form.tp-create [data-err-slot] .callout .refusal-name', text: true, free: { x: WHERE, y: WHERE } },
];

/** Tp-01: the list pane with nothing in it and the page that says so. */
export const NONE: Probe[] = [
  ...SHELL,
  { el: 'list', board: '.list', live: 'aside.list' },
  { el: 'list head', board: '.list > .row.sb.ce', live: 'aside.list .list-h' },
  { el: 'list cap', board: '.list .cap', live: '.list .cap', text: true, free: { w: TEXT_W } },
  { el: 'list new', board: '.list .btn', live: '.list .btn', text: true },
  { el: 'list none', board: '.list .col.g4 .b', live: '.list .tp-list-none .b', text: true },
  { el: 'main', board: 'main.main', live: 'main.main' },
  { el: 'id band', board: 'main > .row.sb.ce', live: 'main > .idband' },
  { el: 'h1', board: 'main h1', live: 'main h1', text: true, free: { w: TEXT_W } },
  { el: 'band note', board: 'main h1 + .note', live: 'main h1 + .note', free: { w: TEXT_W } },
  { el: 'band create', board: 'main > .row.sb.ce .btn.pri', live: 'main > .idband .btn.pri', text: true },
  { el: 'none card', board: 'main > .card', live: '#topics-none', free: { h: 'its height is its text, which names this broker\'s address and auto-create setting' } },
  { el: 'none cap', board: 'main > .card .cap', live: '#topics-none .cap', text: true },
  { el: 'none title', board: 'main > .card h2', live: '#topics-none h2', text: true },
  { el: 'none create', board: 'main > .card .btn.pri', live: '#topics-none .btn.pri', text: true, free: { y: 'it follows the paragraph above, whose text is this broker\'s' } },
  { el: 'none code', board: 'main > .card pre.code', live: '#topics-none pre.code', free: { y: 'it follows the paragraph above', w: 'its lines carry this broker\'s address', h: 'its lines carry this broker\'s address' } },
];

// ---------- Tp-05 and Tp-06: the topic page ----------
import { TOPIC_COMMON } from '../lib/topic';

const BELOW = 'it is below the neighbours, whose note says where Written by comes from (the board draws none), and the seed\'s own topic list and rows';
const T_ROWS = 'its height is its rows, and the rows are the seed\'s';
/** Tp-05's body, under the shared parts of the topic page (list, band, tabs, dock). */
export const TOPIC_BODY: Probe[] = [
  { el: 'facts', board: 'main > .row.ce.g8.wrap', live: '#topic-facts' },
  { el: 'facts item', board: 'main > .row.ce.g8.wrap > .m', live: '#topic-facts > .m', nth: 0, free: { w: TEXT_W } },
  { el: 'facts sep', board: 'main > .row.ce.g8.wrap > .mut', live: '#topic-facts > .mut', free: { x: TEXT_W } },
  { el: 'neighbours', board: 'main > .well.row', live: '#neighbours', free: { w: 'it is as wide as the page' } },
  { el: 'neighbour label', board: 'main > .well > .note', live: '#neighbours > .note', text: true, free: { x: TEXT_W, w: TEXT_W } },
  { el: 'neighbour', board: 'main > .well > a.row', live: '#neighbours > a.row', free: { x: TEXT_W, w: TEXT_W } },
  { el: 'neighbour chip', board: 'main > .well > a.row .sv', live: '#neighbours > a.row .sv', free: { x: TEXT_W } },
  { el: 'tape head', board: 'main > .card[style*="padding: 8px 12px 6px"] > .row.sb.ce', live: '.tp-tape-h', free: { y: BELOW } },
  { el: 'tape cap', board: 'main > .card[style*="padding: 8px 12px 6px"] .cap', live: '.tp-tape .cap', text: true, free: { y: BELOW } },
  { el: 'tape note', board: 'main > .card[style*="padding: 8px 12px 6px"] .row.ce.g8 .note', live: '.tp-tape .tp-tape-h .note', text: true, free: { y: BELOW } },
  { el: 'tape lane', board: 'main > .card[style*="padding: 8px 12px 6px"] > .row.ce.g8:nth-of-type(2)', live: '.tp-tape > .row:nth-of-type(2)', free: { y: BELOW } },
  { el: 'tape lane label', board: 'main > .card[style*="padding: 8px 12px 6px"] > .row.ce.g8:nth-of-type(2) > .m', live: '.tp-tape > .row:nth-of-type(2) .tp-lane-p', text: true, free: { y: BELOW } },
  { el: 'tape lane track', board: 'main > .card[style*="padding: 8px 12px 6px"] > .row.ce.g8:nth-of-type(2) > .grow', live: '.tp-tape > .row:nth-of-type(2) .tp-lane', free: { y: BELOW } },
  { el: 'tape lane count', board: 'main > .card[style*="padding: 8px 12px 6px"] > .row.ce.g8:nth-of-type(2) > .ra', live: '.tp-tape > .row:nth-of-type(2) .tp-lane-n', free: { w: TEXT_W }, free: { y: BELOW } },
  { el: 'tape axis', board: 'main > .card[style*="padding: 8px 12px 6px"] > .row.ce.g8:last-child .grow', live: '.tp-axis', free: { y: BELOW } },
  { el: 'tape card', board: 'main > .card[style*="padding: 8px 12px 6px"]', live: '.tape-panel', free: { h: T_ROWS, y: BELOW } },
  { el: 'records', board: 'main > .card[style*="--c"]', live: '.tbl-wrap', free: { h: T_ROWS, y: 'it follows the tape' } },
];
const TBL = 'main > .card[style*="--c"]';
const COLS = ['offset', 'p', 'time', 'key', 'value', 'hdr'];
export const TOPIC_TABLE: Probe[] = [
  { el: 'records head', board: `${TBL} > .r.h`, live: '#messages-table thead tr', free: { y: BELOW } },
  ...COLS.map((c, i): Probe => ({ el: `head ${c}`, board: `${TBL} > .r.h > span`, live: '#messages-table thead th', nth: i, text: true, free: { y: BELOW } })),
  { el: 'record', board: `${TBL} > .r:not(.h):not([style])`, live: '#messages-table tbody tr[id]', free: { y: T_ROWS } },
  ...COLS.map((c, i): Probe => ({ el: `cell ${c}`, board: `${TBL} > .r:not(.h):not([style]) > span`, live: '#messages-table tbody tr[id] > td', nth: i, free: { y: T_ROWS } })),
  { el: 'table foot', board: `${TBL} > .r[style]`, live: '.tbl-foot', free: { y: T_ROWS } },
  { el: 'table foot note', board: `${TBL} > .r[style] > .note`, live: '.tbl-foot > span:first-child', free: { y: T_ROWS, w: TEXT_W } },
];
export { TOPIC_COMMON };
