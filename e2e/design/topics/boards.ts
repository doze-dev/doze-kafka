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
  { el: 'tape lane count', board: 'main > .card[style*="padding: 8px 12px 6px"] > .row.ce.g8:nth-of-type(2) > .ra', live: '.tp-tape > .row:nth-of-type(2) .tp-lane-n', free: { w: TEXT_W, y: BELOW } },
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

// ---------- Tp-06: the message inspector ----------
const I = '#inspector';
const VALUE_H = 'its height is the record\'s value, which is the seed\'s';
const INSP_FACT = 'its text is the broker\'s: the record\'s time, key and writer, not the board\'s sample';
const M = (n: number, board: string, live: string, extra: Partial<Probe> = {}): Probe => ({ el: `x${n}`, board, live, ...extra });
const FIELDS = ['timestamp', 'type', 'key', 'sizes', 'written by'];
/** Tp-06's part of the page (the inspector, the table beside it); the shared parts come from TOPIC_COMMON. */
export const MESSAGE_PANEL: Probe[] = [
  { el: 'ins head', board: '.insp .hd', live: `${I} .inspector-h` },
  { el: 'ins cap', board: '.insp .hd .cap', live: `${I} .inspector-h .cap`, text: true },
  { el: 'ins controls', board: '.insp .hd > .row > .row.g4', live: `${I} .inspector-nav` },
  ...['older', 'newer', 'close'].map((n, i): Probe => ({ el: `ins ${n}`, board: '.insp .hd .btn.sm.ghost', live: `${I} .inspector-nav .btn`, nth: i, text: true })),
  { el: 'ins title', board: '.insp .hd .h2', live: '#inspector-title', free: { w: TEXT_W } },
  { el: 'ins sub', board: '.insp .hd .m.mut', live: `${I} .inspector-sub`, free: { w: TEXT_W } },
  { el: 'ins body', board: '.insp .bd', live: `${I} .inspector-b` },
  { el: 'ins fields', board: '.insp .bd > .kv', live: `${I} .tp-fields`, free: { h: 'the page\'s record may carry a schema row, which the board\'s does not' } },
  ...FIELDS.flatMap((f, i): Probe[] => [
    { el: `ins field ${f}`, board: '.insp .bd > .kv > span:not(.m)', live: `${I} .tp-fields > span:not(.m)`, nth: i, text: true },
    { el: `ins value ${f}`, board: '.insp .bd > .kv > .m', live: `${I} .tp-fields > .m`, nth: i, free: { w: INSP_FACT } },
  ]),
  { el: 'ins follow row', board: '.insp .bd > .row.ce.g8', live: `${I} .tp-msg > .row.ce.g8:not(.inspector-acts)` },
  { el: 'ins follow', board: '.insp .bd > .row.ce.g8 .btn', live: `${I} #inspector-trace`, text: true },
  { el: 'ins follow note', board: '.insp .bd > .row.ce.g8 .note', live: `${I} .tp-msg > .row.ce.g8 .note`, text: true },
  { el: 'ins headers', board: '.insp .bd > .col.g4', live: `${I} .tp-msg > .col.g4`, free: { h: 'its rows are the record\'s headers' } },
  { el: 'ins headers cap', board: '.insp .bd > .col.g4 .cap', live: `${I} .tp-msg > .col.g4 .cap`, text: true, free: { y: INSP_FACT } },
  { el: 'ins headers list', board: '.insp .bd > .col.g4 .kv', live: `${I} .hdrkv`, free: { h: 'its rows are the record\'s headers', y: INSP_FACT } },
  { el: 'ins header key', board: '.insp .bd > .col.g4 .kv > span', live: `${I} .hdrkv > span`, free: { y: INSP_FACT } },
  { el: 'ins header value', board: '.insp .bd > .col.g4 .kv > span', live: `${I} .hdrkv > span`, nth: 1, free: { y: INSP_FACT } },
  { el: 'ins value col', board: '.insp .bd > .col.g4', live: `${I} .tp-msg > .col.g4`, nth: 1, free: { h: VALUE_H, y: INSP_FACT } },
  { el: 'ins value head', board: '.insp .bd > .col.g4:nth-of-type(2) > .row.sb', live: `${I} .tp-msg > .col.g4:nth-of-type(2) > .row.sb`, free: { y: INSP_FACT } },
  { el: 'ins value cap', board: '.insp .bd > .col.g4:nth-of-type(2) .cap', live: `${I} .tp-msg > .col.g4:nth-of-type(2) .cap`, text: true, free: { y: INSP_FACT, w: INSP_FACT } },
  { el: 'ins switch', board: '.insp .seg', live: `${I} .seg`, free: { y: INSP_FACT } },
  ...['text', 'hex', 'base64'].map((n, i): Probe => ({ el: `ins switch ${n}`, board: '.insp .seg > *', live: `${I} .seg > *`, nth: i, text: true, free: { y: INSP_FACT } })),
  { el: 'ins code', board: '.insp pre.code', live: `${I} pre.code:not([style*="none"])`, free: { h: VALUE_H, y: INSP_FACT } },
  { el: 'ins actions', board: '.insp .bd > .row.g8.ce:nth-child(5)', live: `${I} .inspector-acts`, free: { y: VALUE_H } },
  ...['send again', 'edit a copy', 'copy'].map((n, i): Probe => ({ el: `ins ${n}`, board: '.insp .bd > .row.g8.ce:nth-child(5) > *', live: `${I} .inspector-acts > .btn`, nth: i, text: true, free: { y: VALUE_H } })),
  { el: 'ins menu', board: '.insp .bd > .card.col', live: `${I} .tp-copymenu`, free: { y: VALUE_H } },
  ...[0, 1, 2, 3, 4].map((i): Probe => ({ el: `ins menu item ${i}`, board: '.insp .bd > .card.col > *', live: `${I} .tp-copymenu > *`, nth: i, text: true, free: { y: VALUE_H } })),
];
const REC = 'main > .card[style*="--c"]';
const CELLS6 = ['offset', 'p', 'key', 'value'];
/** The records beside the open inspector: Offset, P, Key and Value (Time and Hdr are the inspector's). */
export const MESSAGE_TABLE: Probe[] = [
  { el: 'records', board: REC, live: '.tbl-wrap', free: { h: T_ROWS, y: 'it follows the tape' } },
  { el: 'records head', board: `${REC} > .r.h`, live: '#messages-table thead tr', free: { y: BELOW } },
  ...CELLS6.map((c, i): Probe => ({ el: `head ${c}`, board: `${REC} > .r.h > span`, live: '#messages-table thead th:not([style*="none"])', nth: i, text: true, free: { y: BELOW } })),
  { el: 'record open', board: `${REC} > .r.on`, live: '#messages-table tbody tr.on', free: { y: T_ROWS } },
  ...CELLS6.map((c, i): Probe => ({ el: `cell ${c}`, board: `${REC} > .r.on > span`, live: '#messages-table tbody tr.on > td', nth: i, free: { y: T_ROWS } })),
];

// ---------- Tp-10: the composer ----------
const CARD = 'main > .card[style*="flex: 1"]';
const HEAD = `${CARD} > .row.sb.ce:first-child`;
const LB = `${CARD} > .row.g16 > .col.g12:first-child`;
const RB = `${CARD} > .row.g16 > .col.g12.grow`;
const AT_FOOT10 = 'it sits at the page\'s foot under the records, which fill the page, as the card does on every topic page; the board draws three sample records above it';
const COMPOSE_Y = 'the composer is below the page\'s records, so every y in it is lower than the board\'s by their height; the console\'s card is as tall as its content';
const CY = { y: COMPOSE_Y } as const;
/** Tp-10: the open composer, every part of it, in the state the board draws: a key, a JSON value, two headers, acks 1. */
export const COMPOSE: Probe[] = [
  { el: 'composer', board: CARD, live: '#compose-panel', free: { y: AT_FOOT10, h: 'the board\'s card fills the space under three records; the console\'s is its content' } },
  { el: 'c head', board: HEAD, live: '#compose-head', free: CY },
  { el: 'c head title', board: `${HEAD} .h2`, live: '#compose-head .h2', text: true, free: CY },
  { el: 'c head topic', board: `${HEAD} .m.mut`, live: '#compose-head .open-only .m', free: { ...CY, w: TEXT_W } },
  { el: 'c head note', board: `${HEAD} .note`, live: '#compose-head .open-only .note', text: true, free: CY },
  { el: 'c fold', board: `${HEAD} .btn.sm.ghost`, live: '#compose-head .btn.sm', text: true, free: CY },
  { el: 'c body', board: `${CARD} > .row.g16`, live: '.tp-cp-body', free: { ...CY, h: 'its height is its taller column' } },
  { el: 'c left', board: LB, live: '.tp-cp-left', free: { ...CY, h: 'its height is its fields, which carry the console\'s own extras (the schema picker, the empty-key switch)' } },
  { el: 'c part col', board: `${LB} > .row.g12 > .col.g4:first-child`, live: '.tp-cp-part', free: { ...CY, h: 'its note is the sentence for this topic\'s partitions' } },
  { el: 'c part label', board: '#l-part', live: '#l-part', text: true, free: CY },
  { el: 'c part seg', board: '[aria-labelledby="l-part"]', live: '[aria-labelledby="l-part"]', free: CY },
  { el: 'c part on', board: '[aria-labelledby="l-part"] .on', live: '[aria-labelledby="l-part"] .on', text: true, free: CY },
  { el: 'c part off', board: '[aria-labelledby="l-part"] > span:not(.on)', live: '[aria-labelledby="l-part"] > button:not(.on)', text: true, free: CY },
  { el: 'c part note', board: `${LB} > .row.g12 > .col.g4:first-child > .note`, live: '.tp-cp-part > .note', free: { ...CY, h: TEXT_H, w: 'its text is wrapped by the column' } },
  { el: 'c key col', board: `${LB} > .row.g12 > .col.g4.grow`, live: '.tp-cp-left > .row.g12 > .col.g4.grow', free: { ...CY, h: 'it is as tall as the partition column beside it' } },
  { el: 'c key label', board: 'label[for="c-key"]', live: 'label[for="f-key"]', free: { ...CY, w: 'the console\'s label also carries the empty-key switch' } },
  { el: 'c key optional', board: 'label[for="c-key"] .note', live: 'label[for="f-key"] .note', text: true, free: CY },
  { el: 'c key b64', board: 'label[for="c-key"] .tag', live: 'label[for="f-key"] .tp-tag', text: true, free: CY },
  { el: 'c key', board: '#c-key', live: '#f-key', free: CY },
  { el: 'c acks label', board: '#l-acks', live: '#l-acks', text: true, free: { ...CY, h: 'it is the field of the line above' } },
  { el: 'c acks seg', board: '[aria-labelledby="l-acks"]', live: '[aria-labelledby="l-acks"]', free: CY },
  { el: 'c acks on', board: '[aria-labelledby="l-acks"] .on', live: '[aria-labelledby="l-acks"] .on', text: true, free: CY },
  { el: 'c value col', board: `${LB} > .col.g4:last-child`, live: '.tp-ord2', free: { ...CY, h: 'its height is the note under the textarea' } },
  { el: 'c value row', board: `${LB} > .col.g4:last-child > .row.sb.ce`, live: '.tp-ord2 > .row.sb.ce', free: CY },
  { el: 'c value label', board: 'label[for="c-val"]', live: 'label[for="f-value"]', text: true, free: CY },
  { el: 'c value b64', board: 'label[for="c-val"] .tag', live: 'label[for="f-value"] .tp-tag', text: true, free: CY },
  { el: 'c mode seg', board: '[aria-label="Value mode"]', live: '[aria-label="Value mode"]', free: CY },
  { el: 'c mode on', board: '[aria-label="Value mode"] .on', live: '[aria-label="Value mode"] .on', text: true, free: CY },
  { el: 'c value', board: '#c-val', live: '#f-value', free: CY },
  { el: 'c valid', board: `${LB} > .col.g4:last-child > .note`, live: '#compose-valid', free: { ...CY, w: TEXT_W } },
  { el: 'c right', board: RB, live: '.tp-cp-body > .col.g12.grow', free: { ...CY, h: 'it is as tall as its content' } },
  { el: 'c headers col', board: `${RB} > .col.g4:first-child`, live: '.tp-cp-body > .col.g12.grow > .col.g4:first-child', free: { ...CY, h: 'the console adds the Rows/JSON switch under the rows' } },
  { el: 'c headers label', board: '#l-hdrs', live: '#l-hdrs', text: true, free: CY },
  { el: 'c header key 0', board: 'input[aria-label="Header key"]', live: 'input[aria-label="Header key"]', free: CY },
  { el: 'c header value 0', board: 'input[aria-label="Header value"]', live: 'input[aria-label="Header value"]', free: { ...CY, w: 'it takes the rest of the row' } },
  { el: 'c header b64 0', board: 'button[aria-label="Value is base64"]', live: 'button[aria-label="b64 header value: the value is base64"]', text: true, free: { ...CY, x: 'it follows the value, whose width is the row\'s rest' } },
  { el: 'c header remove 0', board: 'button[aria-label="Remove header"]', live: 'button[aria-label="Remove header"]', text: true, free: CY },
  { el: 'c header key 1', board: 'input[aria-label="Header key"]', nth: 1, live: 'input[aria-label="Header key"]', free: CY },
  { el: 'c add header', board: `${RB} > .col.g4:first-child > .btn.sm`, live: '.tp-cp-body > .col.g12.grow > .col.g4:first-child .btn.sm.tp-fit', text: true, free: CY },
  { el: 'c sent col', board: `${RB} > .col.g4:nth-child(2)`, live: '.tp-cp-body > .col.g12.grow > .col.g4:has(.tp-cp-sent)', free: CY },
  { el: 'c sent cap', board: `${RB} > .col.g4:nth-child(2) .cap`, live: '.tp-cp-body > .col.g12.grow > .col.g4:has(.tp-cp-sent) .cap', text: true, free: CY },
  { el: 'c sent kv', board: `${RB} .kv`, live: '.tp-cp-sent', free: CY },
  { el: 'c callout', board: `${RB} > .callout`, live: '.tp-cp-body > .col.g12.grow > .callout:not([style*="none"])', free: { ...CY, h: TEXT_H } },
  { el: 'c callout title', board: `${RB} > .callout .b`, live: '.tp-cp-body > .col.g12.grow > .callout:not([style*="none"]) .b', free: CY },
  { el: 'c foot', board: `${CARD} > .row.sb.ce:last-child`, live: '.tp-cp-foot', free: CY },
  { el: 'c foot note', board: `${CARD} > .row.sb.ce:last-child > .note`, live: '.tp-cp-foot > .note:not([style*="none"])', text: true, free: CY },
  { el: 'c clear', board: `${CARD} > .row.sb.ce:last-child .btn:not(.pri)`, live: '.tp-cp-foot .btn:not(.pri)', text: true, free: CY },
  { el: 'c send', board: `${CARD} > .row.sb.ce:last-child .btn.pri`, live: '.tp-cp-foot .btn.pri', free: { ...CY, w: 'the console draws the send glyph before the word' } },
];
