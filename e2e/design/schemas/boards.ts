import type { Probe } from '../lib/compare';
import { SHELL, TEXT_W, TEXT_H } from '../lib/probes';

// The Schemas boards and what the gate compares on each. What is data (the addresses in
// the producer's lines, the registry's requests) is held to the broker in schemas.spec.ts;
// the probes compare the page's shape, type and colour with the board's.

const ADDR = 'its lines carry this broker\'s and this registry\'s addresses, which are the test broker\'s ports, not the board\'s 9092 and 8081';

/** The registry's wire dock at the foot of a Schemas page. */
export const DOCK: Probe[] = [
  { el: 'dock', board: '.dock', live: '#wire-dock' },
  { el: 'dock head', board: '.dock .dh', live: '#wire-dock .dh' },
  { el: 'dock title', board: '.dock .dh > .b', live: '#wire-dock .dh > .b', text: true },
  { el: 'dock pill', board: '.dock .dh > .pill', live: '#wire-dock .dh > .pill', text: true },
  { el: 'dock note', board: '.dock .dh > .note', live: '#wire-dock .dh > .note', text: true },
  { el: 'dock table head', board: '.dock .r.h', live: '#wire-dock .r.h' },
  { el: 'dock head cell', board: '.dock .r.h > span', live: '#wire-dock .r.h > span', text: true },
  { el: 'dock row', board: '.dock .r:not(.h)', live: '#wire-dock .r[data-reg-row]' },
  { el: 'dock time', board: '.dock .r:not(.h) > :nth-child(1)', live: '#wire-dock .r[data-reg-row] > :nth-child(1)', free: { w: TEXT_W } },
  { el: 'dock method', board: '.dock .r:not(.h) > :nth-child(2)', live: '#wire-dock .r[data-reg-row] > :nth-child(2)' },
  { el: 'dock path', board: '.dock .r:not(.h) > :nth-child(3)', live: '#wire-dock .r[data-reg-row] > :nth-child(3)' },
  { el: 'dock status', board: '.dock .r:not(.h) > :nth-child(4)', live: '#wire-dock .r[data-reg-row] > :nth-child(4)' },
];

/** Sc-01: no subject registered. */
export const NONE: Probe[] = [
  ...SHELL,
  { el: 'list', board: '.list', live: 'aside.list' },
  { el: 'list filter', board: '.list > :first-child', live: '.list > .list-filter' },
  { el: 'list none', board: '.list > .col.g4', live: '.list .list-none' },
  { el: 'list none title', board: '.list > .col.g4 > .b', live: '.list .list-none > .b', text: true },
  { el: 'list none note', board: '.list > .col.g4 > .note', live: '.list .list-none > .note', text: true },
  { el: 'main', board: 'main.main', live: 'main.main' },
  { el: 'id band', board: 'main > .row.sb', live: 'main > .idband' },
  { el: 'h1', board: 'main h1', live: 'main h1', text: true },
  { el: 'h1 note', board: 'main h1 + .note', live: 'main h1 + .note', free: { w: TEXT_W, text: ADDR } },
  { el: 'register button', board: 'main > .row.sb .btn.pri', live: 'main > .idband .btn.pri', text: true },
  { el: 'lead', board: 'main > .callout', live: 'main > .sc-none-lead' },
  { el: 'lead title', board: 'main > .callout > .b', live: 'main > .sc-none-lead > .b', text: true },
  { el: 'lead text', board: 'main > .callout > span:last-child', live: 'main > .sc-none-lead > span:last-child', text: true },
  { el: 'steps row', board: 'main > .row.g12', live: 'main > .sc-none' },
  { el: 'steps card', board: 'main > .row.g12 > .card', live: 'main > .sc-none > .card', free: { h: 'its height is the producer\'s lines, which are this broker\'s (with SASL on, the login lines are added)' } },
  { el: 'steps title', board: 'main > .row.g12 > .card .h2', live: 'main > .sc-none > .card .h2', text: true },
  { el: 'step 1', board: 'main > .row.g12 > .card .row.g8', live: 'main > .sc-none > .card .row.g8', text: true },
  { el: 'step number', board: 'main > .row.g12 > .card .num', live: 'main > .sc-none > .card .num', text: true },
  { el: 'producer lines', board: 'main > .row.g12 > .card pre.code', live: 'main > .sc-none > .card pre.code', free: { text: ADDR, w: ADDR, h: 'its lines are this broker\'s: the board draws the SASL login lines, a broker without SASL has none' } },
  { el: 'step 2', board: 'main > .row.g12 > .card .row.g8', live: 'main > .sc-none > .card .row.g8', nth: 1, free: { y: 'it follows the producer\'s lines', h: TEXT_H, text: 'it names a topic this broker has, or <topic> when it has none; the board names harbour.checkout.orders' } },
  { el: 'step 3', board: 'main > .row.g12 > .card .row.g8', live: 'main > .sc-none > .card .row.g8', nth: 2, text: true, free: { y: 'it follows the producer\'s lines' } },
  { el: 'side', board: 'main > .row.g12 > .col', live: 'main > .sc-none > .sc-none-side' },
  { el: 'side callout', board: 'main > .row.g12 > .col .callout', live: 'main > .sc-none > .sc-none-side .callout' },
  { el: 'side callout 2', board: 'main > .row.g12 > .col .callout', live: 'main > .sc-none > .sc-none-side .callout', nth: 1 },
  { el: 'side title', board: 'main > .row.g12 > .col .callout .b', live: 'main > .sc-none > .sc-none-side .callout .b', text: true },
  { el: 'side note', board: 'main > .row.g12 > .col .callout .note', live: 'main > .sc-none > .sc-none-side .callout .note', text: true },
  { el: 'side button', board: 'main > .row.g12 > .col .callout .btn', live: 'main > .sc-none > .sc-none-side .callout .btn', text: true },
  ...DOCK,
];

const SAMPLE = 'it is data: the board\'s sample times, counts and neighbours are not this broker\'s';

/** A subject's list pane, title band, facts line, neighbours and tabs (Sc-05 to Sc-08, Sc-18). */
export const SUBJECT_HEAD: Probe[] = [
  ...SHELL,
  { el: 'list', board: '.list', live: 'aside.list' },
  { el: 'list filter', board: '.list > :first-child', live: '.list > .list-filter' },
  { el: 'list item on', board: '.list .li.on', live: '.list .li.on' },
  { el: 'list item', board: '.list .li:not(.on)', live: '.list .li:not(.on)' },
  { el: 'list name', board: '.list .li:not(.on) .m.el', live: '.list .li:not(.on) .m.el', text: true },
  { el: 'list pill', board: '.list .li:not(.on) .pill', live: '.list .li:not(.on) .pill', text: true },
  { el: 'main', board: 'main.main', live: 'main.main' },
  { el: 'id band', board: 'main > .row.sb', live: 'main > .idband' },
  { el: 'h1', board: 'main h1', live: 'main h1', text: true },
  { el: 'type pill', board: 'main > .row.sb .pill', live: 'main > .idband .pill', text: true },
  { el: 'register button', board: 'main > .row.sb .btn.pri', live: 'main > .idband .btn.pri', text: true },
  { el: 'more button', board: 'main > .row.sb .btn[aria-label="More actions"]', live: 'main > .idband .btn[aria-label="More actions"]' },
  { el: 'facts', board: 'main > .row.ce.g8.wrap', live: '#subject-facts', free: { w: SAMPLE } },
  { el: 'fact', board: 'main > .row.ce.g8.wrap > .m', live: '#subject-facts > .m', text: true },
  { el: 'neighbours', board: 'main > .well', live: '#neighbours' },
  { el: 'neighbours cap', board: 'main > .well > .cap', live: '#neighbours > .cap', text: true },
  { el: 'tabs', board: 'main .tabs', live: 'main nav.tabs' },
  { el: 'tab on', board: 'main .tabs .tab.on', live: 'main nav.tabs .tab.on', text: true },
  { el: 'tab', board: 'main .tabs .tab:not(.on)', live: 'main nav.tabs .tab:not(.on)', text: true },
  { el: 'tab 3', board: 'main .tabs .tab:not(.on)', live: 'main nav.tabs .tab:not(.on)', nth: 2, text: true },
];

/** The versions table. */
export const VERSIONS: Probe[] = [
  { el: 'versions', board: 'main > .card:not(.pad)', live: '#versions-table' },
  { el: 'versions head', board: 'main > .card:not(.pad) .r.h', live: '#versions-table .r.h' },
  { el: 'versions head cell', board: 'main > .card:not(.pad) .r.h > span', live: '#versions-table .r.h > span', text: true },
  { el: 'versions head cell 3', board: 'main > .card:not(.pad) .r.h > span', live: '#versions-table .r.h > span', nth: 2, text: true },
  { el: 'versions head cell 5', board: 'main > .card:not(.pad) .r.h > span', live: '#versions-table .r.h > span', nth: 4, text: true },
  { el: 'version row', board: 'main > .card:not(.pad) .r:not(.h)', live: '#versions-table .r:not(.h)' },
  { el: 'version', board: 'main > .card:not(.pad) .r:not(.h) > :nth-child(1)', live: '#versions-table .r:not(.h) > :nth-child(1)', text: true },
  { el: 'version id', board: 'main > .card:not(.pad) .r:not(.h) > :nth-child(2)', live: '#versions-table .r:not(.h) > :nth-child(2)', text: true },
  { el: 'version changed', board: 'main > .card:not(.pad) .r:not(.h) > :nth-child(3)', live: '#versions-table .r:not(.h) > :nth-child(3)', text: true },
  { el: 'version registered', board: 'main > .card:not(.pad) .r:not(.h) > :nth-child(4)', live: '#versions-table .r:not(.h) > :nth-child(4)', free: { text: SAMPLE } },
  { el: 'version checked', board: 'main > .card:not(.pad) .r:not(.h) > :nth-child(5)', live: '#versions-table .r:not(.h) > :nth-child(5)', text: true },
];

/** The schema on show: its heading, buttons and text. */
export const SCHEMA: Probe[] = [
  { el: 'schema head', board: 'main > .row.ce.sb:has(.h2)', live: '#schema-head' },
  { el: 'schema title', board: 'main > .row.ce.sb:has(.h2) .h2', live: '#schema-head .h2', text: true },
  { el: 'schema note', board: 'main > .row.ce.sb:has(.h2) .note', live: '#schema-head .note', text: true },
  { el: 'schema button', board: 'main > .row.ce.sb:has(.h2) .btn', live: '#schema-head .btn', text: true },
  { el: 'schema text', board: 'main pre.code', live: 'main pre.code.sc-text' },
];

/** Try it. */
export const TRY: Probe[] = [
  { el: 'try', board: 'main > .card.pad', live: '#try-it' },
  { el: 'try title', board: 'main > .card.pad .h2', live: '#try-it .h2', text: true },
  { el: 'try note', board: 'main > .card.pad .note', live: '#try-it .note', text: true },
  { el: 'try field', board: 'main > .card.pad .inp', live: '#try-it .inp' },
  { el: 'try button', board: 'main > .card.pad .btn', live: '#try-it .btn', text: true },
  { el: 'try button 2', board: 'main > .card.pad .btn', live: '#try-it .btn', nth: 1, text: true },
  { el: 'try pill', board: 'main > .card.pad .pill', live: '#try-it .try-pill .pill', text: true },
];

/** Sc-05's inspector: one version. */
export const VERSION_INSP: Probe[] = [
  { el: 'inspector', board: 'aside.insp', live: '#inspector-panel' },
  { el: 'inspector head', board: 'aside.insp .hd', live: '#inspector .inspector-h' },
  { el: 'inspector cap', board: 'aside.insp .hd .cap', live: '#inspector .inspector-h .cap', text: true },
  { el: 'inspector title', board: 'aside.insp .hd .h2', live: '#inspector #inspector-title', text: true },
  { el: 'inspector kv', board: 'aside.insp .kv', live: '#inspector .kv' },
  { el: 'inspector key', board: 'aside.insp .kv > :nth-child(1)', live: '#inspector .kv > :nth-child(1)', text: true },
  { el: 'inspector value', board: 'aside.insp .kv > :nth-child(2)', live: '#inspector .kv > :nth-child(2)', text: true },
  { el: 'inspector wire', board: 'aside.insp .callout', live: '#inspector .callout' },
  { el: 'inspector wire title', board: 'aside.insp .callout .b', live: '#inspector .callout .b', text: true },
  { el: 'inspector messages', board: 'aside.insp .bd > .col.g4 > .b', live: '#inspector .versionb > .col.g4 > .b', text: true },
  { el: 'inspector button', board: 'aside.insp .bd > .row.g8 .btn', live: '#inspector .versionb > .row.g8 .btn', text: true },
];

/** Sc-06: the diff. Its facts line is the pair's, not the subject's. */
export const DIFF: Probe[] = [
  ...SUBJECT_HEAD.filter((p) => p.el !== 'facts' && p.el !== 'fact'),
  { el: 'diff facts', board: 'main > .row.ce.g8.wrap', live: '#diff-facts' },
  { el: 'diff fact', board: 'main > .row.ce.g8.wrap > .m', live: '#diff-facts > .m', text: true },
  { el: 'diff fact added', board: 'main > .row.ce.g8.wrap > .m.ok', live: '#diff-facts > .m.ok', text: true },
  { el: 'diff fact removed', board: 'main > .row.ce.g8.wrap > .m.bad', live: '#diff-facts > .m.bad', text: true },
  { el: 'diff fact changed', board: 'main > .row.ce.g8.wrap > .m.warn', live: '#diff-facts > .m.warn', text: true },
  { el: 'diff note', board: 'main > .row.ce.g8.wrap > .note', live: '#diff-facts > .note', text: true },
  { el: 'compare row', board: 'main > .row.sb.ce:has(.seg)', live: 'main > .row.sb.ce:has(.seg)' },
  { el: 'compare title', board: 'main > .row.sb.ce:has(.seg) .h2', live: 'main > .row.sb.ce:has(.seg) .h2', text: true },
  { el: 'compare select', board: 'main > .row.sb.ce:has(.seg) select', live: 'main > .row.sb.ce:has(.seg) select' },
  { el: 'layout', board: 'main .seg', live: 'main .seg' },
  { el: 'layout on', board: 'main .seg .on', live: 'main .seg .on', text: true },
  { el: 'diff', board: 'main > .card', live: '#diff-table' },
  { el: 'diff head', board: 'main > .card .r.h', live: '#diff-table .r.h' },
  { el: 'diff head cell', board: 'main > .card .r.h > span:nth-child(2)', live: '#diff-table .r.h > span:nth-child(2)', free: { text: SAMPLE, w: SAMPLE } },
  { el: 'diff row', board: 'main > .card .r:not(.h)', live: '#diff-table .r:not(.h)' },
  { el: 'diff number', board: 'main > .card .r:not(.h) > :nth-child(1)', live: '#diff-table .r:not(.h) > :nth-child(1)', text: true },
  { el: 'diff line', board: 'main > .card .r:not(.h) > :nth-child(2)', live: '#diff-table .r:not(.h) > :nth-child(2)', text: true },
  { el: 'diff removed', board: 'main > .card .r:not(.h) > :nth-child(2)[style*=badbg]', live: '#diff-table .r:not(.h) > .dl-del:nth-child(2)', text: true },
  { el: 'diff added', board: 'main > .card .r:not(.h) > :nth-child(4)[style*=okbg]', live: '#diff-table .r:not(.h) > .dl-add:nth-child(4)', text: true },
  { el: 'callouts', board: 'main > .row.g12', live: 'main > .row.g12' },
  { el: 'readers', board: 'main > .row.g12 > .callout', live: 'main > .row.g12 > .callout' },
  { el: 'readers title', board: 'main > .row.g12 > .callout .b', live: 'main > .row.g12 > .callout .b', text: true },
  { el: 'levels', board: 'main > .row.g12 > .callout', live: 'main > .row.g12 > .callout', nth: 1 },
  { el: 'levels title', board: 'main > .row.g12 > .callout .b', live: 'main > .row.g12 > .callout .b', nth: 1, text: true },
];
