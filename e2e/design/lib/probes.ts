import type { Probe } from './compare';

// Probes shared by every page: the top bar, the rail and the keys strip are the same
// on each board, and so are a section home's list pane, id band and table.

// A width that is its text: the boards' sample names are not the seed's.
export const TEXT_W = 'its width is its text, and the text is the seed\'s, not the board\'s sample';
// A position after such an element.
export const AFTER_TEXT = 'it sits after text whose width is the seed\'s, not the board\'s sample';
// The jump field follows the brand, whose label is the broker's own name.
export const BRAND_X = 'it follows the brand, whose label is this broker\'s name (the board\'s is "harbour")';

export const SHELL: Probe[] = [
  { el: 'top bar', board: '.top', live: 'header.topbar' },
  { el: 'brand', board: '.brand', live: 'a.brand', free: { w: TEXT_W } },
  { el: 'logo', board: '.logo', live: '.logo', text: true },
  { el: 'brand name', board: '.brand > span:nth-child(2)', live: '.brand-name', text: true },
  { el: 'brand label', board: '.brand .mut', live: '.brand .sub', free: { w: TEXT_W, x: AFTER_TEXT } },
  { el: 'jump', board: '.jump', live: '.tb-palette', free: { x: BRAND_X } },
  { el: 'jump text', board: '.jump .grow', live: '.tb-palette .grow:not([hidden])', text: true, free: { x: BRAND_X } },
  { el: 'jump key', board: '.jump .k', live: '.tb-palette .kbd', text: true, free: { x: BRAND_X } },
  { el: 'broker pill', board: '.top .pill', live: '#live-pill', free: { w: TEXT_W, x: 'it is right-aligned after the address, whose width is the port the test broker got' } },
  { el: 'broker dot', board: '.top .pill .dot', live: '#live-pill .dot', free: { x: 'it is in the broker pill, placed by the address\'s width' } },
  { el: 'compat pill', board: '.top .pill:last-child', live: '#compat-pill', text: true, free: { x: 'it sits between the broker\'s address, whose width is the port the test broker got, and Sy-07\'s theme switch (probed on its own)' } },
  { el: 'theme switch', board: '.top .seg', live: 'header .theme-seg' },
  { el: 'rail', board: 'nav.rail', live: 'nav.rail' },
  { el: 'rail cap', board: 'nav.rail > .cap', live: 'nav.rail > .cap', text: true },
  { el: 'rail cap 2', board: 'nav.rail > .cap', live: 'nav.rail > .cap', nth: 1, text: true },
  { el: 'rail cap 3', board: 'nav.rail > .cap', live: 'nav.rail > .cap', nth: 2, text: true },
  { el: 'rail item', board: 'nav.rail a.nav:not(.on)', live: 'nav.rail a.nav:not(.on)' },
  { el: 'rail item on', board: 'nav.rail a.nav.on', live: 'nav.rail a.nav.on' },
  { el: 'rail chip', board: 'nav.rail a.nav .sv', live: 'nav.rail a.nav .sv', text: true },
  { el: 'rail count', board: 'nav.rail a.nav .n', live: 'nav.rail a.nav .n', free: { w: TEXT_W, x: 'right-aligned: its x is its width\'s' } },
  { el: 'rail connect', board: 'nav.rail a.nav:last-child', live: 'nav.rail a.nav:last-of-type', text: true },
  { el: 'keys strip', board: '.keys', live: '#keys', text: true },
  { el: 'key', board: '.keys .k', live: '#keys .kbd', text: true },
];

/** A section home: the list pane, the id band, the summary, the table and the notes. */
export const HOME: Probe[] = [
  { el: 'list', board: '.list', live: 'aside.list' },
  { el: 'list head', board: '.list > :first-child', live: 'aside.list > :first-child' },
  { el: 'list cap', board: '.list .cap', live: 'aside.list .cap', text: true },
  { el: 'list item', board: '.list .li:not(.on)', live: 'aside.list .li:not(.on)' },
  { el: 'list item on', board: '.list .li.on', live: 'aside.list .li.on' },
  { el: 'list name', board: '.list .li:not(.on) > :first-child', live: 'aside.list .li:not(.on) > :first-child', free: { w: TEXT_W } },
  { el: 'list number', board: '.list .li:not(.on) > :last-child', live: 'aside.list .li:not(.on) > :last-child', free: { w: TEXT_W, x: 'right-aligned: its x is its width\'s' } },
  { el: 'list foot', board: '.list > .note, .list > :last-child.note', live: 'aside.list > .note, aside.list .list-foot' },
  { el: 'main', board: 'main.main', live: 'main.main' },
  { el: 'id band', board: 'main > .row.sb', live: 'main > .idband' },
  { el: 'h1', board: 'main h1', live: 'main h1', text: true },
  { el: 'h1 note', board: 'main h1 + .note, main h1 + span', live: 'main h1 + .note, main h1 + span', text: true },
  { el: 'primary button', board: 'main .btn.pri', live: 'main .btn.pri', free: { x: 'right-aligned: its x is its width\'s' } },
  { el: 'more button', board: 'main .btn[aria-label="More actions"]', live: 'main .btn[aria-label="More actions"]' },
  { el: 'head pill', board: 'main > .row.sb .pill', live: 'main > .idband .pill' },
  { el: 'summary', board: 'main > .row.ce.g8', live: 'main > .row.ce.g8, main > .summary' },
  { el: 'summary item', board: 'main > .row.ce.g8 > .m', live: 'main > .row.ce.g8 > .m, main > .summary > .m', free: { w: TEXT_W } },
  { el: 'card', board: 'main .card', live: 'main .card' },
  { el: 'card 2', board: 'main .card', live: 'main .card', nth: 1 },
  { el: 'table head', board: 'main .card .r.h', live: 'main .card .r.h' },
  { el: 'head cell', board: 'main .card .r.h > span', live: 'main .card .r.h > span', text: true },
  { el: 'row', board: 'main .card .r:not(.h)', live: 'main .card .r:not(.h)' },
  { el: 'cell 1', board: 'main .card .r:not(.h) > :nth-child(1)', live: 'main .card .r:not(.h) > :nth-child(1)' },
  { el: 'cell 2', board: 'main .card .r:not(.h) > :nth-child(2)', live: 'main .card .r:not(.h) > :nth-child(2)' },
  { el: 'cell 3', board: 'main .card .r:not(.h) > :nth-child(3)', live: 'main .card .r:not(.h) > :nth-child(3)' },
  { el: 'callout', board: 'main .callout', live: 'main .callout' },
  { el: 'callout 2', board: 'main .callout', live: 'main .callout', nth: 1 },
  { el: 'callout title', board: 'main .callout .b', live: 'main .callout .b' },
  { el: 'callout note', board: 'main .callout .note', live: 'main .callout .note' },
  { el: 'field', board: 'main .inp, main input', live: 'main .inp, main input' },
  { el: 'segment', board: 'main .seg', live: 'main .seg' },
  { el: 'segment on', board: 'main .seg .on', live: 'main .seg .on' },
  { el: 'pill in main', board: 'main .pill', live: 'main .pill' },
  { el: 'foot note', board: 'main > .note:last-child', live: 'main > .note:last-child', text: true },
  { el: 'dock', board: '.dock', live: '.dock' },
  { el: 'dock head', board: '.dock .dh', live: '.dock .dh' },
  { el: 'dock row', board: '.dock .r', live: '.dock .r:not(.h)' },
];
