import { test, expect } from '@playwright/test';
import { BASE_URL } from '../../playwright.config';
import type { Probe } from '../lib/compare';
import { livePage, runBoard, type Place } from '../lib/gate';
import { AFTER_TEXT, BRAND_X, TEXT_W } from '../lib/probes';
import { openBoard, openLive } from '../lib/render';
import { recordOpen, TRACED } from '../lib/topic';
import { ALLOW } from './allow';

// Sy-03 (a topic at 960, a message stacked under the records) and Sy-04 (a topic at
// 390, a message in a sheet) against the live topic page with its newest record open.
// The record open is Sy-02's: an order with a traceparent (lib/topic.ts, TRACED).
// Both boards are drawn in the corner of a 1440 frame at the width they name, so the
// board is rendered at 1440 and the console at 960 or 390; their x and y agree.

const NEWEST = '#messages-table tbody tr[id^="msg-"]';
const FACT_H = 'its height is the facts sentence, which carries the seed\'s numbers and times';
const BELOW_FACTS = 'it is below the facts sentence and the neighbours, which are the seed\'s';
const BELOW_TAPE = 'it is below Tp-05\'s tape and Tp-14\'s search and jumps (which Sy-03 does not draw), and below the seed\'s facts and neighbours';
const MSG_H = 'its height is the open record\'s fields and value, which are the seed\'s';

const TOP_960: Probe[] = [
  { el: 'top bar', board: '.top', live: 'header.topbar' },
  { el: 'brand', board: '.brand', live: 'a.brand', free: { w: TEXT_W } },
  { el: 'brand label', board: '.brand .mut', live: '.brand .sub', free: { w: TEXT_W, x: AFTER_TEXT } },
  { el: 'jump', board: '.jump', live: '.tb-palette', free: { x: BRAND_X } },
  { el: 'jump text', board: '.jump .grow', live: '.tb-palette .grow:not([hidden])', text: true, free: { x: BRAND_X } },
  { el: 'broker pill', board: '.top .pill', live: '#live-pill', free: { w: TEXT_W, x: 'it is right-aligned after the address, whose width is the port the test broker got' } },
  { el: 'compat pill', board: '.top .pill:nth-of-type(2)', live: '#compat-pill' },
  { el: 'theme switch', board: '.top .seg', live: 'header .theme-seg' },
  { el: 'rail', board: 'nav.rail', live: 'nav.rail' },
  { el: 'rail item', board: 'nav.rail a.nav:not(.on)', live: 'nav.rail a.nav:not(.on)' },
  { el: 'rail item on', board: 'nav.rail a.nav.on', live: 'nav.rail a.nav.on' },
  { el: 'rail chip', board: 'nav.rail a.nav .sv', live: 'nav.rail a.nav .sv', text: true },
  { el: 'list', board: '.list', live: 'aside.list, nav.list' },
  { el: 'keys strip', board: '.keys', live: '#keys', text: true },
];

const SY03: Probe[] = [
  ...TOP_960,
  { el: 'main', board: 'main.main', live: 'main.main' },
  { el: 'back row', board: 'main > .row.ce.g8', live: 'main .backrow' },
  { el: 'back', board: 'main > .row.ce.g8 .btn', live: 'main .backrow .btn', text: true },
  { el: 'back note', board: 'main > .row.ce.g8 .note', live: 'main .backrow .note', free: { w: TEXT_W } },
  { el: 'id band', board: 'main > .row.sb', live: 'main > .idband' },
  { el: 'h1', board: 'main h1', live: 'main h1', free: { w: TEXT_W } },
  { el: 'primary button', board: 'main .btn.pri', live: 'main > .idband .btn.pri', text: true },
  { el: 'facts', board: 'main > div:nth-child(3)', live: '#topic-facts', free: { h: FACT_H } },
  { el: 'neighbour', board: 'main a.well', live: '#neighbours a.well', free: { w: TEXT_W, y: BELOW_FACTS } },
  { el: 'tabs', board: '.tabs', live: 'main .tabs', free: { y: BELOW_FACTS } },
  { el: 'tab on', board: '.tab.on', live: 'main .tab.on', text: true, free: { y: BELOW_FACTS } },
  { el: 'records', board: 'main > .card', live: 'main .tbl-wrap', free: { y: BELOW_TAPE, h: 'its height is the seed\'s records that fit' } },
  { el: 'record', board: 'main > .card .r:not(.h):not(.on)', live: `${NEWEST}:not(.on)`, free: { y: BELOW_TAPE } },
  { el: 'message', board: 'main > section.card', live: 'aside.insp:not([hidden])', free: { y: BELOW_TAPE, h: MSG_H } },
  { el: 'message head', board: 'main > section.card > div:first-child', live: '#inspector .inspector-h', free: { y: BELOW_TAPE } },
  { el: 'message title', board: 'main > section.card > div:first-child .m.b, main > section.card > div:first-child .b', live: '#inspector-title', free: { y: BELOW_TAPE, w: TEXT_W } },
  { el: 'message close', board: 'main > section.card .btn.sm', live: '#inspector [data-inspector-close]', text: true, free: { y: BELOW_TAPE } },
  { el: 'message fields', board: 'main > section.card .kv', live: '#inspector .inspector-b > .kv', free: { y: BELOW_TAPE, h: MSG_H } },
  { el: 'message value', board: 'main > section.card pre.code', live: '#inspector pre.code:not([style*="none"])', free: { y: BELOW_TAPE, h: MSG_H } },
  { el: 'dock', board: '.dock', live: '.dock' },
  { el: 'dock head', board: '.dock .dh', live: '.dock .dh' },
  { el: 'dock row', board: '.dock .r', live: '.dock .r:not(.h)' },
];

const SY04: Probe[] = [
  { el: 'top bar', board: '.top', live: 'header.topbar' },
  { el: 'menu', board: '.top .btn', live: '#menu-btn' },
  { el: 'logo', board: '.top .logo', live: '.topbar .logo' },
  { el: 'title', board: '.top .b.grow', live: '#pg-title', text: true },
  { el: 'jump', board: '.top .btn:nth-of-type(2)', live: '.tb-palette', text: true },
  { el: 'broker dot', board: '.top .dot', live: '#live-pill .dot' },
  { el: 'back', board: '.app > div:nth-child(2) > a.note', live: 'main .backrow a, main .backrow .btn', text: true },
  { el: 'h1', board: 'h1', live: 'main h1', free: { w: TEXT_W } },
  { el: 'actions', board: '.app > div:nth-child(2) > .row.g8', live: 'main .idband-acts' },
  { el: 'primary button', board: '.btn.pri', live: '#produce-open', text: true },
  { el: 'more button', board: '.btn[aria-label="More actions"]', live: 'main .btn[aria-label="More actions"]' },
  { el: 'facts', board: '.app > div:nth-child(2) > div.note', live: '#topic-facts', free: { h: FACT_H } },
  { el: 'tabs', board: '.tabs', live: 'main .tabs', free: { y: BELOW_FACTS } },
  { el: 'tab on', board: '.tab.on', live: 'main .tab.on', text: true, free: { y: BELOW_FACTS } },
  { el: 'card', board: 'li.card', live: NEWEST, nth: 1, free: { y: BELOW_TAPE } },
  { el: 'card key', board: 'li.card .m.b', live: `${NEWEST} .cell-key`, nth: 1, free: { y: BELOW_TAPE, w: TEXT_W } },
  { el: 'card value', board: 'li.card > .m.mut', live: `${NEWEST} .cell-val`, nth: 1, free: { y: BELOW_TAPE } },
  { el: 'card age', board: 'li.card > .note', live: `${NEWEST} .cell-age`, nth: 1, free: { y: BELOW_TAPE, w: TEXT_W } },
  { el: 'scrim', board: '.scrim', live: '.insp-scrim, #inspector-scrim' },
  { el: 'sheet', board: 'section.modal', live: 'aside.insp:not([hidden])' },
  { el: 'grab', board: 'section.modal > div:first-child', live: 'aside.insp .grab, aside.insp .insp-grab' },
  { el: 'sheet cap', board: 'section.modal .cap', live: '#inspector .cap', text: true, free: { w: 'it is as wide as the column it heads, which is as wide as the title, the seed\'s record' } },
  { el: 'sheet title', board: 'section.modal .m.b', live: '#inspector-title', free: { w: TEXT_W } },
  { el: 'sheet close', board: 'section.modal .btn.sm', live: '#inspector [data-inspector-close]', text: true },
  { el: 'sheet fields', board: 'section.modal .kv', live: '#inspector .inspector-b > .kv', free: { h: MSG_H } },
  { el: 'sheet value', board: 'section.modal pre.code', live: '#inspector pre.code:not([style*="none"])', free: { y: MSG_H, h: MSG_H } },
  { el: 'sheet actions', board: 'section.modal > .row.g8', live: '#inspector .inspector-acts' },
  { el: 'sheet follow', board: 'section.modal .btn.pri', live: '#inspector .btn.pri', text: true },
  { el: 'keys strip', board: '.keys', live: '#keys' },
];

const at = (size: 960 | 390): Place[] => [{ size, theme: 'light' }, { size, theme: 'dark' }];

/**
 * Sy-03 carries two of the designer's notes in its UI ("stacked here at this width" in
 * the message's head, and "Try it (produce) is the first row of the next screen" in the
 * wire's): they say what the board shows, not what the page says, and are taken out.
 */
async function dropNotes(page: import('@playwright/test').Page) {
  await page.evaluate(() => {
    document.querySelectorAll('main > section.card > div:first-child .note, .dock .dh .note').forEach((e) => e.remove());
  });
}

test('Sy-03 matches a live topic page at 960, a message stacked under the records', async ({ browser }) => {
  test.setTimeout(120_000);
  const out = await runBoard(browser, {
    id: 'Sy-03', file: 'Sy-03-Narrow', boardSize: 1440, places: at(960), boardState: dropNotes,
    live: livePage(BASE_URL, TRACED, recordOpen), probes: SY03, allow: ALLOW,
  });
  expect(out.text, out.text).toBe('');
});

// SCRIM and GRAB (allow.ts): the scrim is the sheet's shadow and the grab handle its
// ::before; each is read from the board and held against the sheet, in both themes.
for (const theme of ['light', 'dark'] as const) {
  test(`Sy-04: the sheet's scrim and grab handle are the board's (${theme})`, async ({ browser }) => {
    const board = await openBoard(browser, 'Sy-04-Phone', 1440, theme);
    const live = await openLive(browser, BASE_URL + TRACED.slice(1), 390, theme);
    await recordOpen(live);
    try {
      const scrim = await board.locator('.scrim').evaluate((e) => getComputedStyle(e).backgroundColor);
      const grab = await board.locator('section.modal > div:first-child').evaluate((e) => {
        const b = e.getBoundingClientRect(), s = getComputedStyle(e), m = e.parentElement!.getBoundingClientRect();
        return { w: b.width, h: b.height, top: Math.round(b.top - m.top), centre: Math.round(b.left + b.width / 2 - (m.left + m.width / 2)), bg: s.backgroundColor, radius: s.borderRadius };
      });
      const sheet = live.locator('aside.insp:not([hidden])');
      const shadow = await sheet.evaluate((e) => getComputedStyle(e).boxShadow);
      expect(shadow, 'the scrim is the board\'s colour, spread over the page').toBe(`${scrim} 0px 0px 0px 100vmax`.replace('100vmax', `${Math.max(390, 844)}px`));
      const before = await sheet.evaluate((e) => {
        const s = getComputedStyle(e, '::before'), m = e.getBoundingClientRect();
        // The ::before is a flex item centred in the sheet: its top is the sheet's border and its margin.
        return { w: parseFloat(s.width), h: parseFloat(s.height), top: Math.round(parseFloat(getComputedStyle(e).borderTopWidth) + parseFloat(s.marginTop)), centre: 0, bg: s.backgroundColor, radius: s.borderRadius, align: s.alignSelf, width: m.width };
      });
      expect({ w: before.w, h: before.h, top: before.top, centre: before.centre, bg: before.bg, radius: before.radius }).toEqual(grab);
      expect(before.align, 'centred').toBe('center');
    } finally {
      await board.context().close();
      await live.context().close();
    }
  });
}

test('Sy-04 matches a live topic page at 390, a message in a sheet', async ({ browser }) => {
  test.setTimeout(120_000);
  const out = await runBoard(browser, {
    id: 'Sy-04', file: 'Sy-04-Phone', boardSize: 1440, places: at(390),
    live: livePage(BASE_URL, TRACED, recordOpen), probes: SY04, allow: ALLOW,
  });
  expect(out.text, out.text).toBe('');
});
