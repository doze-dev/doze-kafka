import { expect, type Page } from '@playwright/test';
import type { Probe } from './compare';
import { AFTER_TEXT, ROWS_H, TEXT_W } from './probes';

// A topic page with a message open, as Sy-01 to Sy-04 draw it: the list, the id band,
// the facts, the neighbours, the tabs, the records, the composer, the wire and the
// inspector. The board's records are rows of spans (.r); the console's a table.

export const TOPIC = 'seed-orders';

const FACT_H = 'its height is the facts sentence, which carries the seed\'s numbers and times';
const BELOW_FACTS = 'it is below the facts sentence, whose height is the seed\'s numbers and times';
// Under the tabs the console draws Tp-05's tape and Tp-14's search and jumps, which
// Sy-01 and Sy-02 do not: what comes after them is lower by their height.
const BELOW_TAPE = 'it is below Tp-05\'s tape and Tp-14\'s search and jumps (which Sy-01 to Sy-03 do not draw), and below the seed\'s facts and neighbours';
// The records fill the page and the composer sits at its foot, as Tp-05 draws them.
const AT_FOOT = 'it sits at the page\'s foot under records that fill the page, as Tp-05 draws it; Sy-01\'s five sample records end higher';
const OPEN_ROW = 'the open record is the seed\'s, wherever it is in the list';
const VALUE_H = 'its height is the record\'s value, which is the seed\'s';
const BELOW_VALUE = 'it is below the record\'s value, whose height is the seed\'s';

export const TOPIC_PAGE: Probe[] = [
  { el: 'list', board: '.list', live: 'aside.list, nav.list' },
  { el: 'list head', board: '.list > :first-child', live: '.list > :first-child' },
  { el: 'list cap', board: '.list .cap', live: '.list .cap', text: true },
  { el: 'list item', board: '.list .li:not(.on)', live: '.list .li:not(.on)' },
  { el: 'list item on', board: '.list .li.on', live: '.list .li.on' },
  { el: 'list name', board: '.list .li:not(.on) > :first-child', live: '.list .li:not(.on) > :first-child', free: { w: TEXT_W } },
  { el: 'list number', board: '.list .li:not(.on) > :last-child', live: '.list .li:not(.on) > :last-child', free: { w: TEXT_W, x: 'right-aligned: its x is its width\'s' } },
  { el: 'list foot', board: '.list > .note', live: '.list > .note, .list .list-foot' },
  { el: 'main', board: 'main.main', live: 'main.main' },
  { el: 'id band', board: 'main > .row.sb', live: 'main > .idband' },
  { el: 'h1', board: 'main h1', live: 'main h1', free: { w: TEXT_W } },
  { el: 'primary button', board: 'main > .row.sb .btn.pri', live: 'main > .idband .btn.pri', text: true },
  { el: 'more button', board: 'main .btn[aria-label="More actions"]', live: 'main .btn[aria-label="More actions"]' },
  { el: 'facts', board: 'main > :nth-child(2)', live: '#topic-facts', free: { h: FACT_H } },
  { el: 'neighbours', board: 'main > .row.wrap', live: '#neighbours', free: { y: BELOW_FACTS, h: 'its height is the seed\'s neighbours, how many and how long their names are' } },
  { el: 'neighbour', board: 'main a.well', live: '#neighbours a.well', free: { w: TEXT_W, y: BELOW_FACTS } },
  { el: 'neighbour note', board: 'main a.well .note', live: '#neighbours a.well .note', text: true, free: { y: BELOW_FACTS } },
  { el: 'neighbour chip', board: 'main a.well .sv', live: '#neighbours a.well .sv', free: { x: AFTER_TEXT, y: BELOW_FACTS } },
  { el: 'tabs', board: '.tabs', live: 'main .tabs', free: { y: BELOW_FACTS } },
  { el: 'tab on', board: '.tab.on', live: 'main .tab.on', text: true, free: { y: BELOW_FACTS } },
  { el: 'tab', board: '.tab:not(.on)', live: 'main .tab:not(.on)', text: true, free: { y: BELOW_FACTS } },
  { el: 'records', board: 'main > .card:not(.pad)', live: 'main .tbl-wrap', free: { y: BELOW_TAPE, h: ROWS_H } },
  { el: 'records head', board: 'main > .card .r.h', live: '#messages-table thead tr', free: { y: BELOW_TAPE } },
  { el: 'records head cell', board: 'main > .card .r.h > span', live: '#messages-table thead th', nth: 1, text: true, free: { y: BELOW_TAPE } },
  { el: 'record', board: 'main > .card .r:not(.h):not(.on)', live: '#messages-table tbody tr[id^="msg-"]:not(.on)', free: { y: BELOW_TAPE } },
  { el: 'record open', board: 'main > .card .r.on', live: '#messages-table tbody tr.on', free: { y: OPEN_ROW } },
  { el: 'record offset', board: 'main > .card .r:not(.h):not(.on) > span', live: '#messages-table tbody tr[id^="msg-"]:not(.on) > td', nth: 1, free: { y: BELOW_TAPE } },
  { el: 'record key', board: 'main > .card .r:not(.h):not(.on) > span', live: '#messages-table tbody tr[id^="msg-"]:not(.on) > td', nth: 2, free: { y: BELOW_TAPE } },
  { el: 'record value', board: 'main > .card .r:not(.h):not(.on) > span', live: '#messages-table tbody tr[id^="msg-"]:not(.on) > td', nth: 3, free: { y: BELOW_TAPE } },
  { el: 'record age', board: 'main > .card .r:not(.h):not(.on) > span', live: '#messages-table tbody tr[id^="msg-"]:not(.on) > td', nth: 4, free: { y: BELOW_TAPE } },
  { el: 'composer', board: 'main > .card.pad', live: '#compose-panel', free: { y: AT_FOOT } },
  { el: 'composer title', board: 'main > .card.pad .b', live: '#compose-panel .t', text: true, free: { y: AT_FOOT } },
  { el: 'composer note', board: 'main > .card.pad .note', live: '#compose-panel .sub', text: true, free: { y: AT_FOOT } },
  { el: 'composer key', board: 'main > .card.pad input', live: '#compose-quick input.inp', free: { y: AT_FOOT } },
  { el: 'composer value', board: 'main > .card.pad input', live: '#compose-quick input.inp', nth: 1, free: { y: AT_FOOT } },
  { el: 'composer send', board: 'main > .card.pad .btn.pri', live: '#compose-quick .btn.pri', text: true, free: { y: AT_FOOT } },
  { el: 'dock', board: '.dock', live: '.dock' },
  { el: 'dock head', board: '.dock .dh', live: '.dock .dh' },
  { el: 'dock row', board: '.dock .r', live: '.dock .r:not(.h)' },
  { el: 'inspector', board: 'aside.insp', live: 'aside.insp:not([hidden])' },
];

/** The inspector's message, as Sy-02 draws it. */
export const MESSAGE: Probe[] = [
  { el: 'inspector head', board: '.insp .hd', live: '#inspector .inspector-h' },
  { el: 'inspector cap', board: '.insp .hd .cap', live: '#inspector .inspector-h .cap', text: true },
  { el: 'inspector title', board: '.insp .hd .h2', live: '#inspector-title', free: { w: TEXT_W } },
  { el: 'inspector sub', board: '.insp .hd .note', live: '#inspector .inspector-sub', free: { w: TEXT_W } },
  { el: 'inspector body', board: '.insp .bd', live: '#inspector .inspector-b' },
  { el: 'fields', board: '.insp .bd > .kv', live: '#inspector .inspector-b > .kv' },
  { el: 'field name', board: '.insp .bd > .kv > span:not(.m)', live: '#inspector .inspector-b > .kv > span:not(.m)' },
  { el: 'value cap', board: '.insp .bd > .cap', live: '#inspector .inspector-b .cap' },
  { el: 'value', board: '.insp pre.code', live: '#inspector pre.code:not([style*="none"])', free: { h: VALUE_H } },
  { el: 'callout', board: '.insp .callout', live: '#inspector .callout', free: { y: BELOW_VALUE } },
  { el: 'follow', board: '.insp .btn.pri', live: '#inspector .btn.pri', text: true, free: { y: BELOW_VALUE } },
];

/**
 * The record Sy-02 opens is an order with a traceparent and a source header; the
 * seed's orders in partition 0 are (offset 39 is the newest of them).
 */
export const TRACED = `/topics/${TOPIC}?partition=0&open=39`;

/** Waits for the record named in the address to be open in the inspector. */
export async function recordOpen(page: Page) {
  await expect(page.locator('#inspector-title')).toBeVisible();
  await atTop(page);
}

/**
 * The boards draw the page at its top with nothing focused. Opening a record scrolls
 * it into view and moves focus into the inspector; both are put back.
 */
export async function atTop(page: Page) {
  await page.waitForTimeout(300);
  await page.evaluate(() => {
    document.querySelectorAll('main, .tbl-wrap').forEach((e) => { (e as HTMLElement).scrollTop = 0; });
    (document.activeElement as HTMLElement | null)?.blur();
  });
  await page.mouse.move(0, 0);
  await page.waitForTimeout(200);
}

/** Opens the topic page with its newest record open in the inspector. */
export async function openMessage(page: Page) {
  const row = page.locator('#messages-table tbody tr[id^="msg-"]').first();
  await row.click();
  await expect(page.locator('#inspector-title')).toBeVisible();
  await atTop(page);
}

/**
 * The board's numbered markers (span.num, and Sy-01's "1 Rail" line that is only a
 * marker and its label) are annotations: "The numbers on the left mark each part."
 * They are taken out of the board before it is measured, as the console has none.
 */
export async function dropMarkers(page: Page) {
  await page.evaluate(() => {
    document.querySelectorAll('nav.rail > .row').forEach((e) => e.remove());
    document.querySelectorAll('.num').forEach((n) => {
      const row = n.parentElement!;
      n.remove();
      // A row that only put the marker beside a part (the facts, the tabs) is the part.
      if (row.matches('main > .row') && row.children.length === 1 && row.firstElementChild!.classList.contains('grow')) { const part = row.firstElementChild!; part.classList.remove('grow'); row.replaceWith(part); }
    });
  });
}
