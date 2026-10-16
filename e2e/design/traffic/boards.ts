// The Traffic boards and the pairs the gate compares on each: the board's element, the
// live page's, and which of them hold data (compared with the ledger by a truth check,
// truth.ts) rather than the board's copy.

import type { Page } from '@playwright/test';
import type { Pair } from '../lib/diff';
import type { Ledger } from '../lib/ledger';
import { polls } from './truth';

export type Board = {
  id: string;
  /** The board's file under e2e/design/boards, without .dc.html. */
  file: string;
  /** Puts the live page into the board's state. */
  state: (page: Page, base: string, ledger: Ledger) => Promise<void>;
  pairs: Pair[];
  /** The truth checks this board's data pairs name. */
  truths: string[];
  /** When in the run the board is measured: on the empty broker, with the scenario's
   *  traffic (the default), or with the broker stopped. */
  phase?: 'empty' | 'traffic' | 'lost';
};

const M = '.app > .body > main';
const I = '.app > .body > .insp';
const L = '#detail';
const P = '#inspector-panel';
// The live inspector: the page's own summary, or a selection, whichever is drawn.
const PD = `${P} .insp-default`;
const PS = `${P} #inspector`;

// The page's head: the title, the line under it, and the live pill with Pause (while
// the broker is lost, the pill says since when: the time is checked by "lastgood").
const headOf = (lost: boolean): Pair[] => [
  { id: 'head', board: `${M} > .row.sb`, live: `${L} > .tf-head`, skip: ['t'] },
  { id: 'title', board: `${M} > .row.sb .h1`, live: `${L} > .tf-head .h1` },
  { id: 'subtitle', board: `${M} > .row.sb > .col > .note`, live: `${L} > .tf-head > .col > .note` },
  { id: 'controls', board: `${M} > .row.sb > .row`, live: `${L} #traffic-controls`, skip: ['t'] },
  lost ? { id: 'controls items', board: `${M} > .row.sb > .row > *`, live: `${L} #traffic-controls > *`, data: { truth: 'lastgood', geometry: ['x', 'w'] } }
    : { id: 'controls items', board: `${M} > .row.sb > .row > *`, live: `${L} #traffic-controls > *` },
  lost ? { id: 'controls inner', board: `${M} > .row.sb > .row > * > *`, live: `${L} #traffic-controls > * > *`, data: { truth: 'lastgood', geometry: ['x', 'w'] } }
    : { id: 'controls inner', board: `${M} > .row.sb > .row > * > *`, live: `${L} #traffic-controls > * > *` },
];
const head = headOf(false);

// The summary line ("5 clients connected · 412 requests in the last minute · 3 errors ·
// kept: the last 2,000, in memory only"): its numbers are the ledger's.
const summary: Pair[] = [
  { id: 'summary', board: `${M} > .row.wrap:not(:has(> .cap))`, live: `${L} > #traffic-summary`, skip: ['t'] },
  { id: 'summary items', board: `${M} > .row.wrap:not(:has(> .cap)) > span`, live: `${L} > #traffic-summary > span`, data: { truth: 'summary', geometry: ['x', 'w'] } },
];

// The filter row: the field, the kinds, and what is at its right.
const FR = `${M} > .row.ce.g8:not(.wrap):not(.sb)`;
const filter = (right: Pair[] = [], moves?: string): Pair[] => [
  { id: 'filter row', board: FR, live: `${L} > .tf-filter`, skip: ['t'] },
  { id: 'filter field', board: `${FR} > .inp`, live: `${L} > .tf-filter .inp`, ...(moves ? { moved: { truth: moves, props: ['x', 'w'] as const } } : {}) },
  { id: 'kinds', board: `${FR} > .seg`, live: `${L} > .tf-filter > .seg`, ...(moves ? { moved: { truth: moves, props: ['x', 'w'] as const } } : {}) },
  { id: 'kinds items', board: `${FR} > .seg > *`, live: `${L} > .tf-filter > .seg > *`, ...(moves ? { moved: { truth: moves, props: ['x'] as const } } : {}) },
  ...right,
];
const foldNote: Pair = { id: 'filter note', board: `${FR} > .note`, live: `${L} > .tf-filter > .note` };

// The table: the card, its header and its rows. The rows' words are the ledger's
// (truth "rows"); their tracks, type, colours and size are the board's.
const C = `${M} > .card`;
const LC = `${L} #traffic-live > .card`;
const table = (opts: { okRow?: string; errRow?: string; denyRow?: string; cardData?: string } = {}): Pair[] => {
  const pairs: Pair[] = [
    opts.cardData ? { id: 'card', board: C, live: LC, data: { truth: opts.cardData, geometry: ['h'] } } : { id: 'card', board: C, live: LC, skip: ['t'] },
    { id: 'header', board: `${C} > .r.h`, live: `${LC} > .r.h` },
    { id: 'header cells', board: `${C} > .r.h > span`, live: `${LC} > .r.h > span` },
    { id: 'first row', board: `${C} > .r:not(.h)`, live: `${LC} .tf-rows > .r`, n: 1, data: { truth: 'rows', geometry: ['h'] }, skip: ['bg'] },
  ];
  if (opts.okRow) pairs.push({ id: 'ok row cells', board: `${C} > ${opts.okRow} > span`, live: `${LC} .tf-rows > .r:not(.errrow):not(.on) > span`, n: 7, data: { truth: 'rows' }, skip: ['y'] });
  if (opts.errRow) pairs.push({ id: 'refused row', board: `${C} > ${opts.errRow}`, live: `${LC} .tf-rows > .r.errrow.refused`, n: 1, data: { truth: 'rows' }, skip: ['y', 'h'] });
  if (opts.errRow) pairs.push({ id: 'refused row cells', board: `${C} > ${opts.errRow} > span`, live: `${LC} .tf-rows > .r.errrow.refused > span`, n: 7, data: { truth: 'rows' }, skip: ['y', 'h'] });
  if (opts.denyRow) pairs.push({ id: 'denied row', board: `${C} > ${opts.denyRow}`, live: `${LC} .tf-rows > .r.errrow.denied`, n: 1, data: { truth: 'rows' }, skip: ['y', 'h'] });
  return pairs;
};

const foot: Pair = { id: 'foot note', board: `${M} > .note`, live: `${L} > #traffic-note` };

const keys: Pair[] = [
  { id: 'keys', board: '.app > .keys', live: '#keys' },
  { id: 'keys items', board: '.app > .keys > span', live: '#keys > span' },
  { id: 'keys caps', board: '.app > .keys .k', live: '#keys .kbd' },
];

// The inspector's head and body, as the boards draw .hd and .bd.
const insp = (live: string, opts: { headData?: string; bodyKids?: boolean; bodyData?: boolean } = {}): Pair[] => [
  { id: 'panel', board: I, live: P, skip: ['t'] },
  { id: 'panel head', board: `${I} > .hd`, live: `${live} > .hd`, ...(opts.headData ? { data: { truth: opts.headData, geometry: ['h'] as const } } : { skip: ['t'] as const }) },
  { id: 'panel head items', board: `${I} > .hd > *`, live: `${live} > .hd > :not(.tf-close)`, ...(opts.headData ? { data: { truth: opts.headData, geometry: ['w', 'h'] as const } } : {}) },
  { id: 'panel body', board: `${I} > .bd`, live: `${live} > .bd`, ...(opts.headData ? { data: { truth: opts.headData, geometry: ['y', 'h'] as const } } : { skip: ['t'] as const }) },
  ...(opts.bodyKids === false ? [] : [(opts.bodyData && opts.headData ? { id: 'panel body items', board: `${I} > .bd > *`, live: `${live} > .bd > *`, data: { truth: opts.headData, geometry: ['y', 'h'] } } : { id: 'panel body items', board: `${I} > .bd > *`, live: `${live} > .bd > *`, skip: ['t'] }) as Pair]),
];

// The connected clients (Main's panel): one block a client.
const connected: Pair[] = [
  ...insp(PD, { headData: 'connected', bodyKids: false }),
  { id: 'panel header', board: `${I} > .bd > .r.h`, live: `${P} #clients-list > .r.h` },
  { id: 'panel header cells', board: `${I} > .bd > .r.h > span`, live: `${P} #clients-list > .r.h > span` },
  { id: 'client block', board: `${I} > .bd > .col`, live: `${P} #clients-list > .tf-conn`, n: 1, data: { truth: 'connected' } },
  { id: 'client lines', board: `${I} > .bd > .col:nth-child(2) > *`, live: `${P} #clients-list > .tf-conn:nth-child(2) > *`, data: { truth: 'connected', geometry: ['w'] } },
  { id: 'metrics callout', board: `${I} > .bd > .callout`, live: `${P} #clients-list > .callout`, data: { truth: 'connected', geometry: ['y'] } },
  { id: 'metrics callout lines', board: `${I} > .bd > .callout > *`, live: `${P} #clients-list > .callout > *`, data: { truth: 'connected', geometry: ['y'] } },
];

// Opens the newest row of a client's request (a folded one, or not) that the client's
// ledger vouches for: the row is picked by what it is (the client, the API, and the time
// the broker read the request, which must lie within a request of that client and API in
// the ledger), never by where it is on the page. The page puts a new row at the top every
// few hundred milliseconds, so a row that is in view now is gone from view by the next
// poll; the click scrolls the table to the row (it stays among the page's 200 until a
// hundred newer ones come) and the table goes back to its top before the page is measured.
async function openRow(page: Page, ledger: Ledger, client: string, api: string, folded: boolean, result = '') {
  const want = new RegExp(`^${api} v\\d+${folded ? ' ×\\d+' : ''}$`);
  let last = '';
  for (let i = 0; i < 60; i++) {
    const rows = await page.evaluate(() => Array.from(document.querySelectorAll('#traffic-live .tf-rows > .r[data-row]')).map((r) => {
      const s = r.children as unknown as HTMLElement[];
      return { id: r.id, client: s[1].innerText.trim(), request: s[2].innerText.trim(), result: s[5]?.innerText.trim() ?? '', at: Date.parse(s[0].querySelector('time')?.getAttribute('datetime') ?? s[0].getAttribute('datetime') ?? '') };
    }));
    ledger.read();
    const mine = ledger.reqs.filter((r) => r.client === client && r.api === api && !r.err);
    const hit = rows.find((r) => r.client === client && want.test(r.request) && (!result || r.result === result) && mine.some((q) => r.at >= q.start - 1 && r.at <= q.upper + 1));
    last = `${rows.length} rows on the page, ${mine.length} such requests in the ledger`;
    if (hit) {
      try {
        await page.locator(`[id="${hit.id}"]`).click({ timeout: 3000 });
        await page.locator('#inspector [data-traffic-insp]').waitFor({ timeout: 3000 });
      } catch { await page.waitForTimeout(200); continue; } // a poll replaced the row under the click: pick again.
      await page.evaluate(() => {
        for (let el: Element | null = document.querySelector('#traffic-live .tf-rows'); el; el = el.parentElement) el.scrollTop = 0;
        window.scrollTo(0, 0);
      });
      return;
    }
    await page.waitForTimeout(500);
  }
  throw new Error(`no ${client} ${api} row the ledger vouches for was on the page (${last})`);
}

async function openClient(page: Page, client: string) {
  await page.locator('#clients-table .tf-rows > .r', { has: page.locator('> span:first-child', { hasText: new RegExp(`^${client}$`) }) }).first().click();
  await page.locator('#inspector [data-traffic-insp]').waitFor();
}

// The tabs of the clients view (Traf-06, 07, 11).
const tabs: Pair[] = [
  { id: 'tabs', board: `${M} > .tabs`, live: `${L} > .tabs`, skip: ['t'] },
  { id: 'tab', board: `${M} > .tabs > .tab`, live: `${L} > .tabs > .tab`, data: { truth: 'clients', geometry: ['w'] } },
];

// The clients table (Traf-06 at nine columns, Traf-07 and 11 at four).
const CC = `${L} #clients-table`;
const clientsTable = (cells: number): Pair[] => [
  { id: 'card', board: C, live: CC, data: { truth: 'clients', geometry: ['h'] } },
  { id: 'header', board: `${C} > .r.h`, live: `${CC} > .r.h` },
  { id: 'header cells', board: `${C} > .r.h > span`, live: `${CC} > .r.h > span` },
  { id: 'first row', board: `${C} > .r:not(.h)`, live: `${CC} .tf-rows > .r`, n: 1, data: { truth: 'clients' }, skip: ['bg'] },
  { id: 'consumer row cells', board: `${C} > .r:nth-child(3) > *`, live: `${CC} .tf-rows > .r:has(> span:first-child a[href$="client=order-validator"]) > *`, n: cells, data: { truth: 'clients' }, skip: ['y'] },
];

export const BOARDS: Board[] = [
  {
    id: 'Traf-09', file: 'Traf-09-Empty', phase: 'empty',
    state: async (page, base) => { await page.goto(`${base}traffic`); },
    pairs: [
      ...head,
      { id: 'summary', board: `${M} > .row.wrap`, live: `${L} > #traffic-summary`, skip: ['t'] },
      { id: 'summary items', board: `${M} > .row.wrap > span`, live: `${L} > #traffic-summary > span` },
      { id: 'card', board: C, live: `${L} #traffic-empty`, data: { truth: 'empty', geometry: ['h'] } },
      // The card's sentences hold the broker's own address and when it began (truth
      // "empty", which pins their words): the start time and that a restart empties the
      // page are the page's (recorded), a line longer than the board's, which moves what
      // is under it; the SASL sentence is drawn when the listener has SASL.
      { id: 'card parts', board: `${C} > *`, live: `${L} #traffic-empty > *`, data: { truth: 'empty', geometry: ['y', 'h'] } },
      { id: 'heading', board: `${C} > .col:nth-child(1) > *`, live: `${L} #traffic-empty > .col:nth-child(1) > *`, data: { truth: 'empty', geometry: ['h'] } },
      { id: 'the line', board: `${C} > .col:nth-child(2) > :not(.note)`, live: `${L} #traffic-empty > .col:nth-child(2) > :not(.note)`, data: { truth: 'empty', geometry: ['y'] } },
      { id: 'steps lines', board: `${C} > .col:nth-child(3) > *`, live: `${L} #traffic-empty > .col:nth-child(3) > *`, data: { truth: 'empty', geometry: ['y'] } },
      { id: 'steps', board: `${C} .row.g8 > .num`, live: `${L} #traffic-empty .row.g8 > .num`, moved: { truth: 'empty', props: ['y'] } },
      { id: 'buttons', board: `${C} > .row > .btn`, live: `${L} #traffic-empty > .row > .btn`, moved: { truth: 'empty', props: ['y'] } },
      ...keys,
    ],
    truths: ['empty'],
  },
  {
    id: 'Main', file: 'Main',
    state: async (page, base) => { await page.goto(`${base}traffic`); },
    pairs: [...head, ...summary, ...filter([foldNote]), ...table({ okRow: '.r:not(.h):not(.on):not([style])', errRow: '.r[style*="badbg"]' }), foot, ...connected, ...keys],
    truths: ['summary', 'rows', 'connected', 'lastgood'],
  },
  {
    id: 'Traf-02', file: 'Traf-02-Filter',
    state: async (page, base) => { await page.goto(`${base}traffic?topic=harbour.checkout.orders`); },
    pairs: [
      ...head, ...summary, ...filter([foldNote]),
      { id: 'pills row', board: `${M} > .row.wrap:has(> .cap)`, live: `${L} > #filter-pills`, skip: ['t'] },
      { id: 'pills row items', board: `${M} > .row.wrap:has(> .cap) > *`, live: `${L} > #filter-pills > *`, data: { truth: 'filtered', geometry: ['x', 'w'] } },
      { id: 'pill parts', board: `${M} > .row.wrap:has(> .cap) > .pill > *`, live: `${L} > #filter-pills > .pill > *`, skip: ['x'] },
      ...table({ okRow: '.r:not(.h):not(.on)' }),
      // The line under the table says what the filter looks at in rows and requests
      // (AD15): its words are pinned by the truth check, with its count.
      { ...foot, data: { truth: 'filtered' } },
      ...insp(PD, { headData: 'touched' }),
      { id: 'touched facts', board: `${I} > .bd > .kv > *`, live: `${PD} #touched > .kv > *`, data: { truth: 'touched', geometry: ['w', 'h', 'y'] } },
      { id: 'touched well', board: `${I} > .bd > .well > *`, live: `${PD} #touched > .well > *`, skip: ['y'] },
      ...keys,
    ],
    truths: ['summary', 'rows', 'filtered', 'touched', 'lastgood'],
  },
  {
    id: 'Traf-03', file: 'Traf-03-Errors',
    state: async (page, base) => { await page.goto(`${base}traffic?kind=errors`); },
    pairs: [
      ...head, ...summary, ...filter([{ id: 'refused count', board: `${FR} > span.m`, live: `${L} > .tf-filter > #refused-count`, data: { truth: 'errors', geometry: ['x', 'w'] } }]),
      ...table({ denyRow: '.r[style*="denybg"]', cardData: 'errors' }), { ...foot, data: { truth: 'errors', geometry: ['y', 'h'] } },
      ...insp(PD, { headData: 'errors', bodyData: true }),
      { id: 'what to do', board: `${I} > .bd > .col.g8 > a.well`, live: `${PD} #error-summary > .col.g8 > a.well`, data: { truth: 'errors', geometry: ['y'] } },
      { id: 'what to do lines', board: `${I} > .bd > .col.g8 > a.well > .col > *`, live: `${PD} #error-summary > .col.g8 > a.well > .col > *`, data: { truth: 'errors', geometry: ['y', 'w'] } },
      { id: 'refusal callout', board: `${I} > .bd > .callout > *`, live: `${PD} #error-summary > .callout > *`, data: { truth: 'errors', geometry: ['h', 'y', 'w'] } },
      { id: 'refusal facts', board: `${I} > .bd > .kv > *`, live: `${PD} #error-summary > .kv > *`, data: { truth: 'errors', geometry: ['y', 'w'] } },
      ...keys,
    ],
    truths: ['summary', 'rows', 'errors', 'lastgood'],
  },
  {
    id: 'Traf-04', file: 'Traf-04-Request',
    state: async (page, base, ledger) => { await page.goto(`${base}traffic`); await openRow(page, ledger, 'checkout-api', 'Produce', false, 'ok'); },
    pairs: [
      ...head, ...summary, ...filter([foldNote]), ...table({ okRow: '.r:not(.h):not(.on):not([style])' }), foot,
      ...insp(PS, { headData: 'request', bodyData: true }),
      { id: 'head line', board: `${I} > .hd > .row > *`, live: `${PS} > .hd > .row > *`, data: { truth: 'request', geometry: ['x', 'w'] } },
      { id: 'request facts', board: `${I} > .bd > .kv > *`, live: `${PS} > .bd > .kv > *`, data: { truth: 'request', geometry: ['w', 'h', 'y'] } },
      { id: 'partitions', board: `${I} > .bd .card > .r`, live: `${PS} #result-per-partition .card > .r`, data: { truth: 'request', geometry: ['y'] } },
      { id: 'partition cells', board: `${I} > .bd .card > .r > span`, live: `${PS} #result-per-partition .card > .r > span`, data: { truth: 'request', geometry: ['y'] } },
      { id: 'decoded', board: `${I} > .bd pre.code`, live: `${PS} #decoded pre.code`, data: { truth: 'request', geometry: ['h', 'y', 'w'] } },
      { id: 'follow', board: `${I} > .bd > a.btn`, live: `${PS} > .bd > a.btn` },
      ...keys,
    ],
    truths: ['summary', 'rows', 'request', 'lastgood'],
  },
  {
    id: 'Traf-05', file: 'Traf-05-Folded',
    state: async (page, base, ledger) => { await page.goto(`${base}traffic`); await openRow(page, ledger, 'order-validator', 'Heartbeat', true); },
    pairs: [
      ...head, ...summary, ...filter([foldNote]), ...table({ okRow: '.r:not(.h):not(.on):not([style])' }), foot,
      ...insp(PS, { headData: 'folded' }),
      { id: 'head line', board: `${I} > .hd > .row > *`, live: `${PS} > .hd > .row > *`, data: { truth: 'folded', geometry: ['x', 'w'] } },
      { id: 'fold facts', board: `${I} > .bd > .kv > *`, live: `${PS} > .bd > .kv > *`, data: { truth: 'folded', geometry: ['w'] } },
      { id: 'ticks', board: `${I} > .bd .well`, live: `${PS} #fold-ticks .well`, data: { truth: 'folded' } },
      { id: 'tick', board: `${I} > .bd .well .tick`, live: `${PS} #fold-ticks .well .tick`, n: 1, skip: ['x'] },
      { id: 'why', board: `${I} > .bd > .callout > *`, live: `${PS} > .bd > .callout > *` },
      { id: 'buttons', board: `${I} > .bd > .row > .btn`, live: `${PS} > .bd > .row > .btn`, data: { truth: 'folded', geometry: ['x', 'w'] } },
      ...keys,
    ],
    truths: ['summary', 'rows', 'folded', 'lastgood'],
  },
  {
    id: 'Traf-06', file: 'Traf-06-Clients',
    state: async (page, base) => { await page.goto(`${base}traffic?view=clients`); },
    pairs: [
      ...head, ...tabs, ...summary, ...clientsTable(9),
      { id: 'look row', board: `${C} > .r[style*="warnbg"]`, live: `${CC} .tf-rows > .r.tf-look`, n: 1, data: { truth: 'clients' }, skip: ['y'] },
      { id: 'callout', board: `${M} > .callout > *`, live: `${L} #attention > *`, data: { truth: 'clients', geometry: ['y', 'w', 'h'] } },
      { id: 'foot note', board: `${M} > .note`, live: `${L} > #traffic-note`, data: { truth: 'clients', geometry: ['y', 'h'] } },
      ...keys,
    ],
    truths: ['summary', 'clients', 'lastgood'],
  },
  {
    id: 'Traf-07', file: 'Traf-07-Client',
    state: async (page, base) => { await page.goto(`${base}traffic?view=clients`); await openClient(page, 'order-validator'); },
    pairs: [
      ...head, ...tabs, ...clientsTable(4),
      { id: 'foot note', board: `${M} > .note`, live: `${L} > #traffic-note`, skip: ['y'] },
      ...insp(PS, { headData: 'client', bodyData: true }),
      { id: 'head line', board: `${I} > .hd > .row > *`, live: `${PS} > .hd > .row > *`, data: { truth: 'client', geometry: ['x', 'w'] } },
      { id: 'metrics', board: `${I} > .bd > div[style*="grid"]`, live: `${PS} .tf-metrics`, data: { truth: 'client', geometry: ['y', 'h'] } },
      { id: 'metric rows', board: `${I} > .bd > div[style*="grid"] > *`, live: `${PS} .tf-metrics > *`, data: { truth: 'client', geometry: ['y', 'w', 'h'] } },
      { id: 'buttons', board: `${I} > .bd > .row > .btn`, live: `${PS} > .bd > .row > .btn`, data: { truth: 'client', geometry: ['x', 'w'] } },
      // "m all 136 metrics": the count is the client's (truth "client").
      // "m all 136 metrics": the count is the client's (truth "client"), and moves what
      // follows it.
      { id: 'keys', board: '.app > .keys', live: '#keys', skip: ['t'] },
      { id: 'keys items', board: '.app > .keys > span', live: '#keys > span', data: { truth: 'client', geometry: ['x', 'w'] } },
      { id: 'keys words', board: '.app > .keys > span:not(:nth-child(3))', live: '#keys > span:not(:nth-child(3))', moved: { truth: 'client', props: ['x'] } },
      { id: 'keys caps', board: '.app > .keys .k', live: '#keys .kbd', moved: { truth: 'client', props: ['x', 'w'] } },
    ],
    truths: ['clients', 'client', 'lastgood'],
  },
  {
    id: 'Traf-08', file: 'Traf-08-Paused',
    state: async (page, base, ledger) => {
      polls.length = 0;
      page.on('request', (r) => {
        if (!r.url().includes('/traffic/new')) return;
        const q: { sent: number; got?: number; n: number; atLeast: boolean } = { sent: Date.now(), n: NaN, atLeast: false };
        r.response().then(async (res) => { const j = await res?.json(); q.n = j?.n; q.atLeast = !!j?.atLeast; q.got = Date.now(); }).catch(() => {});
        polls.push(q);
      });
      await page.goto(`${base}traffic`);
      await openRow(page, ledger, 'depot-router', 'OffsetCommit', false);
      await page.locator('#detail .h1').click();
      await page.keyboard.press('p');
      await page.waitForTimeout(3000);
    },
    pairs: [
      ...head,
      { id: 'banner', board: `${M} > .callout`, live: `${L} > #paused-banner`, skip: ['t'] },
      { id: 'banner items', board: `${M} > .callout > *`, live: `${L} > #paused-banner > *`, data: { truth: 'paused', geometry: ['w'] } },
      ...filter([foldNote]), ...table({ okRow: '.r:not(.h):not(.on):not([style])' }),
      { id: 'foot note', board: `${M} > .note`, live: `${L} > #traffic-note`, data: { truth: 'paused' } },
      ...insp(PS, { headData: 'request' }),
      { id: 'head line', board: `${I} > .hd > .row > *`, live: `${PS} > .hd > .row > *`, data: { truth: 'request', geometry: ['x', 'w'] } },
      { id: 'request facts', board: `${I} > .bd > .kv > *`, live: `${PS} > .bd > .kv > *`, data: { truth: 'request', geometry: ['w', 'h', 'y'] } },
      ...keys,
    ],
    truths: ['rows', 'request', 'paused', 'lastgood'],
  },
  {
    id: 'Traf-11', file: 'Traf-11-Metrics-None',
    state: async (page, base) => { await page.goto(`${base}traffic?view=clients`); await openClient(page, 'quiet-app'); },
    pairs: [
      ...head, ...tabs, ...clientsTable(4),
      ...insp(PS, { headData: 'client' }),
      { id: 'head line', board: `${I} > .hd > .row > *`, live: `${PS} > .hd > .row > *`, data: { truth: 'client', geometry: ['x', 'w'] } },
      { id: 'callout', board: `${I} > .bd > .callout > *`, live: `${PS} #no-metrics > *`, data: { truth: 'client', geometry: ['h', 'y'] } },
      { id: 'reasons', board: `${I} > .bd > .col.g6 > *`, live: `${PS} > .bd > .col.g6 > *`, skip: ['y'] },
      { id: 'buttons', board: `${I} > .bd > .row > .btn`, live: `${PS} > .bd > .row > .btn`, skip: ['y'] },
      ...keys,
    ],
    truths: ['clients', 'client', 'lastgood'],
  },
  {
    id: 'Traf-12', file: 'Traf-12-Rebalance-Story',
    state: async (page, base) => { await page.goto(`${base}traffic?group=g-depot-router`); },
    pairs: [
      ...head,
      ...filter([
        { id: 'pill', board: `${FR} > .pill`, live: `${L} > .tf-filter > .pill`, moved: { truth: 'story', props: ['x', 'w'] } },
        { id: 'clear', board: `${FR} > a`, live: `${L} > .tf-filter > a.b`, moved: { truth: 'story', props: ['x'] } },
        { id: 'count', board: `${FR} > span.m`, live: `${L} > .tf-filter > #story-count`, data: { truth: 'story', geometry: ['x', 'w'] } },
      ], 'story'),
      { id: 'card', board: C, live: `${L} #traffic-story`, data: { truth: 'story', geometry: ['h'] } },
      { id: 'header', board: `${C} > .r.h`, live: `${L} #traffic-story > .r.h` },
      { id: 'header cells', board: `${C} > .r.h > span`, live: `${L} #traffic-story > .r.h > span` },
      { id: 'rebalance row', board: `${C} > .r.on`, live: `${L} #traffic-story .tf-rebalance`, n: 1, data: { truth: 'story' } },
      { id: 'rebalance cells', board: `${C} > .r.on > span`, live: `${L} #traffic-story .tf-rows > .tf-rebalance:first-child > span`, data: { truth: 'story', geometry: ['w'] } },
      { id: 'nested cells', board: `${C} > .r:not(.h):not(.on):not(.kid):nth-child(3) > span`, live: `${L} #traffic-story .tf-rows > .r[data-row]:not(.kid):has(> .tf-result.ok) > span`, n: 6, data: { truth: 'story' }, skip: ['y'] },
      { id: 'foot note', board: `${M} > .note`, live: `${L} > #traffic-note`, data: { truth: 'story', geometry: ['y'] } },
      ...insp(PD, { headData: 'story', bodyData: true }),
      { id: 'why', board: `${I} > .bd > .callout > *`, live: `${PD} #group-summary > .callout > *`, n: 2, data: { truth: 'story', geometry: ['h', 'y'] } },
      { id: 'generations header', board: `${I} > .bd .card > .r.h`, live: `${PD} #group-summary .card > .r.h`, moved: { truth: 'story', props: ['y'] } },
      { id: 'newest generation', board: `${I} > .bd .card > .r.on`, live: `${PD} #group-summary .card > .r.on`, data: { truth: 'story', geometry: ['y'] } },
      { id: 'timeline button', board: `${I} > .bd > a.btn`, live: `${PD} #group-summary > a.btn` },
      ...keys,
    ],
    truths: ['rows', 'story', 'lastgood'],
  },
  {
    id: 'Traf-10', file: 'Traf-10-Disconnected', phase: 'lost',
    state: async (page, base) => { await page.goto(`${base}traffic`); },
    pairs: [
      ...headOf(true),
      { id: 'lost callout', board: `${M} > .callout`, live: `${L} > #lost-callout`, skip: ['t'] },
      { id: 'lost callout items', board: `${M} > .callout > *`, live: `${L} > #lost-callout > *`, data: { truth: 'lastgood' } },
      ...filter(),
      { id: 'card', board: C, live: LC, skip: ['t'] },
      { id: 'header', board: `${C} > .r.h`, live: `${LC} > .r.h` },
      { id: 'header cells', board: `${C} > .r.h > span`, live: `${LC} > .r.h > span` },
      { id: 'frozen line', board: `${C} > .r:nth-child(2)`, live: `${LC} > .r.tf-frozen`, data: { truth: 'lastgood' } },
      { id: 'rows', board: `${C} > div[style*="opacity"]`, live: `${LC} > .tf-rows`, skip: ['t', 'h'] },
      { id: 'foot note', board: `${M} > .note`, live: `${L} > #traffic-note` },
      ...keys,
    ],
    truths: ['lastgood'],
  },
];
