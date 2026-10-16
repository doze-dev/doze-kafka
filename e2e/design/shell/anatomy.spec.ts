import { test, expect, type Page } from '@playwright/test';
import { BASE_URL } from '../../playwright.config';
import { livePage, runBoard } from '../lib/gate';
import { SHELL } from '../lib/probes';
import { openBoard, openLive } from '../lib/render';
import { dropMarkers, MESSAGE, openMessage, recordOpen, TOPIC, TOPIC_PAGE, TRACED } from '../lib/topic';
import { ALLOW } from './allow';

// Sy-01 (the anatomy of a page) and Sy-02 (the same page in dark, a message open)
// against a topic page of the live console with its newest record open, at 1440.
// Sy-01 is compared in light and in dark (its own .app.dark tokens); Sy-02 is drawn
// dark and is compared in dark. Sy-01's inspector holds the design's explainer, an
// annotation: only the panel is compared there; Sy-02's holds the message, and its
// parts are compared.

test('Sy-01 matches a live topic page', async ({ browser }) => {
  test.setTimeout(120_000);
  const out = await runBoard(browser, {
    id: 'Sy-01', file: 'Sy-01-Anatomy', boardState: dropMarkers,
    live: livePage(BASE_URL, `/topics/${TOPIC}`, openMessage),
    probes: [...SHELL, ...TOPIC_PAGE], allow: ALLOW,
  });
  expect(out.text, out.text).toBe('');
});

// The records are a <table> (allow.ts, TABLE): the board's row fill, row rule and
// header type are on the console's cells. Each is read from the board's row and held
// against the live cells, in both themes.
for (const theme of ['light', 'dark'] as const) {
  test(`Sy-01: the table's cells carry the board's fill, rules and header type (${theme})`, async ({ browser }) => {
    const board = await openBoard(browser, 'Sy-01-Anatomy', 1440, theme);
    const live = await openLive(browser, `${BASE_URL}topics/${TOPIC}`, 1440, theme);
    await openMessage(live);
    try {
      const style = (page: Page, sel: string) => page.locator(sel).first().evaluate((e) => {
        const s = getComputedStyle(e);
        return { bg: s.backgroundColor, rule: `${s.borderBottomWidth} ${s.borderBottomColor}`, font: `${s.fontWeight} ${s.fontSize}/${s.lineHeight}`, ls: s.letterSpacing, tt: s.textTransform, color: s.color };
      });
      const head = await style(board, 'main > .card .r.h');
      const th = await style(live, '#messages-table thead th');
      expect({ bg: th.bg, rule: th.rule, font: th.font, ls: th.ls, tt: th.tt, color: th.color }, 'header cells').toEqual(head);
      const row = await style(board, 'main > .card .r:not(.h):not(.on)');
      const td = await style(live, '#messages-table tbody tr[id^="msg-"]:not(.on) > td');
      expect(td.rule, 'a row\'s rule').toBe(row.rule);
      const on = await style(board, 'main > .card .r.on');
      const onTd = await style(live, '#messages-table tbody tr.on > td');
      expect(onTd.bg, 'the open row\'s fill').toBe(on.bg);
    } finally {
      await board.context().close();
      await live.context().close();
    }
  });
}

// NO_SCHEMA: the callout Sy-02 draws is there for a record written with a schema.
test('Sy-02: a record written with a schema says which, in the callout', async ({ browser }) => {
  const live = await openLive(browser, `${BASE_URL}topics/seed-events?partition=0&open=0`, 1440, 'dark');
  try {
    await expect(live.locator('#inspector .callout.ok')).toContainText('Decoded with schema seed-events-value v1');
  } finally {
    await live.context().close();
  }
});

test('Sy-02 matches a live topic page in dark, a message open', async ({ browser }) => {
  test.setTimeout(120_000);
  const out = await runBoard(browser, {
    id: 'Sy-02', file: 'Sy-02-Dark', places: [{ size: 1440, theme: 'dark' }],
    live: livePage(BASE_URL, TRACED, recordOpen),
    probes: [...SHELL, ...TOPIC_PAGE, ...MESSAGE], allow: ALLOW,
  });
  expect(out.text, out.text).toBe('');
});
