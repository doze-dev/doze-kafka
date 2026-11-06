import { readFileSync } from 'node:fs';
import { chromium, type Browser } from '@playwright/test';
import { test, expect } from '@playwright/test';
import { PREFIX } from '../../playwright.config';
import { livePage, runBoard } from '../lib/gate';
import { RESET_ALLOW } from './allow-reset';
import { GROUP_HEAD, RESET, SHELL } from './boards';
import { IDLE_GROUP, IDLE_PARTS, IDLE_TOPIC, startGroupsWorld, type GroupsWorld } from './world';

// Gr-06 (the reset form and its preview) against the live Offsets tab of an Empty group,
// in light and dark, in a world of its own: the group Gr-06 draws, a group with offsets and
// nobody in it (e2e/design/gen idle). What may differ is in allow-reset.ts, each with its reason.
// Gr-07, Gr-08 and Gr-15 are held by e2e/tests/groups.spec.ts, not by this gate.

const PORT = Number(process.env.E2E_PORT ?? 14092) + 80;
test.describe.configure({ mode: 'serial' });

let w: GroupsWorld;
let browser: Browser;
let base = '';
let held: { ends: number[]; committed: number[] };

test.beforeAll(async () => {
  test.setTimeout(240_000);
  browser = await chromium.launch();
  w = await startGroupsWorld(PORT, PREFIX);
  held = await w.idle();
  base = w.world.console;
});
test.afterAll(async () => { await browser?.close(); await w?.stop(); });

test('Gr-06 matches the live Offsets tab of an Empty group', async () => {
  const out = await runBoard(browser, { id: 'Gr-06', file: 'Gr-06-Reset', live: livePage(base, `groups/${IDLE_GROUP}?tab=offsets`), probes: [...SHELL, ...GROUP_HEAD, ...RESET], allow: RESET_ALLOW });
  expect(out.text, out.text).toBe('');
});

const plain = (h: string) => h.replace(/<[^>]+>/g, ' ').replace(/ ([,.;)])/g, '$1').replace(/\s+/g, ' ').trim();
const n = (x: number) => x.toLocaleString('en-US');

test('the preview is what the clients left: every number from the ledger', async () => {
  const page = await (await fetch(`${base}groups/${IDLE_GROUP}?tab=offsets`)).text();
  const rows = [...page.matchAll(/<div class="r"><span class="m el">(\d+)<\/span>(.*?)<\/div>/gs)].map((m) => plain(m[0]));
  expect(rows.length).toBe(IDLE_PARTS);
  held.ends.forEach((end, p) => {
    // committed now, "committed → 0", lag after (the whole log: the group goes back to the start)
    expect(rows[p]).toBe(`${p} ${n(held.committed[p])} ${n(held.committed[p])} → 0 ${n(end)}`);
  });
  const behind = held.ends.reduce((a, e, p) => a + e - held.committed[p], 0);
  const log = held.ends.reduce((a, e) => a + e, 0);
  const facts = plain(page.match(/id="group-facts">(.*?)<\/div>/s)?.[1] ?? '');
  expect(facts).toBe(`No members; offsets kept on ${IDLE_PARTS} partitions of one topic, ${n(behind)} records behind. Resetting is allowed because the group is Empty.`);
  expect(plain(page.match(/id="reset-preview".*?<div class="note">(.*?)<\/div>\s*<\/div>/s)?.[1] ?? '')).toBe(`Going to earliest makes the group re-read ${n(log)} records instead of ${n(behind)}. Pick latest to skip the backlog.`);
  expect(plain(page.match(/id="reset-form".*?<\/form>/s)?.[0] ?? '')).toContain(`Apply: reset ${IDLE_PARTS} partitions`);
  expect(page).toContain(`${IDLE_TOPIC} (${IDLE_PARTS} partitions)`);
});

test('the timestamp the form starts on is the hour, in the board\'s form', async () => {
  const page = await (await fetch(`${base}groups/${IDLE_GROUP}?tab=offsets`)).text();
  expect(page).toMatch(/id="reset-at" name="at" value="\d{4}-\d\d-\d\dT\d\d:00:00Z"/);
});
