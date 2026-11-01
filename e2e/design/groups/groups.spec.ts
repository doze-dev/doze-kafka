import { readFileSync } from 'node:fs';
import { chromium, type Browser, type Page } from '@playwright/test';
import { test, expect } from '@playwright/test';
import { PREFIX } from '../../playwright.config';
import { livePage, runBoard } from '../lib/gate';
import { shellAllow } from '../shell/common-allow';
import { ALLOW } from './allow';
import { GROUP_HEAD, LAG, NONE, MEMBERS, SHELL } from './boards';
import { FIRST, GROUP, MORE, TOPIC, startGroupsWorld, type GroupsWorld } from './world';

// The Groups boards against the live pages, element by element, in light and dark: the
// page put in the board's state by real clients (e2e/design/gen), the world's own broker.
// What may differ is in allow.ts, each with its reason.

const PORT = Number(process.env.E2E_PORT ?? 14092) + 60;
test.describe.configure({ mode: 'serial' });

let w: GroupsWorld;
let browser: Browser;
let base = '';

test.beforeAll(async () => {
  test.setTimeout(240_000);
  browser = await chromium.launch();
  w = await startGroupsWorld(PORT, PREFIX);
  base = w.world.console;
});
test.afterAll(async () => { await browser?.close(); await w?.stop(); });

test('Gr-01 matches the live /groups with no group', async () => {
  const out = await runBoard(browser, { id: 'Gr-01', file: 'Gr-01-None', live: livePage(base, 'groups'), probes: [...SHELL, ...NONE], allow: ALLOW });
  expect(out.text, out.text).toBe('');
});

test('the world is the boards\': three held members, 14 records behind', async () => {
  test.setTimeout(180_000);
  await w.fill();
  const g = await (await fetch(`${base}groups/${GROUP}`)).text();
  // The lag the page shows is the ledger's: what the feed wrote, less what the held members took.
  const per = w.produced();
  expect(per.reduce((a, b) => a + b, 0)).toBe(FIRST + MORE);
  const total = g.match(/class="r totalrow">.*?<\/div>/s)?.[0] ?? '';
  expect(total.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ')).toContain(`${FIRST} ${FIRST + MORE} lag is log end minus committed ${MORE}`);
});

// Not yet at the board: see the report of the Groups stage, slice 1 (the page's lag tab
// differs in the neighbours band, the facts line, the tracks and the keys strip).
test('Gr-02 matches the live lag tab', async () => {
  const out = await runBoard(browser, { id: 'Gr-02', file: 'Gr-02-Lag', live: livePage(base, `groups/${GROUP}`), probes: [...SHELL, ...GROUP_HEAD, ...LAG], allow: ALLOW });
  expect(out.text, out.text).toBe('');
});

// Gr-03 draws the second member open on the right.
async function openSecondMember(p: Page) {
  await p.locator('#members-table .r:not(.h)').nth(1).click();
  await p.locator('#inspector:not([hidden]) .memberb').waitFor();
}

test('Gr-03 matches the live members tab', async () => {
  const out = await runBoard(browser, { id: 'Gr-03', file: 'Gr-03-Members', live: livePage(base, `groups/${GROUP}?tab=members`, openSecondMember), probes: [...SHELL, ...GROUP_HEAD, ...MEMBERS], allow: ALLOW });
  expect(out.text, out.text).toBe('');
});

const plain = (h: string) => h.replace(/<[^>]+>/g, ' ').replace(/ ([,.;)])/g, '$1').replace(/\( /g, '(').replace(/\s+/g, ' ').trim();

test('the sentences and the members are what the broker holds', async () => {
  const lag = await (await fetch(`${base}groups/${GROUP}`)).text();
  const facts = plain(lag.match(/id="group-facts">(.*?)<\/div>/s)?.[1] ?? '');
  // The generation is the coordinator's; "committed every 5 s" is a client setting and is not printed.
  expect(facts).toMatch(/^3 members on generation \d+, reading 3 partitions of 1 topic, 14 records behind in total \(the largest is p\d at \d+\)\.$/);
  expect(lag).not.toContain('every 5 s');
  const mem = await (await fetch(`${base}groups/${GROUP}?tab=members`)).text();
  expect(plain(mem.match(/id="group-facts">(.*?)<\/div>/s)?.[1] ?? '')).toMatch(/^3 members on generation \d+; the leader is \S+\. Session timeout \d+ s, rebalance timeout \d+ s\.$/);
  const rows = [...mem.matchAll(/<div class="r" id="mb-[^>]*>(.*?)<\/div>\s*(?=<div class="r"|<\/div>\s*<div class="note")/gs)].map((m) => plain(m[1]));
  expect(rows.length).toBe(3);
  for (const r of rows) expect(r).toMatch(/127\.0\.0\.1:\d+/); // host with port
  expect(rows.filter((r) => r.includes('leader')).length).toBe(1);
  for (const r of rows) expect(r).toMatch(/\d+ s ago|0 s ago/);
});
