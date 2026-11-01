import { readFileSync } from 'node:fs';
import { chromium, type Browser } from '@playwright/test';
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
test.fixme('Gr-02 matches the live lag tab', async () => {
  const out = await runBoard(browser, { id: 'Gr-02', file: 'Gr-02-Lag', live: livePage(base, `groups/${GROUP}`), probes: [...SHELL, ...GROUP_HEAD, ...LAG], allow: ALLOW });
  expect(out.text, out.text).toBe('');
});

test.fixme('Gr-03 matches the live members tab', async () => {
  const out = await runBoard(browser, { id: 'Gr-03', file: 'Gr-03-Members', live: livePage(base, `groups/${GROUP}?tab=members`), probes: [...SHELL, ...GROUP_HEAD, ...MEMBERS], allow: ALLOW });
  expect(out.text, out.text).toBe('');
});
