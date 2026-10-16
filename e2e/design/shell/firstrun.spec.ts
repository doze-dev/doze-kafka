import { test, expect } from '@playwright/test';
import { OwnBroker } from '../../ownbroker';
import type { Probe } from '../lib/compare';
import { runBoard } from '../lib/gate';
import { SHELL, TEXT_W } from '../lib/probes';
import { openLive } from '../lib/render';
import { ALLOW } from './allow';

// Sy-08, the first run: a broker with nothing in it. The spec starts one of its own
// over an empty data directory (the seeded broker is not empty), and compares its
// home with the board at 1440 in light and dark. The steps' numbers are the page's
// (1, 2, 3), not the design's markers.

const broker = new OwnBroker('firstrun', 30);
test.beforeAll(async () => { test.setTimeout(180_000); await broker.start(); });
test.afterAll(async () => { await broker.kill(); });

const ADDR_W = 'its words are this broker\'s address and data directory, not the board\'s';
const STEP = (n: number, el: string, board: string, live: string, extra: Partial<Probe> = {}): Probe =>
  ({ el: `step ${n} ${el}`, board: `main > .row.g16 > section:nth-child(${n}) ${board}`, live: `main .steps > section:nth-child(${n}) ${live}`, ...extra });

const FIRST_RUN: Probe[] = [
  ...SHELL,
  { el: 'main', board: 'main.main', live: 'main.main' },
  { el: 'head', board: 'main > .col.g8', live: 'main > .col.g8' },
  { el: 'running', board: 'main > .col.g8 .pill.ok', live: 'main > .col.g8 .pill.ok', text: true },
  { el: 'where', board: 'main > .col.g8 .row .m', live: 'main > .col.g8 .row .m', free: { w: ADDR_W } },
  { el: 'h1', board: 'main h1', live: 'main h1', text: true },
  { el: 'lede', board: 'main > .col.g8 > p', live: 'main > .col.g8 > p', text: true },
  { el: 'steps', board: 'main > .row.g16', live: 'main .steps' },
  ...[1, 2, 3].flatMap((n) => [
    STEP(n, 'card', '', ''),
    STEP(n, 'number', '.num', '.num', { text: true }),
    STEP(n, 'title', '.row.ce.g8:first-child > :last-child', '.row.ce.g8:first-child > :last-child', { text: true }),
    STEP(n, 'note', '> .note', '> .note'),
    STEP(n, 'command', 'pre.code', 'pre.code', { free: { w: 'its lines carry this broker\'s address' } }),
    STEP(n, 'action', '> .row.ce.g8:last-child', '> .row.ce.g8:last-child'),
    STEP(n, 'button', '> .row.ce.g8:last-child .btn', '> .row.ce.g8:last-child .btn', { text: true }),
  ]),
  STEP(2, 'facts', '.kv', '.kv'),
  { el: 'foot', board: 'main > .well', live: 'main .firstrun-foot' },
  { el: 'foot note', board: 'main > .well > .note', live: 'main .firstrun-foot > .note' },
  { el: 'foot keys', board: 'main > .well > .note:last-child', live: 'main .firstrun-foot > .note:last-child', text: true },
];

test('Sy-08 matches the live first run on an empty broker', async ({ browser }) => {
  test.setTimeout(120_000);
  const out = await runBoard(browser, {
    id: 'Sy-08', file: 'Sy-08-FirstRun',
    live: async (b, place) => openLive(b, broker.console, place.size, place.theme),
    probes: FIRST_RUN, allow: ALLOW,
  });
  expect(out.text, out.text).toBe('');
});
