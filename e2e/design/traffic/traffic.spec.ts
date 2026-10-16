import { mkdirSync, readFileSync, writeFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { chromium, type Browser, type Page } from '@playwright/test';
import { test, expect } from '../../fixtures/console';
import { PREFIX } from '../../playwright.config';
import { openBoard, ROOT } from '../lib/render';
import { snapshot } from '../lib/measure';
import { diffPairs, entryFor, judge, loadAllow, report, selectors, type Diff } from '../lib/diff';
import { Ledger } from '../lib/ledger';
import { BOARDS, type Board } from './boards';
import { TRUTHS, type Edge } from './truth';
import { startWorld, type World } from './world';

// The design gate for Traffic (plan/01-protocol.md, "Design conformance"): each board of
// the design rendered from its own HTML and CSS (e2e/design/boards), the live page put
// in the same state by real clients (e2e/design/gen), and the two compared element by
// element: position, size, type, colour, padding, border, radius, gap, tracks and copy,
// in the light and the dark theme. What is data on the page (a count, a client, a time)
// is compared with the clients' own ledger instead of the board's sample. A difference
// fails unless e2e/design/traffic/allow.json names it exactly, with the words of
// plan/00-foundation.md that record why; an entry no difference uses fails too.
//
// `go tool task design` runs it with the rest of the design gate. DESIGN_WORLD=1 measures a world already
// running (design/traffic/up.ts) instead of starting one.

const PORT = Number(process.env.E2E_PORT ?? 14092) + 40;
const ALLOW = join(ROOT, 'e2e', 'design', 'traffic', 'allow.json');
const REPORT = join(ROOT, 'e2e', '.tmp', 'design-traffic-report.json');
type Theme = 'light' | 'dark';
const THEMES: Theme[] = ['light', 'dark'];

test.describe.configure({ mode: 'serial' });

let world: World | undefined;
let base = '';
let ledger: Ledger;
let browser: Browser;
const found: Diff[] = [];

test.beforeAll(async () => {
  test.setTimeout(240_000);
  browser = await chromium.launch();
  if (process.env.DESIGN_WORLD) {
    const w = JSON.parse(readFileSync(join(ROOT, 'e2e', '.tmp', 'design-world.json'), 'utf8'));
    base = w.console;
    ledger = new Ledger(w.ledger);
    return;
  }
  world = await startWorld(PORT, PREFIX);
  base = world.console;
  ledger = world.ledger;
});

// The run's phases: the boards of an empty broker first, then the scenario's traffic,
// then the broker stopped.
let phase: 'empty' | 'traffic' | 'lost' = 'empty';
const lostPages = new Map<Theme, Page>();
async function enter(want: 'empty' | 'traffic' | 'lost') {
  if (process.env.DESIGN_WORLD) return;
  if (want === phase) return;
  if (want === 'traffic' && phase === 'empty') {
    await world!.scenario();
    phase = 'traffic';
  }
  if (want === 'lost') {
    // Traf-10 is a page that was open when the broker went: the pages of both themes are
    // opened first, then the broker is stopped under them.
    if (phase === 'empty') await world!.scenario();
    const lostBoard = BOARDS.find((b) => b.phase === 'lost')!;
    for (const theme of THEMES) {
      const page = await livePage(theme, 1440, false);
      await lostBoard.state(page, base, ledger);
      lostPages.set(theme, page);
    }
    await world!.stopBroker();
    phase = 'lost';
  }
  if (want !== phase) throw new Error(`the run is past the ${want} phase`);
}

test.afterAll(async () => {
  mkdirSync(join(ROOT, 'e2e', '.tmp'), { recursive: true });
  writeFileSync(REPORT, JSON.stringify(found.map(entryFor), null, 1));
  await browser?.close();
  await world?.stopAll();
});

// The pages are measured as rendered. A live page swaps its list, its summary and its notes
// for a newer render every 2.5 s, and not all at once (the count beside the errors list and
// the filter's "N of M" are drawn when the page is, the rest on each poll), so a poll that
// lands between the render and the read puts parts of two renders in one reading. The
// poll is answered "nothing changed" (204, the console's own answer when the hash is the
// same); a new render is the page opened again (readSettled). A page that is to lose the
// broker (Traf-10) keeps polling: that is how it finds out.
async function livePage(theme: Theme, width: number, frozen = true): Promise<Page> {
  const ctx = await browser.newContext({ viewport: { width, height: 900 }, colorScheme: theme });
  const page = await ctx.newPage();
  if (frozen) await page.route(/\/traffic\/live(\?|$)/, (route) => route.fulfill({ status: 204 }));
  return page;
}

// Reads the page and checks its data against the ledger. A count the ledger cannot settle
// (a request began within a few milliseconds of an edge of the window counted) is read
// again on a render of the page's own (the board's state, made again), and one that passed on a render is not asked of
// the next; a count that differs from a ledger that has no doubt about it on three renders
// in a row is the page's, and so is any other difference.
async function readSettled(page: Page, board: Board, sels: string[], renew?: () => Promise<void>) {
  const truthSels = board.truths.flatMap((t) => TRUTHS[t].sels);
  const strikes = new Map<string, number>();
  const failed = new Set<string>();
  const settled = new Set<string>();
  let last = '';
  for (let attempt = 0; attempt < 30; attempt++) {
    const snap = await snapshot(page, [...sels, ...truthSels]);
    const at = Date.now();
    await page.waitForTimeout(400); // a connection that opened or closed near `at` is in the ledger by now.
    // The ledger as of one moment after the page was read, the same for every client.
    ledger.read(world ? await world.sync() : undefined);
    const problems: string[] = [];
    let again = '';
    const edges: Edge[] = [];
    for (const t of board.truths) {
      const v = TRUTHS[t].check(snap, ledger, at);
      problems.push(...v.problems.map((x) => `${t}: ${x}`));
      if (v.again) again = v.again;
      edges.push(...(v.edges ?? []).map((e) => ({ ...e, key: `${t}: ${e.key}` })));
    }
    const now = new Set(edges.map((e) => e.key));
    for (const k of failed) if (!now.has(k)) settled.add(k);
    const waiting: Edge[] = [];
    for (const e of edges) {
      if (settled.has(e.key)) continue;
      failed.add(e.key);
      const n = e.firm ? (strikes.get(e.key) ?? 0) + 1 : 0;
      strikes.set(e.key, n);
      if (n >= 3) problems.push(`${e.key}: ${e.msg}`);
      else waiting.push(e);
    }
    if (!again && !waiting.length) return { snap, problems };
    last = again || waiting.map((e) => e.msg).join('; ');
    if (waiting.length && renew) await renew();
    else await page.waitForTimeout(700);
  }
  throw new Error(`${board.id}: the page could not be read off an edge in 30 tries (${last})`);
}

// One test, a step a board and theme, each failing softly: every board is measured in
// one run, in the run's order (empty, traffic, lost), and the report lists them all.
test('the Traffic pages are the boards, element by element, at 1440 in light and dark', async () => {
  test.setTimeout(30 * 60_000);
  for (const board of BOARDS) {
    const want = board.phase ?? 'traffic';
    if (process.env.DESIGN_WORLD && want !== 'traffic') continue; // a running world is neither empty nor stopped.
    if (process.env.DESIGN_ONLY && !process.env.DESIGN_ONLY.split(',').includes(board.id)) continue;
    await enter(want);
    for (const theme of THEMES) {
      await test.step(`${board.id} (${theme}, 1440)`, async () => {
        const width = 1440;
        const sels = selectors(board.pairs);
        const boardPage = await openBoard(browser, board.file, width, theme);
        const b = await snapshot(boardPage, sels.board);
        await boardPage.context().close();

        const page = lostPages.get(theme) ?? (await livePage(theme, width));
        if (!lostPages.has(theme)) await board.state(page, base, ledger);
        await page.evaluate(() => document.fonts.ready);
        // Lost: the page has noticed (its first poll after the stop failed).
        if (want === 'lost') await page.locator('html[data-connection-lost]').waitFor({ timeout: 15_000 });
        await page.waitForTimeout(600);
        const { snap, problems } = await readSettled(page, board, sels.live, want === 'lost' ? undefined : async () => {
          await board.state(page, base, ledger);
          await page.evaluate(() => document.fonts.ready);
          await page.waitForTimeout(300);
        });
        await page.context().close();

        const where = { board: board.id, theme, width };
        const diffs = diffPairs(b, snap, board.pairs, where, new Set(board.truths));
        found.push(...diffs);
        const allow = existsSync(ALLOW) ? loadAllow(ALLOW) : [];
        const { failing, stale } = judge(diffs, allow, where);
        const msg = [
          problems.length ? `${board.id} ${theme}: what the page says that the ledger does not:\n  ${problems.join('\n  ')}` : '',
          failing.length ? `${board.id} ${theme}: differences from the board that the allow-list does not name (${failing.length}):\n${report(failing)}` : '',
          stale.length ? `${board.id} ${theme}: allow-list entries no difference used:\n${stale.map((x) => `  ${x.element}[${x.index}].${x.prop}`).join('\n')}` : '',
        ].filter(Boolean).join('\n\n');
        expect.soft(msg, msg).toBe('');
      });
    }
  }
});
