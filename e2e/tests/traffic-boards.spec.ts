import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { test, expect } from '../fixtures/console';
import type { Locator, Page } from '@playwright/test';
import { kafkaAddr } from '../addr';
import { BASE_URL } from '../playwright.config';

// The held consumers read a topic of their own: the seed's topics are other specs' (a topic's
// dock lists the requests that touched it, and the first of them is the seed's).
const HOLD_TOPIC = 'trf-hold-topic';


// What the Traffic boards do, as against what they draw. What they draw (every element's
// geometry, type, colour and copy, and the data against a ledger of real clients) is
// design/traffic/traffic.spec.ts, the design gate; this file is the behaviour the gate
// cannot see: what a key, a click, a poll, a pause or a narrow screen does to the page.
// Consumers that stay in their groups (one classic, two of KIP-848's protocol), and one
// that reports its own metrics, are the traffic.

const holds: ChildProcess[] = [];
const CLIENT = 'trf-hold';
const QUIET = 'trf-quiet';
const GROUP = 'trf-hold-group';
const KGROUP = 'trf-848-group';

test.describe.configure({ mode: 'serial' });

/** A consumer that stays in its group; `env` says how (HOLD_TELEMETRY, HOLD_KIP848, ...). */
async function holdOne(client: string, group: string, env: Record<string, string>) {
  const p = spawn(`e2e/.tmp/bin/hold-trf-${process.pid}`, [kafkaAddr(), client, group, HOLD_TOPIC],
    { cwd: '..', stdio: ['ignore', 'pipe', 'inherit'], env: { ...process.env, ...env } });
  holds.push(p);
  await new Promise<void>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`${client} never joined its group`)), 20_000);
    p.stdout!.on('data', (b) => { if (String(b).includes('ready')) { clearTimeout(t); resolve(); } });
    p.on('exit', () => reject(new Error(`${client} exited`)));
  });
}

test.beforeAll(async () => {
  await fetch(new URL('new/topic', BASE_URL), { method: 'POST', body: new URLSearchParams({ name: HOLD_TOPIC, partitions: '2' }) });
  execFileSync('go', ['build', '-o', `e2e/.tmp/bin/hold-trf-${process.pid}`, './e2e/hold'], { cwd: '..', env: { ...process.env, GOWORK: 'off' } });
  await holdOne(CLIENT, GROUP, { HOLD_TELEMETRY: '1' });
  await holdOne(QUIET, 'trf-quiet-group', { HOLD_NO_METRICS: '1' });
  await holdOne('trf-848-a', KGROUP, { HOLD_KIP848: '1' });
  await holdOne('trf-848-b', KGROUP, { HOLD_KIP848: '1' });
});

test.afterAll(() => { holds.forEach((p) => p.kill('SIGTERM')); });

const REQ_ROWS = '#traffic-table .tf-rows > .r[data-row]';
const CLIENT_ROWS = '#clients-table .tf-rows > .r[data-row]';
const STORY_ROWS = '#traffic-story .tf-rows > .r[data-row]';
const insp = (page: Page) => page.locator('#inspector');
const kind = (page: Page) => page.locator('#inspector [data-inspector-kind]');
const keys = (page: Page) => page.locator('#keys .keys-live');

async function go(page: Page, path = 'traffic') {
  await page.addInitScript(() => { try { localStorage.setItem('theme', 'light'); } catch { /* none */ } });
  await page.goto(path);
}

/** The strip's hints, each as its keys and its words: "j k move". */
async function strip(page: Page) {
  return (await keys(page).evaluateAll((ss) => ss.map((s) => Array.from(s.childNodes).map((n) => n.textContent!.trim()).filter(Boolean).join(' '))));
}

/** Opens the rows one by one until `want` holds in the inspector. */
async function openUntil(page: Page, rows: Locator, want: (p: Page) => Promise<boolean>, timeout = 20_000) {
  await expect.poll(async () => {
    const n = await rows.count();
    for (let i = 0; i < n; i++) {
      await page.keyboard.press('Escape');
      await rows.nth(i).click();
      await expect(kind(page)).toBeVisible();
      if (await want(page)) return true;
    }
    return false;
  }, { timeout, intervals: [1_000] }).toBe(true);
}

// ---- the filter and the kinds (Main, Traf-02, Traf-03) ----

test('Traf-02: removing a pill removes its filter, and clear removes them all', async ({ page }) => {
  await go(page, 'traffic?topic=seed-orders&kind=produce');
  const pills = page.locator('#filter-pills');
  await expect(pills.locator('.tf-pill')).toContainText(['seed-orders']);
  await expect(page.locator('#match-count')).toHaveText(/^\d+ of \d+ requests match$/);
  for (const row of await page.locator(`${REQ_ROWS} > span:nth-child(4)`).all()) await expect(row).toContainText('seed-orders');
  await pills.locator('.tf-pill a').first().click();
  await expect(page).not.toHaveURL(/topic=/);
  await expect(page).toHaveURL(/kind=produce/);
  await page.goto('traffic?topic=seed-orders&kind=produce');
  // clear removes the filters; the kind is the segmented control's, and stays.
  await pills.locator('a.b', { hasText: 'clear' }).click();
  await expect(page).toHaveURL(/\/traffic\?kind=produce$/);
});

// B1: Esc with a filter applied went nowhere (it emptied the field, and the filter stayed).
test('B1: Esc clears the filter, from the page and from the field, and closes a selection first', async ({ page }) => {
  await go(page, 'traffic?topic=seed-orders');
  await page.locator('main h1').click();
  await page.keyboard.press('Escape');
  await expect(page).toHaveURL(/\/traffic$/);
  await expect(page.locator('#filter-pills')).toHaveCount(0);

  await page.goto('traffic?q=doze-seed');
  await page.locator('input[data-traffic-filter]').focus();
  await page.keyboard.press('Escape');
  await expect(page).toHaveURL(/\/traffic$/);
  await expect(page.locator('input[data-traffic-filter]')).toHaveValue('');

  // With a request open, Esc closes it and the filter stays; the next Esc clears it.
  await page.goto('traffic?topic=seed-orders');
  await page.locator(REQ_ROWS).first().click();
  await expect(kind(page)).toHaveAttribute('data-inspector-kind', 'request');
  await page.keyboard.press('Escape');
  await expect(insp(page)).toBeHidden();
  await expect(page).toHaveURL(/topic=seed-orders/);
  await page.keyboard.press('Escape');
  await expect(page).toHaveURL(/\/traffic$/);

  // A group's story clears the same way (Traf-12's strip says "esc clear filter").
  await page.goto(`traffic?group=${GROUP}`);
  await expect(page.locator('#traffic-story')).toBeVisible();
  await page.locator('main h1').click();
  await page.keyboard.press('Escape');
  await expect(page).toHaveURL(/\/traffic$/);
});

test('Traf-03: e switches to errors only and back, and the palette has Show errors', async ({ page, openPalette }) => {
  await go(page);
  await page.keyboard.press('e');
  await expect(page).toHaveURL(/kind=errors/);
  const rows = page.locator(REQ_ROWS);
  await expect(rows.first()).toBeVisible();
  for (const row of await rows.all()) await expect(row).toHaveClass(/errrow/);
  await page.keyboard.press('e');
  await expect(page).not.toHaveURL(/kind=errors/);
  await openPalette();
  await page.keyboard.type('Show errors');
  await expect(page.locator('.pal-item', { hasText: 'Show errors' })).toBeVisible();
});

test('Traf-03: a consumer\'s first join is on the page and is not an error', async ({ page }) => {
  await go(page, 'traffic?q=MEMBER_ID_REQUIRED');
  const join = page.locator(REQ_ROWS, { hasText: 'MEMBER_ID_REQUIRED' }).first();
  await expect(join).toBeVisible();
  await expect(join).not.toHaveClass(/errrow/);
  await page.goto('traffic?kind=errors&q=MEMBER_ID_REQUIRED');
  await expect(page.locator(REQ_ROWS)).toHaveCount(0);
});

test('Traf-03: a refusal opens as one, with what it means', async ({ page }) => {
  await go(page, 'traffic?kind=errors');
  await page.locator(REQ_ROWS, { hasText: 'TOPIC_ALREADY_EXISTS' }).first().click();
  await expect(page.locator('#request-refusal')).toHaveClass(/deny|refused/);
  await expect(page.locator('#request-refusal')).toContainText('TOPIC_ALREADY_EXISTS');
});

// ---- the strip of keys, a state at a time (B2) ----

test('B2: the strip of keys is each state\'s own, as its board draws it', async ({ page }) => {
  await go(page);
  await expect.poll(() => strip(page)).toEqual(['j k move', '↵ open', '/ filter', 'p pause', '⌘K jump or act']);
  await page.goto('traffic?topic=seed-orders');
  await expect.poll(() => strip(page)).toEqual(['j k move', '↵ open', '/ filter', 'esc clear filter', 'p pause', '⌘K jump or act']);
  await page.goto('traffic?kind=errors');
  await expect.poll(() => strip(page)).toEqual(['j k move', '↵ open', '/ filter', 'e errors only', 'p pause', '⌘K jump or act']);
  await page.goto('traffic?view=clients');
  await expect.poll(() => strip(page)).toEqual(['j k move', '↵ open client', 'g r requests', '⌘K jump or act']);
  await page.goto(`traffic?group=${GROUP}`);
  await expect.poll(() => strip(page)).toEqual(['j k move', '↵ open', 'esc clear filter', '⌘K jump or act']);
  // A request open: its copy key, and esc to close it.
  await page.goto('traffic?kind=produce&topic=seed-orders');
  await page.locator(REQ_ROWS).first().click();
  await expect(page.locator('#decoded')).toBeVisible();
  await expect.poll(() => strip(page)).toEqual(['j k move', '↵ open', 'c copy decoded', 'esc close', 'p pause', '⌘K jump or act']);
  // Paused with it open: p says resume, before esc.
  await page.keyboard.press('p');
  await expect.poll(() => strip(page)).toEqual(['j k move', '↵ open', 'c copy decoded', 'p resume', 'esc close', '⌘K jump or act']);
  await page.keyboard.press('p');
});

// ---- the rows: one tab stop, the arrows, j and k, Enter ----

for (const [name, path, row] of [
  ['requests', 'traffic?kind=errors&q=doze-seed', REQ_ROWS],
  ['clients', 'traffic?view=clients', CLIENT_ROWS],
  ['story', `traffic?group=${GROUP}&kind=groups`, STORY_ROWS],
] as const) {
  test(`grid: the ${name} rows are one tab stop, the arrows move, j then Enter and a focused row then Enter open it`, async ({ page }) => {
    await go(page, path);
    const rows = page.locator(row);
    await expect(rows.nth(1)).toBeVisible();
    expect(await rows.evaluateAll((rs) => rs.filter((r) => (r as HTMLElement).tabIndex === 0).length)).toBe(1);
    await rows.first().focus();
    await page.keyboard.press('ArrowDown');
    await expect(rows.nth(1)).toBeFocused();
    await expect(rows.nth(1)).toHaveAttribute('tabindex', '0');
    await expect(rows.first()).toHaveAttribute('tabindex', '-1');
    await page.keyboard.press('ArrowUp');
    await expect(rows.first()).toBeFocused();
    await page.keyboard.press('Enter');
    await expect(kind(page)).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(insp(page)).toBeHidden();
    await page.locator('main h1').click();
    await page.keyboard.press('j');
    await expect(page.locator('main .cursor')).toHaveCount(1);
    await page.keyboard.press('Enter');
    await expect(kind(page)).toBeVisible();
  });
}

test('Main: the open row is drawn open (.on, aria-selected), through polls and a reload', async ({ page }) => {
  await go(page, 'traffic?kind=produce&topic=seed-orders');
  const rows = page.locator(REQ_ROWS);
  await expect(rows.first()).toBeVisible();
  const id = (await rows.first().getAttribute('id'))!;
  await expect(rows.first()).toHaveAttribute('aria-selected', 'false');
  await rows.first().click();
  await expect(page).toHaveURL(/open=request/);
  const row = page.locator(`[id="${id}"]`);
  await expect(row).toHaveClass(/\bon\b/);
  await expect(row).toHaveAttribute('aria-selected', 'true');
  await expect(page.locator(`${REQ_ROWS}.on`)).toHaveCount(1);
  await page.waitForTimeout(3_500); // a poll replaces the rows.
  await expect(page.locator(`[id="${id}"]`)).toHaveClass(/\bon\b/);
  await page.reload();
  await expect(page.locator('#inspector-title')).toBeVisible();
  await expect(page.locator(`[id="${id}"]`)).toHaveAttribute('aria-selected', 'true');
  await page.keyboard.press('Escape');
  await expect(page.locator(`${REQ_ROWS}.on`)).toHaveCount(0);
});

test('a client name in a row opens that client, and its link lists only its requests (TRF-9)', async ({ page }) => {
  await go(page, `traffic?client=${CLIENT}`);
  const rows = page.locator(REQ_ROWS);
  await expect(rows.first()).toBeVisible();
  for (const r of await rows.all()) await expect(r.locator('> span').nth(1)).toHaveText(CLIENT);
  await page.goto('traffic');
  await page.locator(`${REQ_ROWS} a.tf-who`, { hasText: CLIENT }).first().click();
  await expect(kind(page)).toHaveAttribute('data-inspector-kind', 'client');
  await expect(page.locator('#inspector-title')).toContainText(CLIENT);
});

// ---- the inspector's selections (Traf-04, 05, 07, 11) ----

test('Traf-04: a request opens with its decoded fields, which c copies whole', async ({ page, context }) => {
  await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  await go(page, 'traffic?kind=produce&topic=seed-orders');
  await page.locator(REQ_ROWS).first().click();
  await expect(page.locator('#inspector-title')).toHaveText(/^Produce v\d+$/);
  const code = page.locator('#decoded pre.code');
  await expect(code).toContainText('"topic_data"');
  await expect(insp(page)).toContainText('Record bytes are shown on the topic page, not here.');
  await page.locator('main h1').click();
  await page.keyboard.press('c');
  await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe((await code.textContent())!);
});

test('Traf-05: a folded row unfolds, by its button and by u', async ({ page }) => {
  await go(page, `traffic?q=${GROUP}&kind=groups`);
  const folded = page.locator(REQ_ROWS, { hasText: 'Heartbeat' }).filter({ has: page.locator('.tag') }).first();
  await expect(folded).toBeVisible({ timeout: 20_000 });
  await folded.click();
  await expect(page.locator('#fold-sentence')).toContainText('identical heartbeats are shown as one row');
  expect(await page.locator('#fold-ticks i.tick').count()).toBeGreaterThanOrEqual(2);
  await page.locator('main h1').click();
  await page.keyboard.press('u');
  await expect(page.locator('#unfolded')).toBeVisible();
  await expect(page.locator('#unfolded .r').nth(1)).toBeVisible();
});

test('Traf-06: opening a client brings the panel; Esc puts the page back; the tab goes to the requests', async ({ page }) => {
  await go(page, 'traffic?view=clients');
  await expect(page.locator('#inspector-panel')).toBeHidden();
  const row = page.locator(CLIENT_ROWS, { hasText: CLIENT }).first();
  await row.click();
  await expect(insp(page)).toBeVisible();
  await expect(page.locator('html')).toHaveAttribute('data-traffic-open', /^client-/);
  await page.keyboard.press('Escape');
  await expect(page.locator('#inspector-panel')).toBeHidden();
  await expect(page.locator('html')).not.toHaveAttribute('data-traffic-open', /./);
  await page.locator('nav.tabs a.tab', { hasText: 'Requests' }).click();
  await expect(page.locator('#traffic-table')).toBeVisible();
});

test('Traf-06: clients are counted by client id, with their connections per client', async ({ page }) => {
  await go(page, 'traffic?view=clients');
  const rows = page.locator(CLIENT_ROWS);
  await expect(rows.first()).toBeVisible();
  const names = await rows.evaluateAll((rs) => rs.map((r) => r.querySelector('span')!.textContent!.trim()));
  expect(new Set(names).size, `client names ${names.join(', ')}`).toBe(names.length);
  await expect(page.locator('#traffic-summary')).toContainText(`${names.length} client${names.length === 1 ? '' : 's'} connected`);
  await expect(page.locator('#clients-tab-n')).toHaveText(String(names.length));
});

test('Traf-07: a client that reports its metrics, and m lists all of them', async ({ page }) => {
  await holdOne('trf-many', 'trf-many-group', { HOLD_TELEMETRY: 'many' });
  await go(page, 'traffic?view=clients');
  const rows = page.locator(CLIENT_ROWS, { hasText: 'trf-many' });
  await expect(rows.first()).toBeVisible();
  await openUntil(page, rows, async (p) => (await p.locator('[data-all-metrics]').count()) > 0, 30_000);
  await expect(page.locator('html')).toHaveAttribute('data-traffic-open', 'client-metrics');
  await expect(page.locator('#client-inspector .spark').first()).toBeVisible();
  await expect(page.locator('#client-inspector')).toContainText(/Sparklines (cover .+, one bar per report|draw one bar per report)\./);
  await page.locator('main h1').click();
  await page.keyboard.press('m');
  await expect(page.locator('[data-all-metrics]')).toHaveCount(0);
  await expect(page.locator('#client-inspector')).toContainText('records.lag.max');
});

test('Traf-11: a client that does not report metrics says why and what to flip', async ({ page }) => {
  await go(page, 'traffic?view=clients');
  const rows = page.locator(CLIENT_ROWS, { hasText: QUIET });
  await expect(rows.first()).toBeVisible();
  await openUntil(page, rows, async (p) => (await p.locator('#no-metrics').count()) > 0);
  await expect(page.locator('html')).toHaveAttribute('data-traffic-open', 'client-none');
  await expect(page.locator('#client-inspector pre.code')).toContainText('enable.metrics.push=true');
  await expect(page.locator('#client-inspector a.btn', { hasText: 'Its requests' })).toHaveAttribute('href', new RegExp(`client=${QUIET}`));
});

// ---- paused (Traf-08, B3) ----

test('Traf-08: paused, the rows stay, the count of what was missed goes up, and Resume adds them', async ({ page }) => {
  await go(page);
  const banner = page.locator('#paused-banner');
  await expect(banner).toBeHidden();
  await page.keyboard.press('p');
  await expect(page.locator('html')).toHaveAttribute('data-paused', '');
  await expect(banner).toBeVisible();
  await expect(page.locator('[data-live-state]')).toHaveText('paused');
  const first = await page.locator(REQ_ROWS).first().getAttribute('id');
  await expect(async () => {
    expect(Number(await banner.locator('[data-new-n]').textContent())).toBeGreaterThan(0);
  }).toPass({ timeout: 15_000 });
  // The line under the table counts the same requests.
  await expect(page.locator('#traffic-note [data-new-n2]')).toHaveText((await banner.locator('[data-new-n]').textContent())!);
  expect(await page.locator(REQ_ROWS).first().getAttribute('id')).toBe(first);
  await banner.locator('#paused-resume').click();
  await expect(banner).toBeHidden();
  await expect(page.locator('html')).not.toHaveAttribute('data-paused', '');
});

test('Traf-08: a request open when the page paused says it is still selected from before', async ({ page }) => {
  await go(page, 'traffic?kind=produce&topic=seed-orders');
  await page.locator(REQ_ROWS).first().click();
  await expect(page.locator('#decoded')).toBeVisible();
  const kept = page.locator('#inspector .tf-kept');
  await expect(kept).toBeHidden();
  await page.keyboard.press('p');
  await expect(kept).toBeVisible();
  // Another row opened while paused is not one from before the pause.
  await page.locator(REQ_ROWS).nth(1).click();
  await expect(page.locator('#inspector .tf-kept')).toBeHidden();
  await page.keyboard.press('p');
});

// ---- a group's story (Traf-12), classic and KIP-848 ----

test('Traf-12: the requests that start a rebalance are under it, in the order they happened', async ({ page }) => {
  await go(page, `traffic?group=${GROUP}&kind=groups`);
  const story = page.locator('#traffic-story');
  await expect(story.locator('.tf-rebalance').first()).toBeVisible();
  const parts = await story.evaluate((el) => {
    const out: { block: number; api: string; at: string; kid: boolean }[] = [];
    let block = -1;
    for (const r of Array.from(el.querySelectorAll('.tf-rows > .r'))) {
      if (r.classList.contains('tf-rebalance')) { block++; continue; }
      const spans = r.querySelectorAll(':scope > span');
      out.push({ block: r.classList.contains('kid') ? -1 : block, api: (spans[2]?.textContent ?? '').trim(), at: spans[0].querySelector('time')?.getAttribute('datetime') ?? '', kid: r.classList.contains('kid') });
    }
    return out;
  });
  const nested = parts.filter((p) => !p.kid);
  expect(nested.some((p) => /^JoinGroup\b/.test(p.api))).toBe(true);
  expect(parts.filter((p) => p.kid && /^(FindCoordinator|JoinGroup)\b/.test(p.api)), 'a join filed as before the rebalance').toEqual([]);
  for (const b of new Set(nested.map((p) => p.block))) {
    const ats = nested.filter((p) => p.block === b).map((p) => Date.parse(p.at));
    expect(ats, `block ${b} is in time order`).toEqual([...ats].sort((x, y) => x - y));
  }
  await expect(page.locator('#group-summary .card .r.h')).toContainText('Gen');
});

test('Traf-12, KIP-848: a group of the consumer protocol is told in epochs, by its ConsumerGroupHeartbeats', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await go(page, `traffic?group=${KGROUP}`);
  const story = page.locator('#traffic-story');
  await expect(story.locator('.tf-rebalance').first()).toBeVisible({ timeout: 20_000 });
  await expect(story.locator('.tf-rebalance').first()).toContainText('epoch');
  await expect(story.locator('.tf-rows > .r[data-row]', { hasText: 'ConsumerGroupHeartbeat' }).first()).toBeVisible();
  await expect(story.locator('.tf-rows > .r[data-row]', { hasText: /^.*JoinGroup/ })).toHaveCount(0);
  const g = page.locator('#group-summary');
  await expect(g.locator('.cap', { hasText: /^Epochs$/ })).toBeVisible();
  await expect(g.locator('.card .r.h > span').first()).toHaveText('Epoch');
  await expect(g).toContainText('there is no JoinGroup or SyncGroup');
  await expect(g).not.toContainText('any join in the classic protocol');
});

// ---- widths (Sy-03, Sy-04) ----

/** What does not fit at this width: the page wider than the screen, a Time, Result or
 * Took cell cut, the summary or the filter row on more than one line. */
async function misfits(page: Page, oneLine: boolean) {
  return page.evaluate((oneLine) => {
    const out: string[] = [];
    const doc = document.documentElement;
    if (doc.scrollWidth > doc.clientWidth) out.push(`the page is ${doc.scrollWidth - doc.clientWidth} px wider than the screen`);
    for (const grid of Array.from(document.querySelectorAll('#traffic-table, #traffic-story'))) {
      const heads = Array.from(grid.querySelectorAll(':scope > .r.h > span')).map((s) => s.textContent!.trim());
      for (const r of Array.from(grid.querySelectorAll('.tf-rows > .r[data-row]')).slice(0, 40)) {
        const cells = Array.from(r.querySelectorAll(':scope > span')) as HTMLElement[];
        cells.forEach((c, i) => {
          if (!['Time', 'Result', 'Took'].includes(heads[i]) || c.offsetParent === null) return;
          if (c.scrollWidth > c.clientWidth + 1) out.push(`${heads[i]} "${c.textContent!.trim()}" is cut (${c.scrollWidth} in ${c.clientWidth})`);
        });
        const box = r.getBoundingClientRect(), card = grid.getBoundingClientRect();
        if (box.right > card.right + 0.5) out.push(`a row runs ${box.right - card.right} px past its card`);
      }
    }
    if (oneLine) {
      for (const sel of ['#traffic-summary', '.tf-filter']) {
        const el = document.querySelector(sel) as HTMLElement | null;
        if (!el || el.offsetParent === null) continue;
        const tops = new Set(Array.from(el.children).filter((c) => (c as HTMLElement).offsetParent !== null && c.getBoundingClientRect().width > 0).map((c) => Math.round(c.getBoundingClientRect().top + c.getBoundingClientRect().height / 2)));
        if (tops.size > 1) out.push(`${sel} is on ${tops.size} lines`);
      }
    }
    return out;
  }, oneLine);
}

for (const width of [1600, 1440, 1366, 1280, 1180, 1100, 960, 390]) {
  test(`at ${width}: nothing is cut or off the edge, with and without a request open`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    for (const path of ['traffic', 'traffic?topic=seed-orders', 'traffic?kind=errors', 'traffic?view=clients', `traffic?group=${GROUP}`]) {
      await go(page, path);
      await expect(page.locator('main h1')).toHaveText('Traffic');
      await page.waitForTimeout(200);
      // The summary and the filter row are one line where the boards draw them (from 1180,
      // the inspector beside the list; Sy-03 wraps them at 960).
      expect(await misfits(page, width >= 1180 && !path.includes('group=')), `${path} at ${width}`).toEqual([]);
      const row = page.locator(`${REQ_ROWS}, ${STORY_ROWS}, ${CLIENT_ROWS}`).first();
      if (!(await row.count())) continue;
      await row.click();
      await expect(kind(page)).toBeVisible();
      await page.waitForTimeout(200);
      expect(await misfits(page, false), `${path} at ${width}, open`).toEqual([]);
    }
  });
}

// ---- the key sheet (Sy-06) ----

async function countKeyClicks(page: Page) {
  await page.evaluate(() => {
    const w = window as any;
    w.__keyClicks = [];
    document.addEventListener('click', (e) => {
      for (const sel of ['data-unfold', 'data-all-metrics', 'data-copy-decoded', 'data-retry']) {
        if ((e.target as Element).closest(`[${sel}]`)) w.__keyClicks.push(sel);
      }
    }, true);
  });
}
const keyClicks = (page: Page) => page.evaluate(() => (window as any).__keyClicks as string[]);

test('Sy-06: behind the key sheet Traffic ignores p, e, r, j, k, u, m and c; closed, they act', async ({ page }) => {
  await go(page);
  await expect(page.locator(REQ_ROWS).first()).toBeVisible();
  await countKeyClicks(page);
  const toggle = page.locator('[data-live-toggle]').first();
  await page.locator('main h1').click();
  await page.evaluate(() => document.documentElement.setAttribute('data-connection-lost', ''));
  await page.keyboard.press('?');
  await expect(page.locator('#help')).toBeVisible();
  for (const k of ['p', 'e', 'r', 'j', 'k', 'u', 'm', 'c']) {
    await page.keyboard.press(k);
    await page.waitForTimeout(250);
    await expect(page.locator('#help'), `after ${k}`).toBeVisible();
    await expect(toggle, `after ${k}`).toHaveAttribute('aria-pressed', 'false');
    await expect(page, `after ${k}`).not.toHaveURL(/kind=errors/);
    await expect(page.locator('.cursor'), `after ${k}`).toHaveCount(0);
    expect(await keyClicks(page), `after ${k}`).toEqual([]);
  }
  await page.keyboard.press('Escape');
  await expect(page.locator('#help')).toBeHidden();
  await page.keyboard.press('r');
  expect(await keyClicks(page)).toEqual(['data-retry']);
  await page.evaluate(() => document.documentElement.removeAttribute('data-connection-lost'));
  await page.keyboard.press('p');
  await expect(toggle).toHaveAttribute('aria-pressed', 'true');
  await page.keyboard.press('p');
  await expect(toggle).toHaveAttribute('aria-pressed', 'false');
  await page.keyboard.press('j');
  await expect(page.locator('main .cursor')).toHaveCount(1);
  await page.keyboard.press('e');
  await expect(page).toHaveURL(/kind=errors/);
});
