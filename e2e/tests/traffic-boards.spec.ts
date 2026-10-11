import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { test, expect } from '../fixtures/console';
import type { Page } from '@playwright/test';
import { kafkaAddr } from '../addr';

// The Traffic boards, one by one: Main and Traf-02 to Traf-12. Each test says which
// board it holds, and holds the board's structure, its copy and the computed tokens of
// what it draws (the colours of a refusal, of a pill, of a callout), not only that the
// text is there. A consumer that stays in its group, and reports its own metrics over a
// connection of its own, is the traffic the boards need to be real.

const holds: ChildProcess[] = [];
const CLIENT = 'trf-hold';
const QUIET = 'trf-quiet';
const GROUP = 'trf-hold-group';

test.describe.configure({ mode: 'serial' });

/** A consumer that stays in its group; `env` says whether it reports its metrics. */
async function holdOne(client: string, group: string, env: Record<string, string>) {
  const p = spawn(`e2e/.tmp/bin/hold-trf-${process.pid}`, [kafkaAddr(), client, group, 'seed-orders'],
    { cwd: '..', stdio: ['ignore', 'pipe', 'inherit'], env: { ...process.env, ...env } });
  holds.push(p);
  await new Promise<void>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`${client} never joined its group`)), 20_000);
    p.stdout!.on('data', (b) => { if (String(b).includes('ready')) { clearTimeout(t); resolve(); } });
    p.on('exit', () => reject(new Error(`${client} exited`)));
  });
}

test.beforeAll(async () => {
  execFileSync('go', ['build', '-o', `e2e/.tmp/bin/hold-trf-${process.pid}`, './e2e/hold'], { cwd: '..', env: { ...process.env, GOWORK: 'off' } });
  await holdOne(CLIENT, GROUP, { HOLD_TELEMETRY: '1' });
  await holdOne(QUIET, 'trf-quiet-group', { HOLD_NO_METRICS: '1' });
});

test.afterAll(() => { holds.forEach((p) => p.kill('SIGTERM')); });

// The light theme's tokens, as the boards define them (con.css).
const DENY = 'rgb(103, 60, 156)';
const DENYBG = 'rgb(236, 228, 246)';
const OK = 'rgb(22, 107, 63)';
const WARNBG = 'rgb(246, 236, 214)';
const BADBG = 'rgb(248, 227, 224)';

async function lightTraffic(page: Page, path = 'traffic') {
  await page.addInitScript(() => { try { localStorage.setItem('theme', 'light'); } catch { /* none */ } });
  await page.goto(path);
}

test('Main: the title, the three-item summary, one filter field, the six kinds and the seven columns', async ({ page }) => {
  await lightTraffic(page);
  await expect(page.locator('main h1')).toHaveText('Traffic');
  await expect(page.locator('main .idband .note').first()).toHaveText("What your apps have asked this broker, newest first. The console's own requests are left out.");
  const sum = page.locator('#traffic-summary');
  await expect(sum).toContainText(/\d+ clients? connected/);
  await expect(sum).toContainText(/\d+ requests? in the last minute/);
  await expect(sum).toContainText(/\d+ errors?/);
  await expect(sum).toContainText('kept: the last 2,000, in memory only');
  await expect(page.locator('.filterrow input[name=q]')).toHaveAttribute('placeholder', 'Client, topic, group or request');
  await expect(page.locator('.seg a')).toHaveText(['all', 'errors', 'produce', 'fetch', 'groups', 'admin']);
  await expect(page.locator('#filter-note .live-only')).toHaveText('idle heartbeats and empty fetches fold into one row');
  await expect(page.locator('#filter-note .paused-only')).toBeHidden();
  await expect(page.locator('#traffic-table thead th')).toHaveText(['Time', 'Client', 'Request', 'Topic or group', 'Detail', 'Result', 'Took']);
  // The live pill is the board's: ok-coloured, with its dot.
  const pill = page.locator('#traffic-controls [data-live-state]');
  await expect(pill).toHaveText('live');
  await expect(pill).toHaveCSS('color', OK);
  await expect(page.locator('#traffic-note')).toHaveText('Select a row to open it on the right. A client name opens that client. Nothing here is written to disk.');
  // The connected clients are the inspector's, with the board's sentence about metrics.
  await expect(page.locator('#insp-default .cap').first()).toHaveText('Connected clients');
  await expect(page.locator('#insp-default')).toContainText('Open a client to see them.');
});

test('Traf-02: a topic filter says what it narrows, how many match, and who touched the topic', async ({ page }) => {
  await lightTraffic(page, 'traffic?topic=seed-orders');
  const pills = page.locator('#filter-pills');
  await expect(pills.locator('.cap')).toHaveText('Filters');
  await expect(pills.locator('.pill').first()).toContainText('topic');
  await expect(pills.locator('.pill').first()).toContainText('seed-orders');
  await expect(pills.locator('.pill').nth(1)).toContainText('kind');
  await expect(pills.locator('a', { hasText: 'clear' })).toHaveAttribute('href', /\/traffic$/);
  await expect(page.locator('#match-count')).toHaveText(/\d+ of \d+ requests match/);
  await expect(page.locator('#traffic-note')).toContainText('Filtering looks at the last 2,000 requests kept in memory. Group requests that name this topic are included.');
  const touched = page.locator('#touched');
  await expect(page.locator('#insp-default .h2')).toHaveText('Who touched seed-orders');
  await expect(touched.locator('.kv > span:nth-child(odd)')).toHaveText(['Producers', 'Consumers', 'Produce requests', 'Fetch requests', 'Errors']);
  await expect(touched).toContainText('Open the topic');
  await expect(touched).toContainText('Select a row to see the whole request.');
  // Removing the pill removes the filter.
  await pills.locator('.pill').first().locator('a').click();
  await expect(page).not.toHaveURL(/topic=/);
});

test('Traf-03: errors only keeps the refusals, in the boards purple, and explains the most frequent', async ({ page }) => {
  await lightTraffic(page, 'traffic?kind=errors');
  const rows = page.locator('#traffic-table tbody tr');
  await expect(rows.first()).toBeVisible();
  for (const row of await rows.all()) await expect(row).toHaveClass(/errrow/);
  // The seed's duplicate CreateTopics is a refusal Kafka would make too: purple, not red.
  const refusal = rows.filter({ hasText: 'TOPIC_ALREADY_EXISTS' }).first();
  await expect(refusal.locator('td.cell-result span')).toHaveCSS('color', DENY);
  await expect(refusal.locator('td').first()).toHaveCSS('background-color', DENYBG);
  // The count at the right and the foot say what is counted.
  await expect(page.locator('#refused-count')).toHaveText(/\d+ refused/);
  await expect(page.locator('#traffic-note')).toContainText('Refusals that Kafka would also make are purple.');
  // The inspector's default is the most frequent refusal, with what it means.
  const summary = page.locator('#error-summary');
  await expect(page.locator('#insp-default .cap').first()).toHaveText('Most frequent refusal');
  await expect(summary.locator('.callout')).toHaveClass(/deny/);
  await expect(summary.locator('.callout .b')).toHaveText('Refused');
  await expect(summary).toContainText('In plain words');
  await expect(summary.locator('.kv > span:nth-child(odd)').first()).toHaveText('Client');
  // A consumer's first join is normal flow: it is on the page and is not an error.
  await page.goto('traffic?q=MEMBER_ID_REQUIRED');
  const expected = page.locator('#traffic-table tbody tr', { hasText: 'MEMBER_ID_REQUIRED' }).first();
  await expect(expected).toBeVisible();
  await expect(expected).not.toHaveClass(/errrow/);
  await page.goto('traffic?kind=errors&q=MEMBER_ID_REQUIRED');
  await expect(page.locator('#traffic-table tbody tr')).toHaveCount(0);
});

test('Traf-03: e switches to errors only and back, and the palette has Show errors', async ({ page, openPalette }) => {
  await lightTraffic(page);
  await page.keyboard.press('e');
  await expect(page).toHaveURL(/kind=errors/);
  await page.keyboard.press('e');
  await expect(page).not.toHaveURL(/kind=errors/);
  await openPalette();
  await page.keyboard.type('Show errors');
  await expect(page.locator('.pal-item', { hasText: 'Show errors' })).toBeVisible();
});

test('Traf-04: a request opens with who, what, per partition and the decoded fields, never record bytes', async ({ page }) => {
  await lightTraffic(page, 'traffic?kind=produce&topic=seed-orders');
  await page.locator('#traffic-table tbody tr').first().click();
  const insp = page.locator('#inspector');
  await expect(insp.locator('.inspector-h .cap')).toHaveText('Request');
  await expect(insp.locator('#inspector-title')).toHaveText(/^Produce v\d+$/);
  await expect(insp.locator('.insp-status')).toHaveText(/^ok · /);
  await expect(insp.locator('.inspector-sub')).toContainText('correlation');
  await expect(insp.locator('.kv > span:nth-child(odd)')).toContainText(['Client', 'Software', 'Principal', 'Remote', 'Size in / out', 'Acks']);
  const parts = insp.locator('#result-per-partition');
  await expect(parts.locator('.cap')).toHaveText('Result per partition');
  await expect(parts.locator('.r.h span')).toHaveText(['Topic', 'Part', 'Base offset', 'Result']);
  await expect(parts.locator('.r').nth(1)).toContainText('seed-orders');
  const code = insp.locator('#decoded pre.code');
  await expect(insp.locator('#decoded .cap')).toHaveText('Decoded fields');
  await expect(code).toContainText('"topics"');
  await expect(code.locator('.jt-k').first()).toHaveCSS('color', DENY); // keys are the board's purple.
  await expect(insp).toContainText('Record bytes are shown on the topic page, not here.');
  await expect(insp.locator('a.btn', { hasText: 'Follow in Topics' })).toBeVisible();
  // A refusal opens as one, in its own colour, with what it means and what to do.
  await page.goto('traffic?kind=errors');
  await page.locator('#traffic-table tbody tr', { hasText: 'TOPIC_ALREADY_EXISTS' }).first().click();
  await expect(page.locator('#request-refusal')).toHaveClass(/deny/);
  await expect(page.locator('#request-refusal')).toContainText('Real Kafka would refuse the same call.');
});

test('Traf-05: a folded row says why it folded, draws a tick per repeat and unfolds them', async ({ page }) => {
  await lightTraffic(page, `traffic?q=${GROUP}&kind=groups`);
  const folded = page.locator('#traffic-table tbody tr', { hasText: 'Heartbeat' }).filter({ hasText: /×\d+/ }).first();
  await expect(folded).toBeVisible({ timeout: 20_000 });
  await expect(folded.locator('td.cell-detail')).toHaveText(/^every [\d.]+ s, since \d\d?:\d\d:\d\d$/);
  await folded.click();
  const insp = page.locator('#inspector');
  await expect(insp.locator('.inspector-h .cap')).toHaveText('Folded row');
  await expect(insp.locator('.insp-status')).toHaveText('all ok');
  await expect(insp.locator('#fold-sentence')).toContainText('identical heartbeats are shown as one row. Each one asked the same thing and got the same answer.');
  await expect(insp.locator('.kv > span:nth-child(odd)')).toContainText(['First', 'Last', 'Interval', 'Result']);
  await expect(insp.locator('#fold-ticks .cap')).toContainText('Each tick is one heartbeat');
  expect(await insp.locator('#fold-ticks i.tick').count()).toBeGreaterThanOrEqual(2);
  await expect(insp).toContainText('Why it folded');
  await expect(insp).toContainText('Idle repeats fold; any change breaks the fold.');
  await expect(insp.locator('a.btn', { hasText: /^Unfold these \d+$/ })).toBeVisible();
  await expect(insp.locator('a.btn', { hasText: `Open ${GROUP}` })).toBeVisible();
  await insp.locator('[data-unfold]').click();
  await expect(insp.locator('#unfolded')).toBeVisible();
  await expect(insp.locator('#unfolded .r').nth(1)).toBeVisible();
});

test('Traf-06: the clients view has the board\'s nine columns, its tabs, and a rate for each client', async ({ page }) => {
  await lightTraffic(page, 'traffic?view=clients');
  await expect(page.locator('main .idband .note').first()).toHaveText('Who is connected to this broker right now, and how each one is behaving.');
  await expect(page.locator('nav.tabs a.tab')).toHaveText([/^Requests$/, /^Clients \d+$/]);
  await expect(page.locator('nav.tabs a.tab.on')).toContainText('Clients');
  await expect(page.locator('#clients-table thead th')).toHaveText(['Client', 'Software', 'Principal', 'Address', 'Open for', 'Last request', 'Req/min', 'Groups', 'Error rate']);
  const row = page.locator('#clients-table tbody tr', { hasText: CLIENT }).first();
  await expect(row).toBeVisible();
  await expect(row.locator('td').nth(8)).toHaveText(/^(\d+(\.\d)? %|—)$/);
  await expect(page.locator('#traffic-summary')).toContainText(/\d+ clients? connected/);
  await expect(page.locator('#traffic-note')).toContainText('Error rate is the requests that were refused, denied or failed over all requests in the last 5 minutes.');
  // The Requests tab goes back.
  await page.locator('nav.tabs a.tab', { hasText: 'Requests' }).click();
  await expect(page.locator('#traffic-table')).toBeVisible();
});

test('Traf-07: a client that reports its metrics puts what is worth a look first', async ({ page }) => {
  await lightTraffic(page, 'traffic?view=clients');
  const rows = page.locator('#clients-table tbody tr', { hasText: CLIENT });
  await expect(rows.first()).toBeVisible();
  const n = await rows.count();
  let worth = false;
  for (let i = 0; i < n && !worth; i++) {
    await rows.nth(i).click();
    const insp = page.locator('#client-inspector');
    await expect(insp).toBeVisible();
    if (await insp.locator('.metric-list.worth').count()) {
      worth = true;
      await expect(insp).toContainText('Metrics reported by the client itself');
      await expect(insp).toContainText('KIP-714 client telemetry. These are metrics, not traces');
      await expect(insp.locator('.cap', { hasText: 'Worth a look first' })).toBeVisible();
      await expect(insp.locator('.metric-list.worth')).toContainText('Request error rate');
      await expect(insp.locator('.cap', { hasText: 'The rest' })).toBeVisible();
      await expect(insp.locator('.metric-list.rest')).toContainText('Request latency, average');
      await expect(insp.locator('svg.spark').first()).toBeVisible();
      expect(await insp.locator('svg.spark polyline').count()).toBeGreaterThan(0);
      await expect(insp).toContainText(/Sparklines (cover the last [0-9]+ (minutes|seconds), one point per report|draw one point per report)\./);
      await expect(insp.locator('a.btn', { hasText: 'Its requests' })).toBeVisible();
      await expect(page.locator('#inspector .insp-status')).toHaveText(/^connected /);
    }
    await page.keyboard.press('Escape');
  }
  expect(worth, 'a client that reports its metrics').toBe(true);
});

test('Traf-11: a client that does not report metrics says why and what to flip', async ({ page }) => {
  await lightTraffic(page, 'traffic?view=clients');
  const rows = page.locator('#clients-table tbody tr', { hasText: QUIET });
  await expect(rows.first()).toBeVisible();
  const n = await rows.count();
  let found = false;
  for (let i = 0; i < n && !found; i++) {
    await rows.nth(i).click();
    const insp = page.locator('#client-inspector');
    await expect(insp).toBeVisible();
    if (await insp.locator('#no-metrics').count()) {
      found = true;
      await expect(insp.locator('#no-metrics .b')).toHaveText('This client does not report metrics');
      await expect(insp).toContainText('Two reasons');
      await expect(insp).toContainText('The client is too old');
      await expect(insp).toContainText('The client turned it off');
      await expect(insp).toContainText('enable.metrics.push=true');
      await expect(insp.locator('pre.code')).toContainText('--client-telemetry');
      await expect(insp.locator('a.btn', { hasText: 'Its requests' })).toBeVisible();
    }
    await page.keyboard.press('Escape');
  }
  expect(found, 'a client without metrics').toBe(true);
});

test('Traf-08: paused, the list is frozen and says how many requests it has missed', async ({ page }) => {
  await lightTraffic(page);
  const banner = page.locator('#paused-banner');
  await expect(banner).toBeHidden();
  await page.locator('#traffic-controls [data-live-toggle]').click();
  await expect(page.locator('html')).toHaveAttribute('data-paused', '');
  await expect(banner).toBeVisible();
  await expect(banner).toHaveClass(/warn/);
  await expect(banner).toHaveCSS('background-color', WARNBG);
  await expect(banner).toContainText('Paused.');
  await expect(banner).toContainText(/\d+ new requests? since \d\d:\d\d:\d\d/);
  await expect(page.locator('#traffic-controls [data-live-state]')).toHaveText('paused');
  await expect(page.locator('#filter-note .paused-only')).toHaveText('the list is frozen, the broker is not');
  await expect(page.locator('#filter-note .live-only')).toBeHidden();
  // The broker is not frozen (the held consumer beats): the count goes up while the rows stay.
  const first = await page.locator('#traffic-table tbody tr').first().getAttribute('id');
  await expect(async () => {
    expect(Number((await banner.locator('[data-new-n]').textContent())?.replace(/\D/g, ''))).toBeGreaterThan(0);
  }).toPass({ timeout: 15_000 });
  expect(await page.locator('#traffic-table tbody tr').first().getAttribute('id')).toBe(first);
  await banner.locator('button', { hasText: 'Resume' }).click();
  await expect(banner).toBeHidden();
  await expect(page.locator('html')).not.toHaveAttribute('data-paused', '');
});

test('Traf-12: a group filter tells the rebalance in order, with its generations', async ({ page }) => {
  await lightTraffic(page, `traffic?group=${GROUP}&kind=groups`);
  const story = page.locator('#traffic-story');
  await expect(story).toBeVisible();
  await expect(story.locator('.r.h span')).toHaveText(['Time', 'Client', 'Request', 'What happened', 'Result', 'Took']);
  const reb = story.locator('.r.rebalance').first();
  await expect(reb).toContainText('Rebalance');
  await expect(reb).toHaveCSS('background-color', WARNBG);
  await expect(reb.locator(':scope > span').nth(3)).toContainText(/member/);
  await expect(story.locator('.tree').first()).toHaveText('└─');
  await expect(page.locator('#story-count')).toHaveText(/\d+ requests( \(\d+ folded\))?, \d+ rebalances?/);
  await expect(page.locator('#filter-pills .pill').first()).toContainText(GROUP);
  // The inspector is the group's timeline: why, and the generations.
  const g = page.locator('#group-summary');
  await expect(page.locator('#insp-default .cap').first()).toHaveText('Group timeline');
  await expect(g.locator('.cap', { hasText: /^Generations$/ })).toBeVisible();
  await expect(g.locator('.r.h span')).toHaveText(['Gen', 'Cause', 'Members', 'At']);
  await expect(g).toContainText('JoinGroup collects the members. SyncGroup hands out the partitions.');
  await expect(g.locator('a.btn', { hasText: 'Open the full timeline' })).toHaveAttribute('href', new RegExp(`/groups/${GROUP}\\?tab=timeline$`));
});

test('the clients table links a client name to only its requests (TRF-9)', async ({ page }) => {
  await lightTraffic(page);
  const link = page.locator('#insp-default .insp-client-open', { hasText: CLIENT }).first();
  const href = await link.getAttribute('href');
  expect(href).toMatch(/\/traffic\?client=trf-hold$/);
  await page.goto(new URL(href!, page.url()).toString());
  const rows = page.locator('#traffic-table tbody tr');
  await expect(rows.first()).toBeVisible();
  for (const row of await rows.all()) await expect(row).toContainText(CLIENT);
});
