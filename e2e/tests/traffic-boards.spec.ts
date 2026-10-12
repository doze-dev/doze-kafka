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
  await expect(code).toContainText('"TopicData"');
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
  // The board's tick: 3 px of ink, a 1 px corner (and not the message tape's mark).
  const tick = insp.locator('#fold-ticks i.tick').first();
  await expect(tick).toHaveCSS('width', '3px');
  await expect(tick).toHaveCSS('border-top-left-radius', '1px');
  await expect(tick).toHaveCSS('border-bottom-left-radius', '1px');
  await expect(tick).toHaveCSS('background-color', 'rgb(23, 25, 28)');
  await expect(tick).toHaveCSS('opacity', '1');
  await expect(insp).toContainText('Why it folded');
  await expect(insp).toContainText('Idle repeats fold; any change breaks the fold.');
  await expect(insp.locator('a.btn', { hasText: /^Unfold these \d+$/ })).toBeVisible();
  await expect(insp.locator('a.btn', { hasText: `Open ${GROUP}` })).toBeVisible();
  await insp.locator('[data-unfold]').click();
  await expect(insp.locator('#unfolded')).toBeVisible();
  await expect(insp.locator('#unfolded .r').nth(1)).toBeVisible();
});

test('Traf-06: the clients view has the board\'s nine columns, its tabs, and a rate for each client', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
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
  // The board draws no Connected-clients panel beside the table (the table is the list),
  // so at 1440 the nine columns are not clipped and no row grows to wrap.
  await expect(page.locator('#insp-default')).toHaveCount(0);
  await expect(page.locator('#inspector-panel')).toBeHidden();
  const fit = await page.locator('#clients-table').evaluate((t) => {
    const wrap = t.closest('.tbl-wrap') as HTMLElement;
    return { scroll: wrap.scrollWidth, client: wrap.clientWidth, rows: Array.from(t.querySelectorAll('tbody tr')).map((r) => r.getBoundingClientRect().height) };
  });
  expect(fit.scroll).toBeLessThanOrEqual(fit.client);
  for (const h of fit.rows) expect(h).toBeLessThanOrEqual(40);
  // Opening a client brings the panel; Esc puts the page back as it was.
  await row.click();
  await expect(page.locator('#inspector')).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(page.locator('#inspector-panel')).toBeHidden();
  // The Requests tab goes back, and its panel with it.
  await page.locator('nav.tabs a.tab', { hasText: 'Requests' }).click();
  await expect(page.locator('#traffic-table')).toBeVisible();
  await expect(page.locator('#insp-default')).toBeVisible();
});

test('AD5: a refusal name wraps at its underscores, in the table and in the story, and is never cut', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await lightTraffic(page, 'traffic?kind=errors&q=doze-seed');
  const cell = page.locator('#traffic-table td.cell-result', { hasText: 'TOPIC_ALREADY_EXISTS' }).first();
  await expect(cell).toBeVisible();
  const lines = await cell.evaluate((td) => {
    // Each rendered line of the name, by where its words sit: a line may end only after "_".
    const text = td.firstElementChild!;
    const range = document.createRange();
    range.selectNodeContents(text);
    const rects = Array.from(range.getClientRects());
    const tops = new Set(rects.map((r) => Math.round(r.top)));
    return { tops: tops.size, whole: text.textContent, cut: (td as HTMLElement).scrollWidth > (td as HTMLElement).clientWidth };
  });
  expect(lines.whole).toBe('TOPIC_ALREADY_EXISTS');
  expect(lines.cut).toBe(false);
  // The break opportunities are after the underscores, and nowhere else.
  const html = await cell.innerHTML();
  expect(html).toContain('TOPIC_<wbr>ALREADY_<wbr>EXISTS');
  // Rows are the board's 28 px, or two lines for a long name; never the 3 lines of a mid-word break.
  const h = (await cell.evaluate((td) => td.parentElement!.getBoundingClientRect().height));
  expect(h).toBeLessThan(80);

  // The story's Result is whole as well (it was cut with an ellipsis at 80 px).
  await page.goto(`traffic?group=${GROUP}&kind=groups`);
  const results = page.locator('#traffic-story .r:not(.h) > .cell-result');
  await expect(results.first()).toBeVisible();
  for (const el of await results.all()) {
    const cut = await el.evaluate((e) => e.scrollWidth > e.clientWidth + 1);
    expect(cut).toBe(false);
    await expect(el).toHaveCSS('text-overflow', 'clip');
  }
});

/** In the page, whether a name set in `cell`'s own font has a segment (a run that ends at
 * an underscore) wider than the cell's content box: that name would break mid-word. */
async function midWordBreaks(cell: import('@playwright/test').Locator, names: string[]) {
  return cell.evaluate((td, names) => {
    const cs = getComputedStyle(td);
    const room = td.clientWidth - parseFloat(cs.paddingLeft) - parseFloat(cs.paddingRight);
    const probe = document.createElement('span');
    probe.style.cssText = `position:absolute;visibility:hidden;white-space:nowrap;font:${cs.font};letter-spacing:${cs.letterSpacing}`;
    td.appendChild(probe);
    const bad: string[] = [];
    for (const n of names) {
      const parts = n.split('_');
      for (let i = 0; i < parts.length; i++) {
        probe.textContent = i < parts.length - 1 ? `${parts[i]}_` : parts[i];
        if (probe.getBoundingClientRect().width > room + 0.01) { bad.push(`${n} (${probe.textContent})`); break; }
      }
    }
    probe.remove();
    return { room, bad };
  }, names);
}

const ERROR_NAMES = execFileSync('go', ['run', './e2e/errnames'], { cwd: '..', env: { ...process.env, GOWORK: 'off' } })
  .toString().split('\n').filter(Boolean);

test('AD5: no error name needs a break inside a word, in the table or in the story, at 1440', async ({ page }) => {
  expect(ERROR_NAMES.length).toBeGreaterThan(50);
  expect(ERROR_NAMES).toContain('TOPIC_AUTHORIZATION_FAILED');
  await page.setViewportSize({ width: 1440, height: 900 });
  await lightTraffic(page, 'traffic?kind=errors&q=doze-seed');
  const cell = page.locator('#traffic-table td.cell-result').first();
  await expect(cell).toBeVisible();
  const flat = await midWordBreaks(cell, ERROR_NAMES);
  expect(flat.bad, `the table's Result column is ${flat.room} px`).toEqual([]);

  await page.goto(`traffic?group=${GROUP}&kind=groups`);
  const sc = page.locator('#traffic-story .r:not(.h) > .cell-result').first();
  await expect(sc).toBeVisible();
  const story = await midWordBreaks(sc, ERROR_NAMES);
  expect(story.bad, `the story's Result column is ${story.room} px`).toEqual([]);
});

test('Traf-12: an ordinary story row is the board\'s 28 px', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await lightTraffic(page, `traffic?group=${GROUP}&kind=groups`);
  const rows = page.locator('#traffic-story .r:not(.h):not(.rebalance)');
  await expect(rows.first()).toBeVisible();
  const heights = await rows.evaluateAll((rs) => rs.map((r) => ({
    h: r.getBoundingClientRect().height,
    text: (r.querySelector('.cell-result') as HTMLElement | null)?.getBoundingClientRect().height ?? 0,
  })));
  expect(heights.length).toBeGreaterThan(0);
  for (const r of heights) if (r.text <= 28) expect(r.h).toBe(28);
});

test('Traf-07: a client that reports its metrics puts what is worth a look first', async ({ page }) => {
  await lightTraffic(page, 'traffic?view=clients');
  const rows = page.locator('#clients-table tbody tr', { hasText: CLIENT });
  await expect(rows.first()).toBeVisible();
  // A client's first telemetry push comes a few seconds after it connects (the broker asks
  // for one every 5 s): wait for it rather than read the page the moment the client is listed.
  const hasMetrics = async () => {
    await page.keyboard.press('Escape');
    await rows.first().click();
    // The inspector is fetched: count what it holds once it has loaded, not while it says
    // "Loading" (a count taken at once is 0 whatever the client has reported).
    await expect(page.locator('#client-inspector')).toBeVisible();
    return (await page.locator('#client-inspector .metric-list.worth').count()) > 0;
  };
  await expect.poll(hasMetrics, { timeout: 20_000, intervals: [1_000] }).toBe(true);
  await page.keyboard.press('Escape');
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

// The key strip says "↵ open": j to a row then Enter, and a focused row then Enter, open
// its request or client in the inspector, on every kind of row the page has.
for (const [name, path, row, title] of [
  ['requests', 'traffic?kind=errors&q=doze-seed', '#traffic-table tbody tr', /CreateTopics/],
  ['clients', 'traffic?view=clients', '#clients-table tbody tr', /\S/],
  ['story', `traffic?group=${GROUP}&kind=groups`, '#traffic-story .r[data-inspector-open]', /\S/],
] as const) {
  test(`keyboard: j then Enter, and a focused row then Enter, open the inspector on ${name} rows`, async ({ page }) => {
    await lightTraffic(page, path);
    const rows = page.locator(row);
    await expect(rows.first()).toBeVisible();
    const inspector = page.locator('#inspector');
    await page.locator('main h1').click(); // nothing focused; not the top bar's search, whose width follows the workspace's name.
    await page.keyboard.press('j');
    await expect(page.locator('main .cursor')).toHaveCount(1);
    await expect(page.locator('main .cursor')).toHaveAttribute('data-inspector-open', '');
    await page.keyboard.press('Enter');
    await expect(inspector).toBeVisible();
    await expect(inspector.locator('#inspector-title')).toContainText(title);
    await page.keyboard.press('Escape');
    await expect(inspector).toBeHidden();
    await rows.first().focus();
    await page.keyboard.press('Enter');
    await expect(inspector).toBeVisible();
    await expect(inspector.locator('#inspector-title')).toContainText(title);
  });
}

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
  // Traf-12 draws ONE row above the story: the field, the kinds, the group's pill,
  // "clear", and the count at the right; no summary line, no "Filters" row.
  const filterRow = page.locator('.filterrow');
  await expect(filterRow.locator('#story-count')).toHaveText(/\d+ requests( \(\d+ folded\))?, \d+ rebalances?/);
  await expect(filterRow.locator('.pill').first()).toContainText(GROUP);
  await expect(filterRow.locator('a.b', { hasText: 'clear' })).toHaveAttribute('href', /\/traffic$/);
  await expect(page.locator('#traffic-summary')).toHaveCount(0);
  await expect(page.locator('#filter-pills')).toHaveCount(0);
  // The chrome between the header and the story is the filter row (two rows at most
  // beside the inspector: 804 px cannot hold the field, the kinds, the pill, "clear" and
  // the count) and the board's gaps, not the ~104 px of summary, row, note, Filters and count.
  const chrome = await page.evaluate(() => {
    const idband = document.querySelector('main .idband')!.getBoundingClientRect();
    const story = document.querySelector('#traffic-story')!.getBoundingClientRect();
    return story.top - idband.bottom;
  });
  expect(chrome).toBeLessThanOrEqual(90);
  // Nothing is off the card's right edge: the count and "clear" are on the screen.
  const edge = await page.evaluate(() => document.querySelector('.filterrow')!.getBoundingClientRect().right);
  for (const sel of ['#story-count', '.filterrow a.b']) {
    expect((await page.locator(sel).boundingBox())!.x + (await page.locator(sel).boundingBox())!.width).toBeLessThanOrEqual(edge + 1);
  }
  // The board's foot note, and the way back to a plain list.
  const note = page.locator('#traffic-note');
  await expect(note).toContainText('Indented rows belong to the rebalance above them.');
  await expect(note).toContainText('Idle repeats fold; any change breaks the fold.');
  await expect(note.locator('a')).toHaveText('Show them as a plain list');
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

// The same boards in the dark theme (Sy-02's tokens) and at a phone's width (Sy-04).
test('dark: a refusal is the dark purple on its tint, a failure the dark red', async ({ page }) => {
  await page.addInitScript(() => { try { localStorage.setItem('theme', 'dark'); } catch { /* none */ } });
  await page.goto('traffic?kind=errors');
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
  const refusal = page.locator('#traffic-table tbody tr', { hasText: 'TOPIC_ALREADY_EXISTS' }).first();
  await expect(refusal.locator('td.cell-result span')).toHaveCSS('color', 'rgb(195, 163, 242)');
  await expect(refusal.locator('td').first()).toHaveCSS('background-color', 'rgb(42, 32, 64)');
  await page.goto('traffic?view=clients');
  await expect(page.locator('#clients-table')).toBeVisible();
});

test('at 390 the Traffic pages do not scroll sideways, in either view', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 800 });
  for (const path of ['traffic', 'traffic?view=clients', 'traffic?kind=errors', `traffic?group=${GROUP}`]) {
    await page.goto(path);
    await expect(page.locator('main h1')).toHaveText('Traffic');
    const over = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
    expect(over, `${path} is wider than the screen by ${over}px`).toBeLessThanOrEqual(0);
  }
});

// ---- the fix round: what a reviewer verified on the live page ----

const SEL = 'rgb(233, 232, 226)'; // --sel in the light theme: the fill of .r.on.

test('Main: the open row is drawn open (.on, --sel, aria-selected), through polls and a reload', async ({ page }) => {
  await lightTraffic(page, 'traffic?kind=produce&topic=seed-orders');
  const rows = page.locator('#traffic-table tbody tr');
  await expect(rows.first()).toBeVisible();
  const id = (await rows.first().getAttribute('id'))!;
  await expect(rows.first()).toHaveAttribute('aria-selected', 'false');
  await rows.first().click();
  await expect(page).toHaveURL(/open=request/);
  const row = page.locator(`[id="${id}"]`);
  await expect(row).toHaveClass(/\bon\b/);
  await expect(row).toHaveAttribute('aria-selected', 'true');
  await expect(row.locator('td').first()).toHaveCSS('background-color', SEL);
  await expect(page.locator('#traffic-table tbody tr.on')).toHaveCount(1);
  // A poll replaces the rows: the mark is still there afterwards.
  await page.waitForTimeout(3_500);
  await expect(page.locator(`[id="${id}"]`)).toHaveClass(/\bon\b/);
  // A reload with ?open= reopens it and marks it.
  await page.reload();
  await expect(page.locator('#inspector #inspector-title')).toBeVisible();
  await expect(page.locator(`[id="${id}"]`)).toHaveClass(/\bon\b/);
  await expect(page.locator(`[id="${id}"]`)).toHaveAttribute('aria-selected', 'true');
  // Closing it clears the mark.
  await page.keyboard.press('Escape');
  await expect(page.locator('#traffic-table tbody tr.on')).toHaveCount(0);
});

test('Main: a row is 28 px high, as the boards draw it', async ({ page }) => {
  await lightTraffic(page, 'traffic?kind=produce&topic=seed-orders');
  const box = await page.locator('#traffic-table tbody tr').first().boundingBox();
  expect(Math.round(box!.height)).toBe(28);
});

test('Traf-06: clients are counted by client id, with their connections per client', async ({ page }) => {
  await lightTraffic(page, 'traffic?view=clients');
  const rows = page.locator('#clients-table tbody tr');
  await expect(rows.first()).toBeVisible();
  const names = await rows.evaluateAll((trs) => trs.map((tr) => (tr.querySelector('td') as HTMLElement).childNodes[0].textContent!.trim()));
  expect(new Set(names).size, `client names ${names.join(', ')}`).toBe(names.length);
  await expect(page.locator('#traffic-summary')).toContainText(`${names.length} client${names.length === 1 ? '' : 's'} connected`);
  await expect(page.locator('nav.tabs a.tab.on')).toContainText(`Clients ${names.length}`);
  await expect(page.locator('nav.rail a.nav', { hasText: 'Traffic' }).locator('.n')).toHaveText(`${names.length} open`);
  // A client with more than one connection says how many.
  for (const row of await rows.all()) {
    const text = (await row.locator('td').first().textContent())!;
    if (/connections/.test(text)) expect(text).toMatch(/\d+ connections/);
  }
});

test('Traf-12: the requests that start a rebalance are under it, in the order they happened, with a <time> for its folded rows', async ({ page }) => {
  await lightTraffic(page, `traffic?group=${GROUP}&kind=groups`);
  const story = page.locator('#traffic-story');
  await expect(story).toBeVisible();
  const reb = story.locator('.r.rebalance').first();
  await expect(reb.locator(':scope > span').nth(3)).toContainText('a member joined');
  const parts = await story.evaluate((el) => {
    const out: { block: number; api: string; at: string; kid: boolean }[] = [];
    let block = -1;
    for (const r of Array.from(el.querySelectorAll(':scope > .r:not(.h)'))) {
      if (r.classList.contains('rebalance')) { block++; continue; }
      const spans = r.querySelectorAll(':scope > span');
      out.push({ block: r.classList.contains('kid') ? -1 : block, api: (spans[2]?.textContent ?? '').trim(), at: spans[0].querySelector('time')?.getAttribute('datetime') ?? '', kid: r.classList.contains('kid') });
    }
    return out;
  });
  const nested = parts.filter((p) => !p.kid);
  expect(nested.length).toBeGreaterThan(0);
  // The join that began the group's rebalance is under it, not before it.
  expect(parts.filter((p) => p.kid && /^(FindCoordinator|JoinGroup)\b/.test(p.api)), 'a join filed as before the rebalance').toEqual([]);
  expect(nested.some((p) => /^JoinGroup\b/.test(p.api))).toBe(true);
  // Inside a block the rows are chronological.
  for (const b of new Set(nested.map((p) => p.block))) {
    const ats = nested.filter((p) => p.block === b).map((p) => Date.parse(p.at));
    expect(ats, `block ${b} is in time order`).toEqual([...ats].sort((x, y) => x - y));
  }
  // A folded row's "since" is a <time>, localised like the flat table's.
  const since = story.locator('.r span.el', { hasText: /every [\d.]+ s, since/ });
  if (await since.count()) {
    await expect(since.first().locator('time')).toHaveCount(1);
  }
});

test('Main: Detail says something for Metadata, ListOffsets, OffsetFetch and a refused CreateTopics', async ({ page }) => {
  await lightTraffic(page);
  const wants: [string, RegExp][] = [['Metadata', /topic|auto-create/], ['OffsetFetch', /\S/], ['ListOffsets', /offset/], ['CreateTopics', /\S/]];
  for (const [api, re] of wants) {
    const rows = page.locator('#traffic-table tbody tr', { has: page.locator('td.cell-req .b', { hasText: new RegExp(`^${api}$`) }) });
    const n = await rows.count();
    for (let i = 0; i < Math.min(n, 6); i++) {
      const text = (await rows.nth(i).locator('td.cell-detail').textContent())!.trim();
      expect(text, `${api} row ${i} has no detail`).not.toBe('');
      expect(text, `${api}: ${text}`).toMatch(re);
    }
  }
  const dup = page.locator('#traffic-table tbody tr', { hasText: 'TOPIC_ALREADY_EXISTS' }).first();
  if (await dup.count()) await expect(dup.locator('td.cell-detail')).toHaveText('the topic already exists');
});

test('the open client row and the open story row are drawn open too', async ({ page }) => {
  await lightTraffic(page, 'traffic?view=clients');
  const client = page.locator('#clients-table tbody tr', { hasText: CLIENT }).first();
  await client.click();
  await expect(client).toHaveClass(/\bon\b/);
  await expect(client).toHaveAttribute('aria-selected', 'true');
  await expect(client.locator('td').first()).toHaveCSS('background-color', SEL);
  await expect(page.locator('#clients-table tbody tr.on')).toHaveCount(1);

  await lightTraffic(page, `traffic?group=${GROUP}&kind=groups`);
  const kid = page.locator('#traffic-story .r[data-row]').first();
  await expect(kid).toBeVisible();
  await kid.click();
  await expect(kid).toHaveAttribute('aria-current', 'true');
  await expect(kid).toHaveClass(/\bon\b/);
  await expect(kid).toHaveCSS('background-color', SEL);
  await expect(page.locator('#traffic-story .r[aria-current="true"]')).toHaveCount(1);
});

// ---------------------------------------------------------------------------------------
// What the evidence run measured on Main against the board.

test('Main: the fold count is never cut, a long name gives way to it, and the time is ink', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await lightTraffic(page);
  const folded = page.locator('#traffic-table tbody tr', { hasText: 'Heartbeat' }).filter({ has: page.locator('.tf-count') }).first();
  await expect(folded).toBeVisible({ timeout: 30_000 });
  // Every folded row in view: its count lies wholly inside its cell, however long the name.
  const cut = await page.evaluate(() => Array.from(document.querySelectorAll('#traffic-table td.cell-req')).flatMap((td) => {
    const count = td.querySelector('.tf-count');
    if (!count) return [];
    const c = count.getBoundingClientRect(), t = td.getBoundingClientRect();
    const pr = parseFloat(getComputedStyle(td).paddingRight);
    return c.right <= t.right - pr + 0.5 && c.left >= t.left ? [] : [`${td.textContent!.trim()}: count ${c.left}-${c.right} in cell ${t.left}-${t.right - pr}`];
  }));
  expect(cut).toEqual([]);
  // A name that does not fit is ellipsised, and says itself whole on hover.
  const longest = page.locator('#traffic-table td.cell-req', { hasText: 'GetTelemetrySubscriptions' }).first();
  if (await longest.count()) {
    await expect(longest).toHaveAttribute('title', /^GetTelemetrySubscriptions v\d+/);
    await expect(longest.locator('.req-n')).toHaveCSS('text-overflow', 'ellipsis');
  }
  // The whole request name, its version too, is the board's 600 ("Produce v9"), the count is not.
  const nameWeight = await folded.locator('.req-n .b').evaluate((e) => ({ w: getComputedStyle(e).fontWeight, t: e.textContent }));
  expect(nameWeight.w).toBe('600');
  expect(nameWeight.t).toMatch(/^\S+ v\d+$/);
  // The count is the board's plain 11 px tag, not a pill.
  const count = folded.locator('.tf-count');
  await expect(count).toHaveCSS('font-size', '11px');
  await expect(count).toHaveCSS('background-color', 'rgba(0, 0, 0, 0)');
  // The time is plain .m, ink; the client's name is plain too (it still opens the client).
  await expect(folded.locator('td.cell-time')).toHaveCSS('color', 'rgb(23, 25, 28)');
  const name = folded.locator('td.cell-client a');
  await expect(name).toHaveCSS('text-decoration-line', 'none');
  await expect(name).toHaveAttribute('hx-get', /inspect\/client\?key=/);
});

test('Main: the filter row is one row with its note at the right, as the board draws it', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await lightTraffic(page);
  const geo = await page.evaluate(() => {
    const q = (s: string) => document.querySelector(s)!.getBoundingClientRect();
    return { input: q('.filterrow input[name=q]'), seg: q('.filterrow .seg'), note: q('#filter-note'), row: q('.filterrow') };
  });
  // The note shares the field's row (its top is inside the field's height) and sits to the right of the kinds.
  expect(geo.note.top).toBeLessThan(geo.input.bottom);
  expect(geo.note.left).toBeGreaterThanOrEqual(geo.seg.right);
  expect(geo.note.right).toBeLessThanOrEqual(geo.row.right + 1);
});

test('Traf-06: a client\'s name is a bold, underlined link, as the board draws it', async ({ page }) => {
  await lightTraffic(page, 'traffic?view=clients');
  const name = page.locator('#clients-table tbody tr', { hasText: CLIENT }).first().locator('td').first().locator('a');
  await expect(name).toHaveText(CLIENT);
  await expect(name).toHaveCSS('font-weight', '600');
  await expect(name).toHaveCSS('text-decoration-line', 'underline');
  await name.click();
  await expect(page.locator('#inspector')).toBeVisible();
});

test('Main: page padding is the board\'s 16 20 0 with the 10 px gap, whatever the filter', async ({ page }) => {
  for (const path of ['traffic', 'traffic?topic=seed-orders', 'traffic?kind=errors', 'traffic?view=clients', `traffic?group=${GROUP}`]) {
    await lightTraffic(page, path);
    const main = page.locator('main.main');
    await expect(main, path).toHaveCSS('padding', '16px 20px 0px');
    await expect(main, path).toHaveCSS('row-gap', '10px');
  }
});

test('Traf-12: the filter row is one row at 1440 beside the inspector; the pill, clear and the count are never cut', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await lightTraffic(page, `traffic?group=${GROUP}`);
  await expect(page.locator('#story-count')).toBeVisible({ timeout: 30_000 });
  const m = await page.evaluate(() => {
    const r = (s: string) => document.querySelector(s)!.getBoundingClientRect();
    const row = r('.filterrow'), field = r('.filterrow input[name=q]'), seg = r('.filterrow .seg'), pill = r('.filterrow .pill');
    const clear = r('.filterrow a.b'), count = r('#story-count');
    const cy = (b: DOMRect) => b.top + b.height / 2;
    const count_el = document.querySelector('#story-count') as HTMLElement;
    return { row, field, seg, pill, clear, count, ys: [cy(field), cy(seg), cy(pill), cy(clear)], countCut: count_el.scrollWidth > count_el.clientWidth };
  });
  const ys = m.ys;
  expect(Math.max(...ys) - Math.min(...ys), JSON.stringify(m.ys)).toBeLessThanOrEqual(4);
  expect(m.count.right).toBeLessThanOrEqual(m.row.right + 1);
  expect(m.count.left).toBeGreaterThanOrEqual(m.clear.right - 1);
  expect(m.countCut).toBe(false);
  expect(m.field.width).toBeGreaterThanOrEqual(64);
  expect(m.count.height, "the count wraps in at most two lines").toBeLessThanOrEqual(42);
});

test('Main: with the inspector open Took is whole and the card does not scroll sideways at 1180, 1280, 1366 and 1440', async ({ page }) => {
  await lightTraffic(page);
  for (const width of [1180, 1280, 1366, 1440]) {
    await page.setViewportSize({ width, height: 900 });
    await page.goto('traffic');
    const row = page.locator('#traffic-table tbody tr[id]').first();
    await expect(row).toBeVisible({ timeout: 30_000 });
    await row.click();
    await expect(page.locator('#inspector')).toBeVisible();
    const m = await page.evaluate(() => {
      const t = document.querySelector('#traffic-table') as HTMLElement;
      const wrap = t.closest('.tbl-wrap') as HTMLElement;
      const took = t.querySelector('tbody td.cell-took') as HTMLElement;
      const w = wrap.getBoundingClientRect(), k = took.getBoundingClientRect();
      const heads = Array.from(t.querySelectorAll('thead th')).map((th) => Math.round(th.getBoundingClientRect().width));
      return { scroll: wrap.scrollWidth, client: wrap.clientWidth, wrapRight: w.right, tookRight: k.right, tookCut: took.scrollWidth > took.clientWidth, doc: document.documentElement.scrollWidth - document.documentElement.clientWidth, heads };
    });
    expect(m.scroll, `${width}: ${JSON.stringify(m)}`).toBeLessThanOrEqual(m.client);
    expect(m.tookRight, `${width}: ${JSON.stringify(m)}`).toBeLessThanOrEqual(m.wrapRight + 0.5);
    expect(m.tookCut, `${width}`).toBe(false);
    // Client and Topic keep room to read (the Detail column, not drawn under 1300, is what gave).
    expect(m.heads[1], `${width}: ${JSON.stringify(m.heads)}`).toBeGreaterThanOrEqual(60);
    expect(m.heads[3], `${width}: ${JSON.stringify(m.heads)}`).toBeGreaterThanOrEqual(60);
    expect(m.doc, `${width}`).toBeLessThanOrEqual(0);
  }
});
