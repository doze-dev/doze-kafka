import { test, expect } from '../fixtures/console';
import type { Page } from '@playwright/test';
import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

// Design conformance. The design (the boards, Sy-01 to Sy-08 and the section
// homes) is the specification for the console, and what these tests hold is what the
// boards state: the tokens of each theme, what is in flow and what overlays, what the
// rail holds, the copy a board prints. A change that drifts from a board fails here
// before a reviewer has to see it.

const TOKENS = {
  light: { '--bg': '#f3f3f0', '--card': '#ffffff', '--ink': '#17191c', '--mut': '#565b63', '--line': '#d9d8d2', '--line2': '#e9e8e3', '--sel': '#e9e8e2', '--well': '#f7f7f4',
    '--ok': '#166b3f', '--warn': '#855000', '--bad': '#b3251b', '--deny': '#673c9c', '--okbg': '#e3f1e8', '--warnbg': '#f6ecd6', '--badbg': '#f8e3e0', '--denybg': '#ece4f6' },
  dark: { '--bg': '#111315', '--card': '#191c1f', '--ink': '#e9eaeb', '--mut': '#a3a9b0', '--line': '#2d3136', '--line2': '#23272b', '--sel': '#262a2f', '--well': '#15181a',
    '--ok': '#5fca8f', '--warn': '#e0b060', '--bad': '#ff8a7e', '--deny': '#c3a3f2', '--okbg': '#17301f', '--warnbg': '#33290f', '--badbg': '#3a1a17', '--denybg': '#2a2040' },
  contrast: { '--bg': '#000000', '--card': '#000000', '--ink': '#ffffff', '--mut': '#d4d4d4', '--line': '#ffffff', '--line2': '#6b6b6b', '--sel': '#1f1f1f', '--well': '#0d0d0d',
    '--ok': '#6dff9f', '--warn': '#ffd24d', '--bad': '#ff9f94', '--deny': '#d9c2ff', '--okbg': '#06260f', '--warnbg': '#2b2000', '--badbg': '#330806', '--denybg': '#1f1238' },
} as const;

async function useTheme(page: Page, theme: string) {
  await page.addInitScript((t) => { try { localStorage.setItem('theme', t); } catch { /* private window */ } }, theme);
}

for (const theme of ['light', 'dark', 'contrast'] as const) {
  test(`the ${theme} theme's tokens are the board's, value for value`, async ({ page }) => {
    await useTheme(page, theme);
    await page.goto('topics');
    await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
    const got = await page.evaluate((names) => {
      const cs = getComputedStyle(document.documentElement);
      return Object.fromEntries(names.map((n) => [n, cs.getPropertyValue(n).trim().toLowerCase()]));
    }, Object.keys(TOKENS[theme]));
    expect(got).toEqual(TOKENS[theme]);
  });
}

test('the top bar is white, with the workspace, the jump field, the address and the Kafka version', async ({ page }) => {
  await useTheme(page, 'light');
  await page.goto('topics');
  const bar = page.locator('header.topbar');
  await expect(bar).toHaveCSS('background-color', 'rgb(255, 255, 255)');
  await expect(bar).toHaveCSS('height', '44px');
  await expect(bar.locator('.brand')).toContainText('doze-kafka');
  // The workspace label is drawn only for an instance with a name of its own (the brand test below holds that).
  await expect(bar.locator('#palette-open')).toContainText('Jump to a topic, group or subject, or do something…');
  await expect(bar.locator('#live-pill .pill-addr')).toHaveText(/^127\.0\.0\.1:\d+$/);
  await expect(bar.locator('#compat-pill')).toHaveText(/^Kafka \d\.0 compat$/);
  // No highlighter yellow anywhere in the chrome: the primary action is ink.
  const reds = await page.evaluate(() => Array.from(document.querySelectorAll('.topbar, .topbar *, .rail, .rail *')).map((e) => getComputedStyle(e).backgroundColor).filter((c) => /255, 2(14|1\d), \d{1,2}\)/.test(c)));
  expect(reds).toEqual([]);
});

test('a primary button is ink on paper, in every theme', async ({ page }) => {
  for (const [theme, bg, fg] of [['light', 'rgb(23, 25, 28)', 'rgb(255, 255, 255)'], ['dark', 'rgb(233, 234, 235)', 'rgb(25, 28, 31)'], ['contrast', 'rgb(255, 255, 255)', 'rgb(0, 0, 0)']] as const) {
    await page.addInitScript((t) => { try { localStorage.setItem('theme', t); } catch { /* ignore */ } }, theme);
    await page.goto('new/topic');
    await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
    const btn = page.locator('main .btn.pri').first();
    await expect(btn).toHaveCSS('background-color', bg);
    await expect(btn).toHaveCSS('color', fg);
  }
});

test('the rail has Observe, Data and Broker, each item its chip, and Connect an app at the foot', async ({ page }) => {
  await page.goto('topics');
  const rail = page.locator('nav.rail');
  await expect(rail.locator('.cap')).toHaveText(['Observe', 'Data', 'Broker']);
  await expect(rail.locator('a.nav .nm')).toHaveText(['Traffic', 'Trace', 'Topics', 'Groups', 'Schemas', 'Access', 'Cluster', 'Connect an app']);
  await expect(rail.locator('a.nav .sv')).toHaveText(['TR', 'TC', 'TP', 'GR', 'SC', 'AC', 'CL', '⇢']);
  // The chips carry the boards' colours (white letters on them).
  // (the boards' own #2f6bff and #c85f0a; their letters are black where white would
  // fall under 4.5:1, which plan/00-foundation.md records).
  await expect(rail.locator('a.nav .sv.c-tp')).toHaveCSS('background-color', 'rgb(47, 107, 255)');
  await expect(rail.locator('a.nav .sv.c-tp')).toHaveCSS('color', 'rgb(0, 0, 0)');
  await expect(rail.locator('a.nav .sv.c-tr')).toHaveCSS('background-color', 'rgb(200, 95, 10)');
  await expect(rail.locator('a.nav .sv.c-tr')).toHaveCSS('color', 'rgb(0, 0, 0)');
  await expect(rail.locator('a.nav .sv.c-gr')).toHaveCSS('color', 'rgb(255, 255, 255)');
  await expect(rail.locator('a.nav .sv.c-gr')).toHaveCSS('background-color', 'rgb(0, 133, 122)');
  // The numbers: open connections on Traffic, how many on the others.
  await expect(rail.locator('a[href$="/traffic"] .n')).toHaveText(/^\d+ open$/);
  for (const section of ['topics', 'groups', 'schemas']) await expect(rail.locator(`a[href$="/${section}"] .n`)).toHaveText(/^\d+$/);
  await expect(rail.locator('a[href$="/topics"]')).toHaveAttribute('aria-current', 'page');
  // Connect is last, and the foot item sits below a spacer.
  const last = await rail.locator('a.nav').last().evaluate((a) => a.getAttribute('href'));
  expect(last).toMatch(/\/connect$/);
});

test('the console opens on Traffic, the home page', async ({ page }) => {
  await page.goto('');
  await expect(page.locator('nav.rail a.nav.on .nm')).toHaveText('Traffic');
  await expect(page.locator('main h1')).toHaveText('Traffic');
});

test('Trace without an id asks for one, and reads a pasted header', async ({ page }) => {
  const res = await page.goto('trace');
  expect(res?.status()).toBe(200);
  await expect(page.locator('nav.rail a.nav.on .nm')).toHaveText('Trace');
  await expect(page.locator('main h1')).toHaveText('Trace');
  await expect(page.locator('#tid')).toBeVisible();
  await expect(page.getByText('Header formats it reads')).toBeVisible();
  await expect(page.getByText('It only shows what your apps propagate')).toBeVisible();
  await page.locator('#tid').fill('00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01');
  await page.getByRole('button', { name: 'Follow' }).click();
  await expect(page.locator('.copychip .cc-val')).toHaveText('4bf92f3577b34da6a3ce929d0e0e4736');
});

test('Connect an app is built from the broker: bootstrap, protocol, and no secret', async ({ page }) => {
  await page.goto('connect');
  await expect(page.locator('nav.rail a.nav.on .nm')).toHaveText('Connect an app');
  await expect(page.locator('main h1')).toHaveText('Connect an app');
  const table = page.locator('#connect-settings');
  await expect(table).toContainText('bootstrap.servers');
  await expect(table).toContainText('security.protocol');
  const boot = await table.locator('.r', { hasText: 'bootstrap.servers' }).locator('.m').nth(1).innerText();
  expect(boot).toMatch(/^[\w.\-]+:\d+$/);
  // The console's own password is never on the page, whatever the listener.
  expect(await page.content()).not.toMatch(/__console/);
  await expect(page.locator('.snippets .code').first()).toContainText(`export KAFKA_BOOTSTRAP_SERVERS=${boot}`);
});

// A fresh, empty broker: its console opens on "Your broker is up" (Sy-08).
async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const s = createServer();
    s.listen(0, '127.0.0.1', () => { const p = (s.address() as { port: number }).port; s.close(() => resolve(p)); });
    s.on('error', reject);
  });
}

test('Sy-08 at 320 with a 60-character console prefix: an address is never broken mid-word, it scrolls in its own box', async ({ page }) => {
  test.setTimeout(90_000);
  const dir = mkdtempSync(join(tmpdir(), 'doze-first-run-prefix-'));
  const bin = join(dir, 'doze-kafka');
  execFileSync('go', ['build', '-o', bin, './cmd/doze-kafka'], { cwd: '..', env: { ...process.env, GOWORK: 'off' }, stdio: 'inherit' });
  const consolePort = await freePort();
  const prefix = '/' + 'p'.repeat(59);
  let child: ChildProcess | undefined;
  try {
    child = spawn(bin, ['--listen', '127.0.0.1:0', '--console-addr', `127.0.0.1:${consolePort}`, '--console-prefix', prefix, '--schema-registry-addr', `127.0.0.1:${await freePort()}`, '--data-dir', join(dir, 'data')],
      { env: { ...process.env, DOZE_ZONE: 'off' }, stdio: 'ignore' });
    for (let i = 0; i < 100; i++) {
      try { if ((await fetch(`http://127.0.0.1:${consolePort}/healthz`)).ok) break; } catch { /* not up yet */ }
      await new Promise((r) => setTimeout(r, 200));
    }
    await page.setViewportSize({ width: 320, height: 800 });
    await page.goto(`http://127.0.0.1:${consolePort}${prefix}/`);
    await expect(page.locator('h1')).toHaveText('Your broker is up');
    const values = page.locator('section.card .kv.kv-stack > .m');
    expect(await values.count()).toBeGreaterThan(0);
    let withPrefix = 0;
    for (const v of await values.all()) {
      const m = await v.evaluate((el) => {
        const cs = getComputedStyle(el);
        return { lines: Math.round(el.getBoundingClientRect().height / parseFloat(cs.lineHeight)), wrap: cs.overflowWrap, text: el.textContent!.trim(), scroll: el.scrollWidth > el.clientWidth };
      });
      expect(m.lines, `"${m.text}" is one line`).toBe(1);
      expect(m.wrap, m.text).not.toBe('anywhere');
      if (m.text.includes(prefix)) { withPrefix++; expect(m.scroll, `"${m.text}" scrolls in its own box`).toBe(true); }
    }
    expect(withPrefix, 'an address carrying the prefix is drawn').toBeGreaterThan(0);
    const over = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
    expect(over, 'the page does not scroll sideways').toBeLessThanOrEqual(0);
  } finally {
    child?.kill('SIGTERM');
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a fresh empty broker lands on "Your broker is up", with the three steps', async ({ page }) => {
  test.setTimeout(90_000);
  const dir = mkdtempSync(join(tmpdir(), 'doze-first-run-'));
  const bin = join(dir, 'doze-kafka');
  execFileSync('go', ['build', '-o', bin, './cmd/doze-kafka'], { cwd: '..', env: { ...process.env, GOWORK: 'off' }, stdio: 'inherit' });
  const consolePort = await freePort();
  let child: ChildProcess | undefined;
  try {
    child = spawn(bin, ['--listen', '127.0.0.1:0', '--console-addr', `127.0.0.1:${consolePort}`, '--schema-registry-addr', `127.0.0.1:${await freePort()}`, '--data-dir', join(dir, 'data')],
      { env: { ...process.env, DOZE_ZONE: 'off' }, stdio: 'ignore' });
    const base = `http://127.0.0.1:${consolePort}/_console/`;
    for (let i = 0; i < 100; i++) {
      try { if ((await fetch(`http://127.0.0.1:${consolePort}/healthz`)).ok) break; } catch { /* not up yet */ }
      await new Promise((r) => setTimeout(r, 200));
    }
    await page.goto(base);
    await expect(page.locator('h1')).toHaveText('Your broker is up');
    await expect(page.locator('.firstrun .pill.ok')).toContainText('running');
    await expect(page.locator('section.card .h2')).toHaveText(['Create a topic', 'Connect an app', 'Watch it arrive']);
    await expect(page.locator('section.card .num')).toHaveText(['1', '2', '3']);
    await expect(page.locator('section.card pre.code').first()).toContainText('kafka-topics.sh');
    await expect(page.getByText('waiting for the first request')).toBeVisible();
    // Sy-08's copy where it is true for this build: step 1's sentence is the board's
    // word for word, and the page is padded 36 56 0 with a 20 px gap.
    await expect(page.locator('section.card').first().locator('.note').first())
      .toHaveText('Any Kafka tool works. Auto-create is also on, so a producer can simply name a new topic.');
    await expect(page.locator('main.firstrun')).toHaveCSS('padding', '36px 56px 0px');
    await expect(page.locator('main.firstrun')).toHaveCSS('row-gap', '20px');
    // The status line is one line: the address, the compat and the data directory (cut
    // with the whole of it on hover when the path is long), never wrapped.
    const status = page.locator('main.firstrun .pill.ok + .m');
    expect(await status.evaluate((el) => Math.round(el.getBoundingClientRect().height / parseFloat(getComputedStyle(el).lineHeight)))).toBe(1);
    await expect(status).toHaveAttribute('title', /data in /);
    // At 1440 each address is one line: the console's URL is never broken mid-word.
    await page.setViewportSize({ width: 1440, height: 900 });
    for (const v of await page.locator('section.card .kv.kv-stack > .m').all()) {
      const lines = await v.evaluate((el) => Math.round(el.getBoundingClientRect().height / parseFloat(getComputedStyle(el).lineHeight)));
      expect(lines, `"${await v.innerText()}" is on one line`).toBe(1);
    }
    await expect(page.locator('nav.rail a.nav.on .nm')).toHaveText('Traffic');
    // The counts the board prints for an empty broker.
    await expect(page.locator('nav.rail a[href$="/topics"] .n')).toHaveText('0');
    await expect(page.locator('nav.rail a[href$="/groups"] .n')).toHaveText('0');
    await expect(page.locator('nav.rail a[href$="/traffic"] .n')).toHaveText('0 open');
    // Tp-01: the empty Topics page says nothing exists, what it is not, and the shell command.
    await page.goto(`${base}topics`);
    await expect(page.locator('.list .list-note').first()).toContainText('No topics');
    await expect(page.locator('.list .list-note').first()).toContainText('Nothing exists on this broker yet.');
    await expect(page.locator('#topics-none')).toContainText('This broker has no topics.');
    await expect(page.locator('#topics-none')).toContainText('This is not a filter and not a permissions problem.');
    await expect(page.locator('#topics-none pre.code')).toContainText(/kafka-topics\.sh --bootstrap-server 127\.0\.0\.1:\d+ \\\n\s+--create --topic orders\.created/);
    await expect(page.locator('#topics-none')).toContainText('The console makes exactly the same CreateTopics request.');
    // Tp-02: the create form has the replication factor, the shell command that follows it, and what happens next.
    await page.goto(base);
    await page.getByRole('link', { name: 'Or create it here' }).click();
    await expect(page.locator('#f-replication')).toBeDisabled();
    await expect(page.locator('#f-replication-teach')).toContainText('Always 1. This is one broker with no followers to copy to.');
    await expect(page.locator('#f-partitions-teach')).toContainText(/^1 to \d+\. This is the broker's cap\. Partitions can be added later and never removed/);
    // No raw record fields in any field's help.
    const help = await page.locator('.teach, .hint').allInnerTexts();
    for (const h of help) expect(h, 'help text').not.toMatch(/unit:|range:|; applied/);
    await expect(page.getByText('What happens next')).toBeVisible();
    await expect(page.locator('#shell-preview')).toContainText('--partitions 1 --replication-factor 1');
    await page.locator('#f-name').fill('first-topic');
    await page.locator('#f-partitions').fill('3');
    await page.locator('#cfg-segment\\.bytes').fill('1MB');
    await expect(page.locator('#shell-preview')).toContainText('--create --topic first-topic');
    await expect(page.locator('#shell-preview')).toContainText('--partitions 3 --replication-factor 1');
    await expect(page.locator('#shell-preview')).toContainText('--config segment.bytes=1048576');
    // Creating a topic ends it.
    await page.locator('#f-name').fill('first-topic');
    await page.getByRole('button', { name: 'Create topic' }).click();
    await expect(page).toHaveURL(/topics\/first-topic/);
    await page.goto(base);
    await expect(page.locator('h1')).not.toHaveText('Your broker is up');
  } finally {
    child?.kill('SIGTERM');
    await new Promise((r) => setTimeout(r, 300));
    rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------------------
// Sy-01 and Sy-02: the inspector is part of the page, in flow, beside the content.

test('the inspector is in flow: it takes room from the page, covers nothing, and has no scrim', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('topics/seed-orders?partition=0');
  const main = page.locator('main#detail');
  const before = await main.boundingBox();
  await page.locator('#msg-5').click();
  const panel = page.locator('#inspector-panel');
  await expect(panel).toBeVisible();
  await expect(page.locator('#inspector #inspector-title')).toHaveAttribute('data-offset', '5');
  const after = await main.boundingBox();
  const box = await panel.boundingBox();
  expect(after!.width).toBeLessThan(before!.width); // it took room from the page,
  expect(after!.x + after!.width).toBeLessThanOrEqual(box!.x + 1); // and does not overlap it,
  expect(box!.width).toBe(420); // at the width Sy-02 draws it,
  expect(await panel.evaluate((el) => getComputedStyle(el).position)).not.toMatch(/fixed|absolute/);
  // Nothing is drawn over the content: no overlay, no scrim, no dialog.
  await expect(page.locator('#inspector-overlay, .scrim')).toHaveCount(0);
  expect(await page.locator('.overlay:visible').count()).toBe(0);
  await expect(page.locator('#inspector')).not.toHaveAttribute('aria-modal', 'true');
  // It holds the board's parts: the kind, the title, the key, the value as code, the headers.
  await expect(panel.locator('.inspector-h .cap')).toHaveText('Message');
  await expect(panel.locator('.kv').first()).toContainText('Key');
  await expect(panel.locator('pre.code').first()).toBeVisible();
  await expect(panel.getByRole('link', { name: 'Follow this trace' })).toBeVisible();
  // Esc puts the page back as it was.
  await page.keyboard.press('Escape');
  await expect(panel).toBeHidden();
  const back = await main.boundingBox();
  expect(back!.width).toBe(before!.width);
});

test('on Traffic the inspector lists the open connections until a row replaces them, and Esc puts them back', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('traffic');
  const panel = page.locator('#inspector-panel');
  await expect(panel).toBeVisible();
  await expect(panel.locator('.cap').first()).toHaveText('Connected clients');
  await expect(panel.locator('.h2')).toHaveText(/\d+ open connections?/);
  await expect(panel.locator('.r.h')).toContainText('Client');
  const row = page.locator('#traffic-table tbody tr').first();
  await row.click();
  await expect(page.locator('#inspector #inspector-title')).toBeVisible();
  await expect(page.locator('#insp-default')).toBeHidden(); // replaced, not stacked.
  await page.keyboard.press('Escape');
  await expect(page.locator('#insp-default')).toBeVisible();
  // The requests table comes first, then one field and one segmented control above it.
  const order = await page.evaluate(() => {
    const m = document.querySelector('main#detail')!;
    const y = (sel: string) => (m.querySelector(sel) as HTMLElement | null)?.getBoundingClientRect().top ?? -1;
    return { summary: y('#traffic-summary'), filter: y('.filterrow'), table: y('#traffic-table') };
  });
  expect(order.summary).toBeGreaterThan(0);
  expect(order.summary).toBeLessThan(order.filter);
  expect(order.filter).toBeLessThan(order.table);
  await expect(page.locator('.filterrow input[name=q]')).toHaveAttribute('placeholder', 'Client, topic, group or request');
  await expect(page.locator('.filterrow .seg a')).toHaveText(['all', 'errors', 'produce', 'fetch', 'groups', 'admin']);
  await expect(page.locator('#traffic-table thead th')).toHaveText(['Time', 'Client', 'Request', 'Topic or group', 'Detail', 'Result', 'Took']);
  await expect(page.locator('#traffic-summary')).toContainText('kept: the last');
  await expect(page.locator('main#detail')).toContainText("The console's own requests are left out");
});

// Sy-01: the anatomy of a detail page.
test('a topic page is the board: one sentence, one action and ···, neighbours, tabs, try it, the wire', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('topics/seed-orders?partition=0');
  const head = page.locator('main#detail .idband').first();
  await expect(head.locator('h1')).toHaveText('seed-orders');
  // One solid primary button, and the destructive actions are under ···.
  await expect(head.locator('.btn.pri')).toHaveCount(1);
  await expect(head.locator('.btn.pri')).toHaveText('Produce a message');
  await expect(page.getByRole('button', { name: 'Delete', exact: true })).toBeHidden();
  await page.getByRole('button', { name: 'More actions' }).click();
  await expect(page.getByRole('button', { name: 'Purge' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Delete', exact: true })).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(page.getByRole('button', { name: 'Delete', exact: true })).toBeHidden();
  // Facts: a sentence, not a strip of boxes.
  const facts = page.locator('#topic-facts');
  await expect(facts).toHaveText(/^3 partitions, [\d,]+ messages?, .*\.$/);
  expect(await facts.evaluate((el) => el.tagName)).toBe('P');
  // Neighbours: linked wells.
  await expect(page.locator('#neighbours a.well').first()).toBeVisible();
  // Tabs, in the board's order.
  await expect(page.locator('main .tabs .tab')).toHaveText([/^Messages/, /^Partitions/, /^Readers/, /^Config/]);
  await expect(page.locator('main .tabs .tab.on')).toHaveText(/^Messages/);
  // Try it, and the wire underneath with its live pill, below the page and beside the inspector.
  await expect(page.locator('#compose-panel .t')).toHaveText('Try it: produce');
  const dock = page.locator('#wire-dock');
  await expect(dock.locator('.dh .b')).toHaveText('The wire, underneath');
  await expect(dock.locator('.dh .livetoggle, .dh .pill.ok').first()).toContainText(/live/i);
  const m = await page.locator('main#detail').boundingBox();
  const d = await dock.boundingBox();
  expect(d!.y).toBeGreaterThanOrEqual(m!.y + m!.height - 1);
  expect(d!.x).toBeCloseTo(m!.x, 0);
  // The list is on the left, 248 px, with the number that matters.
  const list = await page.locator('aside.list').boundingBox();
  expect(list!.width).toBe(248);
  await expect(page.locator('aside.list .li[data-name="seed-orders"]')).toHaveClass(/on/);
});

test('the keys strip is at the foot of every page and says keys the page has', async ({ page }) => {
  for (const [path, want] of [
    ['traffic', ['pause']], ['topics', ['create topic']], ['topics/seed-orders', ['close panel', 'topics', 'produce']],
    ['groups', ['filter']], ['schemas', ['register schema']], ['cluster', ['cluster']], ['access', ['open']], ['trace', ['trace id']], ['connect', ['jump or act']],
  ] as const) {
    await page.goto(path);
    const strip = page.locator('#keys');
    await expect(strip, path).toBeVisible();
    for (const w of want) await expect(strip, `${path} says ${w}`).toContainText(w);
    expect(await strip.locator('.kbd').count(), path).toBeGreaterThan(1);
  }
  // It follows the page when it is navigated to by the rail.
  await page.goto('topics');
  await page.locator('nav.rail a.nav', { hasText: 'Traffic' }).click();
  await expect(page.locator('#keys')).toContainText('pause');
});

test('every section has the board\'s home: its title, its summary line and its table', async ({ page }) => {
  await page.goto('topics');
  await expect(page.locator('#topics-summary')).toContainText(/\d+ topics?/);
  await expect(page.locator('#topics-table .r.h span')).toHaveText(['Topic', 'Partitions', 'Messages', 'Retention', 'Read by', 'Compact', 'Schema subject']);
  await page.goto('groups');
  await expect(page.locator('#groups-summary')).toContainText('records behind in total');
  await expect(page.locator('#groups-table .r.h span')).toHaveText(['Group', 'Protocol', 'State', 'Members', 'Topics', 'Lag', 'Generation']);
  await expect(page.locator('.filterrow .seg button')).toHaveText(['all', 'with members', 'empty', 'behind']);
  await page.goto('schemas');
  await expect(page.locator('#schemas-summary')).toContainText('subjects');
  await expect(page.locator('#schemas-table .r.h span')).toHaveText(['Subject', 'Type', 'Versions', 'Latest id', 'Level', 'Topics using it']);
  await page.goto('cluster');
  await expect(page.locator('#cluster-sentence')).toContainText(/^Node \d+/);
  await expect(page.locator('#cluster-identity .r.h')).toContainText('Identity');
  await expect(page.locator('#cluster-counts .r.h')).toContainText('Counts');
  await expect(page.locator('aside.list .li').first()).toHaveText(/Overview/);
  await page.goto('access');
  await expect(page.locator('#acl-table .r.h span')).toHaveText(['Principal', 'Type', 'Name', 'Pattern', 'Operation', 'Permission', 'Host']);
  await expect(page.locator('main .callout').first()).toContainText('These are enforced');
});

test('the groups segmented control narrows the table', async ({ page }) => {
  await page.goto('groups');
  const all = await page.locator('#groups-table > a.r:not([hidden])').count();
  await page.getByRole('button', { name: 'behind', exact: true }).click();
  const behind = await page.locator('#groups-table > a.r:not([hidden])').count();
  expect(behind).toBeGreaterThan(0);
  expect(behind).toBeLessThan(all);
  await page.getByRole('button', { name: 'all', exact: true }).click();
  expect(await page.locator('#groups-table > a.r:not([hidden])').count()).toBe(all);
  await page.locator('main input[data-filter-target]').first().fill('g-lag');
  await expect(page.locator('#groups-table > a.r:not([hidden])')).toHaveCount(1);
});

// Sy-03 and Sy-04: the same page at 960 and at 390.
test('at 960 the rail is chips, the list is one tap away and the inspector is a card between the page and its dock', async ({ page }) => {
  await page.setViewportSize({ width: 960, height: 760 });
  await page.goto('topics/seed-orders?partition=0');
  expect(await page.locator('nav.rail').evaluate((el) => el.getBoundingClientRect().width)).toBe(48);
  await expect(page.locator('aside.list')).toBeHidden();
  await expect(page.locator('.backrow a')).toContainText('‹ Topics');
  await expect(page.locator('#compat-pill')).toBeHidden();
  await page.locator('#msg-5').click();
  const panel = page.locator('#inspector-panel');
  await expect(panel).toBeVisible();
  const p = await panel.boundingBox();
  const dock = await page.locator('#wire-dock').boundingBox();
  const main = await page.locator('main#detail').boundingBox();
  expect(p!.y).toBeGreaterThanOrEqual(main!.y + main!.height - 1); // below the page,
  expect(p!.y + p!.height).toBeLessThanOrEqual(dock!.y + 1); // above its dock: stacked, in flow.
  expect(await panel.evaluate((el) => getComputedStyle(el).position)).toBe('static');
});

// Sy-07: three token sets, and a text control for them.
test('the theme control is words: system, light, dark, high contrast', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('topics');
  await expect(page.locator('#theme-seg button')).toHaveText(['system', 'light', 'dark', 'high contrast']);
  await page.locator('#theme-seg [data-mode="contrast"]').click();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'contrast');
  // Selection is drawn with a 2 px outline as well, so it does not depend on a fill.
  const on = page.locator('nav.rail a.nav.on');
  await expect(on).toHaveCSS('outline-width', '2px');
  await expect(on).toHaveCSS('outline-style', 'solid');
  await page.locator('#theme-seg [data-mode="light"]').click();
});

// Sy-05: the palette.
test('the palette is grouped under Topics, Groups, Subjects and Actions, says what each row is, and has a foot', async ({ page, openPalette }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('topics/seed-orders');
  await openPalette();
  const modal = page.locator('#palette [role=dialog]');
  await page.waitForTimeout(300); // the pop-in finishes.
  expect((await modal.boundingBox())!.width).toBe(640);
  await expect(page.locator('#pal-q')).toHaveAttribute('placeholder', 'Jump to a topic, group, subject or action');
  await page.locator('#pal-q').fill('seed');
  const caps = page.locator('#pal-list .pal-cap');
  await expect(caps.first()).toHaveText('Topics');
  await expect(page.locator('#pal-list .pal-cap', { hasText: 'Subjects' })).toBeVisible();
  // The match is bold, and a topic says its partitions and messages.
  const first = page.locator('#pal-list .pal-item').first();
  await expect(first.locator('b')).toHaveText('seed');
  await expect(first.locator('.pal-detail')).toContainText(/\d+ partitions? · [\d,]+ messages?/);
  await expect(first.locator('.sv')).toHaveText('TP');
  await expect(first.locator('.kbd')).toHaveText('↵'); // the selected row says what Enter does.
  await expect(page.locator('#pal-count')).toHaveText(/^\d+ groups? of results$/); // the board counts groups.
  // Groups say their state, members and lag.
  await page.locator('#pal-q').fill('g-lag');
  await expect(page.locator('#pal-list .pal-cap').first()).toHaveText('Groups');
  await expect(page.locator('#pal-list .pal-item').first().locator('.pal-detail')).toContainText(/Empty · 0 members · lag 39/);
  // Actions: produce to the topic you are on, and create one; with their keys.
  await page.locator('#pal-q').fill('produce');
  await expect(page.locator('#pal-list .pal-cap', { hasText: 'Actions' })).toBeVisible();
  await expect(page.locator('#pal-list .pal-item', { hasText: 'Produce to seed-orders' })).toBeVisible();
  await page.locator('#pal-q').fill('create topic');
  await expect(page.locator('#pal-list .pal-item', { hasText: 'Create topic' }).locator('.kbd').first()).toHaveText('c');
  // No Go to group: the sections are the rail's and the g chords'.
  await page.locator('#pal-q').fill('a');
  await expect(page.locator('#pal-list .pal-cap', { hasText: 'Go to' })).toHaveCount(0);
  for (const cap of await page.locator('#pal-list .pal-cap').allTextContents()) expect(['Topics', 'Groups', 'Subjects', 'Actions']).toContain(cap);
  await page.locator('#pal-q').fill('cluster');
  await expect(page.locator('#pal-list .pal-item', { hasText: /^Cluster$/ })).toHaveCount(0);
  // The foot: Connect an app, and the keys.
  const foot = page.locator('#palette .pal-foot');
  await expect(foot).toContainText('Connect an app');
  await expect(foot).toContainText('move');
  await expect(foot).toContainText('close');
  // Produce to opens the composer.
  await page.locator('#pal-q').fill('produce to');
  await page.keyboard.press('Enter');
  await expect(page.locator('html')).toHaveAttribute('data-dock', 'open');
});

test('a group\'s page offers to reset its offsets from the palette, and says it asks first', async ({ page, openPalette }) => {
  await page.goto('groups/g-lag');
  await openPalette();
  await page.locator('#pal-q').fill('reset offsets');
  const row = page.locator('#pal-list .pal-item', { hasText: 'Reset offsets for g-lag' });
  await expect(row).toBeVisible();
  await expect(row).toContainText('asks first');
  await page.keyboard.press('Enter');
  await expect(page).toHaveURL(/tab=offsets/);
});

// Sy-04: at 390 the top bar is a menu, the logo, the page title and ⌘K; messages are
// cards; the message is a sheet from the foot.
test('at 390 the top bar is a menu button, logo, title and the palette, and messages are cards in a sheet', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('topics/seed-orders?partition=0');
  await expect(page.locator('#menu-btn')).toBeVisible();
  await expect(page.locator('#menu-btn')).toHaveText('☰');
  await expect(page.locator('#pg-title')).toHaveText('Topics');
  await expect(page.locator('#palette-open .kbd')).toBeVisible();
  await expect(page.locator('nav.rail')).toBeHidden();
  await page.locator('#menu-btn').click();
  await expect(page.locator('nav.rail')).toBeVisible();
  await page.locator('#menu-btn').click();
  await expect(page.locator('nav.rail')).toBeHidden();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  const row = page.locator('#msg-5');
  expect(await row.evaluate((el) => getComputedStyle(el).display)).toBe('grid');
  await row.click();
  const panel = page.locator('#inspector-panel');
  await expect(panel).toBeVisible();
  expect(await panel.evaluate((el) => getComputedStyle(el).position)).toBe('fixed');
  const b = await panel.boundingBox();
  expect(b!.y + b!.height).toBeGreaterThanOrEqual(843);
});

// Main: the Traffic table shows the whole time, the whole "took" and the whole result;
// a header never runs into the next one. (The audit read "17:12:57.1…", "0.0…" and
// "TOPIC_ALREADY…" off the first build of this table.)
test('the Traffic table does not truncate time, took or result, and its headers do not collide', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('traffic');
  const table = page.locator('#traffic-table');
  await expect(table.locator('tbody tr', { hasText: 'TOPIC_ALREADY_EXISTS' })).toHaveCount(1);
  const bad = await table.evaluate((t) => {
    const clipped: string[] = [];
    // Time (1), Result (6), Took (7) are shown whole on every row.
    for (const tr of Array.from(t.querySelectorAll('tbody tr'))) {
      for (const i of [0, 5, 6]) {
        const td = tr.children[i] as HTMLElement;
        if (td.scrollWidth > td.clientWidth) clipped.push(`${td.textContent!.trim()} (${td.scrollWidth} > ${td.clientWidth})`);
      }
    }
    // A header's words fit its cell, so neighbours cannot run into one another.
    for (const th of Array.from(t.querySelectorAll('thead th'))) {
      const el = th as HTMLElement;
      if (el.scrollWidth > el.clientWidth) clipped.push(`th ${el.textContent} (${el.scrollWidth} > ${el.clientWidth})`);
    }
    return clipped;
  });
  expect(bad).toEqual([]);
  // The result column's text starts where its header does, and Took ends where its header ends.
  const left = await table.evaluate((t) => {
    const th = t.querySelector('thead th:nth-child(6)') as HTMLElement;
    const td = t.querySelector('tbody tr td:nth-child(6)') as HTMLElement;
    return [th.getBoundingClientRect().left, td.getBoundingClientRect().left];
  });
  expect(left[1]).toBeCloseTo(left[0], 0);
  const right = await table.evaluate((t) => {
    const th = t.querySelector('thead th:nth-child(7)') as HTMLElement;
    const td = t.querySelector('tbody tr td:nth-child(7)') as HTMLElement;
    return [th.getBoundingClientRect().right, td.getBoundingClientRect().right];
  });
  expect(right[1]).toBeCloseTo(right[0], 0);
});

test('the Traffic summary says the window of each number', async ({ page }) => {
  await page.goto('traffic');
  const sum = page.locator('#traffic-summary');
  await expect(sum).toContainText(/clients? connected/);
  // Three items, as Main prints them, then the note on what is kept.
  await expect(sum).toHaveText(/^\s*\d+ clients? connected\s*·\s*(at least )?\d+ requests? in the last minute\s*·\s*(at least )?\d+ errors?\s*·\s*kept: the last [\d,]+, in memory only\s*$/);
  await expect(sum.locator('> .m')).toHaveCount(4);
  await expect(sum.locator('> .mut:not(.m)')).toHaveText(['·', '·', '·']);
  // The errors are those of the minute, and the number says so on hover.
  await expect(sum.locator('> .m').nth(2)).toHaveAttribute('title', /last minute/);
  await expect(sum).toContainText(/kept: the last [\d,]+, in memory only/);
});

test('the keys strip is never clipped: whole at 960 and 1440, not drawn at 390 where the board draws none', async ({ page }) => {
  for (const w of [1440, 960]) {
    await page.setViewportSize({ width: w, height: 800 });
    for (const path of ['traffic', 'topics', 'topics/seed-orders']) {
      await page.goto(path);
      const strip = page.locator('#keys');
      await expect(strip).toBeVisible();
      expect(await strip.evaluate((el) => el.scrollWidth <= el.clientWidth), `${path} at ${w}`).toBe(true);
    }
  }
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('traffic');
  await expect(page.locator('#keys')).toBeHidden();
});

test('at 390 the Traffic requests are cards: time, request, what it was about, and how it went', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('traffic');
  const row = page.locator('#traffic-table tbody tr').first();
  await expect(row).toBeVisible();
  expect(await row.evaluate((el) => getComputedStyle(el).display)).toBe('grid');
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  // The result is on the card, whole.
  const res = row.locator('td.cell-result');
  await expect(res).toBeVisible();
  expect(await res.evaluate((el) => el.scrollWidth <= el.clientWidth)).toBe(true);
});

// Ac-Home: type and operation are words (Topic, Write), the permission is ALLOW or DENY.
test('the ACL table writes type and operation as words and the permission in capitals', async ({ page }) => {
  await page.goto('access');
  const rows = page.locator('#acl-table .r:not(.h)');
  await expect(rows.first()).toBeVisible();
  for (const row of await rows.all()) {
    const cells = await row.locator('span').allInnerTexts();
    expect(cells[1], 'type').toMatch(/^(Topic|Group|Cluster|TransactionalId|DelegationToken)$/);
    expect(cells[4], 'operation').toMatch(/^[A-Z][A-Za-z]+$/);
    expect(cells[5], 'permission').toMatch(/^(ALLOW|DENY)$/);
  }
});

// Tp-Home: "read by nobody" says what it is counted from, and "written by" says where its
// number comes from and how old the oldest of those requests is.
test('the Topics table qualifies read by and written by', async ({ page }) => {
  await page.goto('topics');
  await expect(page.locator('#topics-table')).not.toContainText(/^nobody$/m);
  await expect(page.locator('#topics-summary')).toContainText(/written by \d+ clients?/);
  await expect(page.locator('#topics-summary')).toContainText(/from the last .+ of requests/);
  await page.goto('topics/seed-orders');
  await expect(page.locator('#neighbours-note')).toContainText('Written by is from the last ');
});

// Gr-02: the Lag tab ends in Try it, the fields OffsetCommit takes.
test('a group\'s Lag tab ends in Try it: topic, partition, new committed offset and Set offset', async ({ page }) => {
  await page.goto('groups/g-lag');
  const card = page.locator('#try-offset');
  await expect(card).toContainText('Try it');
  await expect(card).toContainText('the same fields OffsetCommit and the admin API take');
  await expect(card.getByLabel('Topic')).toHaveValue('seed-orders');
  await expect(card.getByLabel('Partition')).toHaveValue('0');
  await expect(card.getByLabel('New committed offset')).toHaveValue(/^\d+$/);
  await expect(card.getByRole('button', { name: 'Set offset' })).toBeEnabled();
  // The neighbours: what it reads.
  await expect(page.locator('#neighbours')).toContainText('seed-orders');
});

// Tp-02: the form says a name is taken before it is sent.
test('the create form says a name already exists before anything is sent', async ({ page }) => {
  await page.goto('new/topic');
  await expect(page.getByText('Names that already exist')).toBeVisible();
  await expect(page.getByText(/are taken\. The form says so before you send, and the broker would answer TOPIC_ALREADY_EXISTS/)).toBeVisible();
  const warn = page.locator('#f-name-taken');
  await expect(warn).toBeHidden();
  const shell = page.waitForResponse((r) => r.url().includes('/new/topic/shell'));
  await page.locator('#f-name').fill('seed-orders');
  await expect(warn).toBeVisible();
  await expect(warn).toContainText('already exists on this broker');
  // The command under the form follows what is typed (and the route gate sees it driven).
  await shell;
  await expect(page.locator('#shell-preview')).toContainText('--create --topic seed-orders');
  await page.locator('#f-name').fill('seed-orders-new');
  await expect(warn).toBeHidden();
});

// ---------------------------------------------------------------------------------------
// The audit's measurements, each against the board's own CSS or markup.

test('the shell is the board\'s: the top bar, the brand, the page padding and the page title', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  // The page boards (Main, Traf-02..12, the section homes, Tp-05) draw `padding: 16px
  // 20px 0` with the gap each sets: 10 on a page of parts, 12 on a home with nothing
  // open, 8 on a topic (Tp-05). Sy-01/02/05 draw 14 18 as the specimen of the anatomy;
  // that is not the page.
  for (const [path, gap] of [['topics', '12px'], ['traffic', '10px'], ['groups', '10px'], ['schemas', '10px'], ['access', '10px'], ['cluster', '12px'],
    ['trace', '12px'], ['topics/seed-orders', '8px']] as const) {
    await page.goto(path);
    const main = page.locator('.panes > .main, main.main').first();
    await expect(main, `${path} padding`).toHaveCSS('padding', '16px 20px 0px');
    await expect(main, `${path} gap`).toHaveCSS('row-gap', gap);
    await expect(main.locator('h1').first()).toHaveCSS('font-size', '18px');
    await expect(main.locator('h1').first()).toHaveCSS('line-height', '24px');
    // The inspector is 400 px (the board's .insp) until a message is open.
    expect((await page.locator('.insp').boundingBox())?.width ?? 400).toBe(400);
    // The brand is as wide as its words ("width: auto" on every board), never a fixed 182.
    const brand = page.locator('header.topbar .brand');
    const w = (await brand.boundingBox())!.width;
    // "width: auto": exactly as wide as its logo, its name and its label, whatever the label is.
    const content = await brand.evaluate((el) => {
      const kids = Array.from(el.children) as HTMLElement[];
      return kids[kids.length - 1].getBoundingClientRect().right - el.getBoundingClientRect().left;
    });
    expect(w).toBeCloseTo(content, 0);
  }
});

test('the brand says doze-kafka once, and the instance only when it has a name of its own', async ({ page }) => {
  await page.goto('topics');
  const text = (await page.locator('header.topbar .brand').innerText()).replace(/\s+/g, ' ').trim();
  expect(text).not.toMatch(/doze-kafka doze-kafka/);
  expect(text).not.toMatch(/doze-kafka local$/);
  const label = page.locator('#workspace-label');
  if (await label.count()) expect(['doze-kafka', 'local']).not.toContain((await label.innerText()).trim());
});

test('the top bar holds no control that is hidden: Retry is drawn with the disconnected pill, and only then', async ({ page }) => {
  await page.goto('topics');
  await expect(page.locator('header.topbar #pill-retry')).toHaveCount(0);
  expect(await page.locator('header.topbar button[hidden], header.topbar a[hidden]').count()).toBe(0);
});

test('the Traffic columns: the board\'s tracks where the page has room, and nothing pushed off the edge at 1440 beside the inspector', async ({ page }) => {
  // The board's tracks are 92 150 118 1fr 190 84 52, 906 px with the gaps and the row's
  // padding; its own render at 1440 beside the 400 px inspector has 802, leaves the
  // topic 8 px and cuts the Time. So: Time 92, Request 130 (the board's 118 is
  // "Heartbeat v4 x18" and cannot hold "FindCoordinator v4"), Result 118 (AD5) and Took 52
  // are fixed; Client, Topic and Detail share the rest, and there is no sideways scroll.
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('traffic');
  const wrap = page.locator('#traffic-table').locator('xpath=ancestor::div[contains(@class,"tbl-wrap")]');
  const fit = await wrap.evaluate((w) => ({ client: w.clientWidth, scroll: w.scrollWidth }));
  expect(fit.scroll, 'the table is as wide as its card').toBeLessThanOrEqual(fit.client);
  const cells = await page.locator('#traffic-table thead tr:first-child th').evaluateAll((ths) => ths.map((th) => {
    const cs = getComputedStyle(th);
    const w = th.getBoundingClientRect().width;
    // The track is the cell less its padding; the board's gap is the two paddings that meet.
    return { track: Math.round(w - parseFloat(cs.paddingLeft) - parseFloat(cs.paddingRight)), pl: cs.paddingLeft, pr: cs.paddingRight, right: th.getBoundingClientRect().right };
  }));
  const [time, , request, , , result, took] = cells.map((c) => c.track);
  expect([time, request, result, took]).toEqual([92, 130, 118, 52]);
  for (const i of [1, 3, 4]) expect(cells[i].track, `column ${i + 1} keeps room to be read`).toBeGreaterThanOrEqual(80);
  for (let i = 0; i < cells.length - 1; i++) {
    const meet = parseFloat(cells[i].pr) + parseFloat(cells[i + 1].pl);
    expect(meet, `gap after column ${i + 1}`).toBe(14);
  }
  expect(cells[0].pl).toBe('12px');
  expect(cells[6].pr).toBe('12px');
  // The last column ends inside the card: Took is on screen.
  const cardRight = await wrap.evaluate((w) => w.getBoundingClientRect().right);
  expect(cells[6].right).toBeLessThanOrEqual(cardRight + 1);
  // From 1600 the board's own tracks hold beside the inspector, and the topic's 1fr is the rest.
  await page.setViewportSize({ width: 1920, height: 1000 });
  const wide = await page.locator('#traffic-table thead tr:first-child th').evaluateAll((ths) => ths.map((th, i) => Math.round(th.getBoundingClientRect().width - (i === 0 ? 19 : i === 6 ? 19 : 14))));
  expect([wide[0], wide[1], wide[2], wide[4], wide[5], wide[6]]).toEqual([92, 150, 130, 190, 118, 52]);
  expect(wide[3]).toBeGreaterThan(300);
  // The status strip's weights: the three counts and the note are mono, the dots are muted.
  const strip = page.locator('#traffic-summary');
  for (const el of await strip.locator('> .m').all()) await expect(el).toHaveCSS('font-family', /Plex Mono/);
  await expect(strip.locator('> .mut:not(.m)').first()).toHaveCSS('color', 'rgb(86, 91, 99)');
});

test('the wire dock writes the client, the request, the detail, the result and the time taken whole at 1280', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.goto('topics/seed-orders');
  const dock = page.locator('#wire-dock');
  await expect(dock.locator('.dh .b')).toHaveText('The wire, underneath');
  const row = dock.locator('.r').first();
  await expect(row).toBeVisible();
  const cols = await row.evaluate((r) => getComputedStyle(r).gridTemplateColumns.split(' ').map(parseFloat));
  expect(cols.length).toBe(6);
  expect(cols.slice(0, 3)).toEqual([92, 120, 100]);
  expect(cols.slice(4)).toEqual([60, 48]);
  expect(cols[3]).toBeGreaterThanOrEqual(120);
  // Nothing in the first rows is cut: the detail reads "p0 · 1 batch · ..." in full, not "p…".
  const cut = await dock.locator('.r').evaluateAll((rows) => rows.slice(0, 4).flatMap((r) =>
    Array.from(r.children).filter((c) => c.scrollWidth > c.clientWidth).map((c) => c.textContent!.trim())));
  expect(cut).toEqual([]);
  const detail = await row.locator('> span').nth(3).innerText();
  expect(detail).not.toMatch(/…$/);
  expect(detail.length).toBeGreaterThan(3);
});

test('the palette offers Produce to and Reset offsets for what the query matched, with the section chords', async ({ page, openPalette }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('traffic');
  await openPalette();
  // The first row that is not the selected one in a group says the chord to its section.
  const groups = page.locator('#pal-list .pal-cap', { hasText: 'Groups' });
  await expect(groups).toBeVisible();
  const chords = await page.locator('#pal-list .pal-item .kbd').allTextContents();
  expect(chords).toEqual(expect.arrayContaining(['g g']));
  // No theme row: the theme is the top bar's control, and Sy-05 lists none.
  await page.locator('#pal-q').fill('theme');
  await expect(page.locator('#pal-list .pal-item', { hasText: /theme/i })).toHaveCount(0);
  // Typing a name offers to produce to the topic it matched, from a page that is not that topic.
  await page.locator('#pal-q').fill('seed-orders');
  const produce = page.locator('#pal-list .pal-item', { hasText: 'Produce to seed-orders' });
  await expect(produce).toBeVisible();
  await expect(produce.locator('.kbd')).toHaveText('p');
  await produce.click();
  await expect(page).toHaveURL(/topics\/seed-orders/);
  await expect(page.locator('html')).toHaveAttribute('data-dock', 'open');
  // And the group it matched can have its offsets reset, which says it asks first.
  await openPalette();
  await page.locator('#pal-q').fill('g-lag');
  const reset = page.locator('#pal-list .pal-item', { hasText: 'Reset offsets for g-lag' });
  await expect(reset).toBeVisible();
  await expect(reset.getByText('asks first', { exact: true })).toBeVisible();
  await reset.click();
  await expect(page).toHaveURL(/groups\/g-lag\?tab=offsets/);
});

// ---------------------------------------------------------------------------------------
// What the evidence run measured against the boards (plan/evidence). Each is a computed
// value or a structure of the page, held so that it cannot drift again.

test('the title of a topic, a group and a subject is the boards\' .h1.m: Plex Sans 600, 18 px', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  for (const path of ['topics/seed-orders', 'groups/g-lag', 'schemas/seed-events-value']) {
    await page.goto(path);
    const h1 = page.locator('main h1').first();
    await expect(h1, path).toHaveCSS('font-family', /IBM Plex Sans/);
    await expect(h1, path).toHaveCSS('font-weight', '600');
    await expect(h1, path).toHaveCSS('font-size', '18px');
    await expect(h1, path).toHaveCSS('line-height', '24px');
  }
});

test('every .btn is weight 400, <button> or <a> (the boards draw them all as buttons); only Cl-Home\'s "Connect an app" is 500', async ({ page }) => {
  await page.goto('topics/seed-orders');
  await expect(page.locator('main button.btn.pri').first()).toHaveCSS('font-weight', '400');
  await expect(page.locator('main button.btn').first()).toHaveCSS('font-weight', '400');
  await page.goto('topics');
  await expect(page.locator('a.btn', { hasText: '+ New' })).toHaveCSS('font-weight', '400');
  await page.goto('groups/g-lag');
  await expect(page.locator('main a.btn', { hasText: 'Reset offsets' }).first()).toHaveCSS('font-weight', '400');
  await page.goto('cluster');
  await expect(page.locator('main a.btn', { hasText: 'Connect an app' }).first()).toHaveCSS('font-weight', '500');
});

test('Sy-01: the record table is P, Offset, Key, Value, Age, in rows of 28 with the offset a plain 12 px mono', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('topics/seed-orders?partition=0');
  const table = page.locator('#messages-table');
  await expect(table.locator('thead th')).toHaveText(['P', 'Offset', 'Key', 'Value', 'Age']);
  const row = table.locator('tbody tr[id^="msg-"]').first();
  expect((await row.boundingBox())!.height).toBe(28);
  const off = row.locator('.cell-offset');
  await expect(off).toHaveCSS('font-family', /Plex Mono/);
  await expect(off).toHaveCSS('font-size', '12px');
  await expect(off).toHaveCSS('font-weight', '400');
  // The board's columns: 26 62 120 1fr 62 with 14 px between (the offset is 72: the
  // board's 62 holds 4,789, not 1,234,567).
  const tracks = await table.locator('thead th').evaluateAll((ths) => ths.map((th) => {
    const cs = getComputedStyle(th);
    return Math.round(th.getBoundingClientRect().width - parseFloat(cs.paddingLeft) - parseFloat(cs.paddingRight));
  }));
  expect([tracks[0], tracks[1], tracks[2], tracks[4]]).toEqual([26, 72, 120, 62]);
  // Nothing a record has is out of reach: the age says the moment on hover, and the
  // headers and the timestamp are the inspector's.
  await expect(row.locator('.cell-age time')).toHaveAttribute('title', /\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3} UTC$/);
  const withHeaders = table.locator('tbody tr[id^="msg-"] .cell-val[title*="header"]').first();
  await expect(withHeaders).toHaveAttribute('title', /\d+ headers?$/);
  await withHeaders.click();
  await expect(page.locator('#inspector')).toContainText('Headers ·');
  await expect(page.locator('#inspector')).toContainText('Timestamp');
});

test('the shortcuts sheet is three columns in a 900 px dialog, with no scroll (Sy-06)', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('traffic');
  await page.locator('main h1').click();
  await page.keyboard.press('?');
  const help = page.locator('#help .help');
  await expect(help).toBeVisible();
  expect(await help.evaluate((el) => (el as HTMLElement).offsetWidth)).toBe(900); // not the box mid-animation (scaled .98)
  const lefts = await page.locator('#help .help-g').evaluateAll((gs) => gs.map((g) => Math.round(g.getBoundingClientRect().left)));
  expect(new Set(lefts).size, 'three columns side by side').toBe(3);
  const body = await page.locator('#help .help-b').evaluate((b) => ({ ch: b.clientHeight, sh: b.scrollHeight, cw: b.clientWidth, sw: b.scrollWidth }));
  expect(body.sh).toBeLessThanOrEqual(body.ch);
  expect(body.sw).toBeLessThanOrEqual(body.cw);
  // The board's sheet starts 90 px down at 900 high (10vh), not 108.
  await expect.poll(async () => Math.round((await help.boundingBox())!.y)).toBe(90);
});

test('Sy-05: the palette says the chord to a section on the first row of each group that is not the selected one', async ({ page, openPalette }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('topics');
  await openPalette();
  const rows = page.locator('#pal-list .pal-item');
  await expect(rows.first()).toHaveClass(/sel/);
  await expect(rows.first().locator('.kbd')).toHaveText('↵');
  // Topics: the selected row says ↵, the next says g t. Groups: g g. Subjects: g s.
  await expect(page.locator('#pal-list .pal-item', { has: page.locator('.sv.c-tp') }).nth(1).locator('.kbd')).toHaveText('g t');
  await expect(page.locator('#pal-list .pal-item', { has: page.locator('.sv.c-gr') }).first().locator('.kbd')).toHaveText('g g');
  await expect(page.locator('#pal-list .pal-item', { has: page.locator('.sv.c-sc') }).first().locator('.kbd')).toHaveText('g s');
  // Actions: Create topic is c.
  await expect(page.locator('#pal-list .pal-item', { hasText: 'Create topic' }).locator('.kbd')).toHaveText('c');
});

test('Sy-04: the message sheet is 47% of the phone, has its grab handle, and leaves the tabs in view', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('topics/seed-orders?partition=0&open=5');
  const sheet = page.locator('aside.insp');
  await expect(sheet.locator('.insp-sel')).toBeVisible();
  const box = (await sheet.boundingBox())!;
  expect(box.height / 844).toBeLessThanOrEqual(0.475);
  expect(box.height / 844).toBeGreaterThan(0.4);
  // The handle: 36 x 4, centred.
  const handle = await sheet.evaluate((el) => {
    const cs = getComputedStyle(el, '::before');
    return { w: cs.width, h: cs.height, content: cs.content, br: cs.borderRadius };
  });
  expect(handle).toEqual({ w: '36px', h: '4px', content: '""', br: '2px' });
  // The page's tabs and its first record are above the sheet, not under it.
  const tabs = (await page.locator('nav.tabs').boundingBox())!;
  expect(tabs.y + tabs.height).toBeLessThanOrEqual(box.y + 1);
  const firstCard = (await page.locator('tbody tr[id^="msg-"]').first().boundingBox())!;
  expect(firstCard.y).toBeLessThan(box.y);
  // The board's sheet is a fixed 400 px, its actions pinned at the foot whatever the value.
  expect(Math.round(box.height)).toBe(400);
  const acts = (await sheet.locator('.inspector-acts').boundingBox())!;
  expect(box.y + box.height - (acts.y + acts.height), 'actions end at the sheet\'s foot, nothing of the value under them').toBeLessThanOrEqual(1);
  expect(acts.y).toBeGreaterThan(box.y + 100);
});

test('Sy-04: at 390 the title is 15 px on its own row, then a full-width Produce a message with the menu beside it', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('topics/seed-orders?partition=0');
  const h1 = page.locator('main h1').first();
  await expect(h1).toHaveCSS('font-size', '15px');
  const t = (await h1.boundingBox())!;
  const produce = (await page.locator('#produce-open').boundingBox())!;
  const more = (await page.locator('[data-more]').first().boundingBox())!;
  expect(produce.y, 'the action is under the title').toBeGreaterThanOrEqual(t.y + t.height - 1);
  expect(produce.width, 'full width, with the menu beside it').toBeGreaterThan(300);
  expect(more.x).toBeGreaterThanOrEqual(produce.x + produce.width);
  expect(Math.abs(more.y - produce.y)).toBeLessThan(4);
});

test('Cl-Home: the compacted partitions are counted from the topics\' own cleanup.policy, and what this broker is not is drawn', async ({ page }) => {
  await page.goto('cluster');
  const counts = page.locator('#cluster-counts');
  const row = counts.locator('a.r', { hasText: 'Compacted partitions' });
  await expect(row).toBeVisible();
  const n = Number(await row.locator('.ra').innerText());
  // The seed's harbour-berths is compacted (cleanup.policy=compact, one partition); the
  // rest are not, so the count is of some partitions, never all of them.
  const all = Number(await counts.locator('a.r', { hasText: /^Partitions/ }).locator('.ra').innerText());
  expect(n).toBeGreaterThanOrEqual(1);
  expect(n).toBeLessThan(all);
  const panel = page.locator('#cluster-is-not');
  await expect(panel.locator('.cap')).toHaveText('What this broker is not');
  await expect(panel.locator('.pill')).toHaveText(['single node', 'replication factor reported as 1', 'no leader election, no ISR', 'no KRaft, no ZooKeeper', 'not durable against disk loss']);
  await expect(panel.locator('a')).toHaveText('All limits');
});

// One name, one component. `.tick` was the message tape's mark (8 px, the input
// colour, the recent ones inked) and, 700 lines later in the stylesheet, the fold
// tick of the Traffic inspector (the boards' 3 px of ink); the later rule redrew the
// tape as 4 px of ink at .55. Each is now its own rule, said by where it stands.
// `.form-sec`, `.note` and `.panes` had a second definition each: what is held here
// is the one that was meant.
test('the message tape draws its own marks, which the inspector\'s fold ticks do not restyle', async ({ page }) => {
  await useTheme(page, 'light');
  await page.goto('topics/seed-orders?tab=messages');
  const strip = page.locator('.tape-strip');
  await expect(strip.locator('.tick').first()).toBeVisible();
  // An older record's mark: the input colour (the line token), 8 px, rounded on top.
  const old = strip.locator('.tick:not(.hot)').first();
  await expect(old).toHaveCSS('width', '8px');
  await expect(old).toHaveCSS('background-color', 'rgb(217, 216, 210)');
  await expect(old).toHaveCSS('border-top-left-radius', '2px');
  await expect(old).toHaveCSS('border-top-right-radius', '2px');
  await expect(old).toHaveCSS('border-bottom-left-radius', '0px');
  await expect(old).toHaveCSS('opacity', '0.55');
  // The most recent are inked, and the newest is whole.
  const hot = strip.locator('.tick.hot');
  await expect(hot.first()).toHaveCSS('background-color', 'rgb(23, 25, 28)');
  await expect(hot.first()).toHaveCSS('opacity', '0.8');
  await expect(hot.first()).toHaveCSS('width', '8px');
  await expect(hot.last()).toHaveCSS('background-color', 'rgb(23, 25, 28)');
  await expect(hot.last()).toHaveCSS('opacity', '1');
  // The page is the Sy-01 grid, and a note is the muted ink.
  await expect(page.locator('.panes')).toHaveCSS('display', 'grid');
  await expect(page.locator('.tape-foot').first()).toBeVisible();
  await expect(page.locator('#wire-dock .dh .note')).toHaveCSS('color', 'rgb(86, 91, 99)');
  await expect(page.locator('#wire-dock .dh .note')).toHaveCSS('font-size', '12px');
});

// Tp-02 stacks the form's cards 12 px apart.
test('Tp-02: the create form\'s cards are 12 px apart', async ({ page }) => {
  await page.goto('new/topic');
  const cards = page.locator('#create-topic .form-sec');
  await expect(cards.first()).toHaveCSS('margin-bottom', '12px');
  const a = (await cards.nth(0).boundingBox())!;
  const b = (await cards.nth(1).boundingBox())!;
  expect(Math.round(b.y - (a.y + a.height))).toBe(12);
  await page.goto('new/schema');
  await expect(page.locator('form.form-page .form-sec').first()).toHaveCSS('margin-bottom', '12px');
});
