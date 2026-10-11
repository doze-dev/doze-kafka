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
  await expect(bar.locator('#workspace-label')).toHaveText(/\S/);
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
  await expect(rail.locator('a.nav .sv.c-tp')).toHaveCSS('background-color', 'rgb(45, 105, 250)');
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
    await expect(page.locator('nav.rail a.nav.on .nm')).toHaveText('Traffic');
    // The counts the board prints for an empty broker.
    await expect(page.locator('nav.rail a[href$="/topics"] .n')).toHaveText('0');
    await expect(page.locator('nav.rail a[href$="/groups"] .n')).toHaveText('0');
    await expect(page.locator('nav.rail a[href$="/traffic"] .n')).toHaveText('0 open');
    // Creating a topic ends it.
    await page.getByRole('link', { name: 'Or create it here' }).click();
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
