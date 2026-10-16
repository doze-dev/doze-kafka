import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { mkdirSync, rmSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { test, expect } from '../fixtures/console';
import { createRequire } from 'node:module';
import type { Page } from '@playwright/test';
import { PREFIX } from '../playwright.config';

const AXE = createRequire(import.meta.url).resolve('axe-core/axe.min.js');
/** Every violation axe finds but the minor ones, of the page as it is now. */
async function axeProblems(page: Page, dimmedRows = false): Promise<string[]> {
  await page.addScriptTag({ path: AXE });
  // The frozen rows are dimmed to .55 as Traf-10 draws them (plan/00-foundation.md): those rows are left out of this one sweep (every other state checks rows).
  const res = await page.evaluate(async (dimmed) => (window as any).axe.run(dimmed ? { exclude: [['#traffic-table .tf-rows']] } : document, { resultTypes: ['violations'] }), dimmedRows);
  return (res.violations as any[]).filter((v) => v.impact !== 'minor')
    .map((v) => `${v.impact} ${v.id}: ${v.help} e.g. ${v.nodes.slice(0, 3).map((n: any) => n.target.join(' ')).join(' | ')}`);
}

// The console when the broker is not there. This spec runs its own broker (its own
// ports, and a data directory whose path has a space in it), kills it in the middle
// of a poll, and brings it back on the same address and data.

const PORT = Number(process.env.E2E_PORT ?? 14092) + 10;
const CONSOLE = `http://127.0.0.1:${PORT + 1}${PREFIX}/`;
const ROOT = resolve(process.cwd(), '..');
const BIN = join(ROOT, 'e2e', '.tmp', 'bin', `doze-kafka-disc-${PORT}`);
const DATA = join(ROOT, 'e2e', '.tmp', `data disc ${PORT}`); // a space in the path, in every run.

let proc: ChildProcess | undefined;

async function up(): Promise<void> {
  proc = spawn(BIN, ['--listen', `127.0.0.1:${PORT}`, '--console-addr', `127.0.0.1:${PORT + 1}`, '--schema-registry-addr', `127.0.0.1:${PORT + 2}`, '--data-dir', DATA],
    { cwd: ROOT, env: { ...process.env, DOZE_ZONE: 'off', DOZE_KAFKA_CONSOLE_PREFIX: PREFIX }, stdio: 'ignore' });
  for (let i = 0; i < 150; i++) {
    try {
      if ((await fetch(`http://127.0.0.1:${PORT + 1}/healthz`)).ok) return;
    } catch { /* not yet */ }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error('the broker did not come up');
}

async function kill(): Promise<void> {
  const p = proc;
  if (!p) return;
  await new Promise<void>((done) => {
    p.once('exit', () => done());
    p.kill('SIGKILL');
  });
  proc = undefined;
}

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
  mkdirSync(join(ROOT, 'e2e', '.tmp', 'bin'), { recursive: true });
  execFileSync('go', ['build', '-o', BIN, './cmd/doze-kafka'], { cwd: ROOT, env: { ...process.env, GOWORK: 'off' } });
  rmSync(DATA, { recursive: true, force: true });
  mkdirSync(DATA, { recursive: true });
  await up();
});

test.afterAll(async () => {
  await kill();
});

test('the broker stops mid-poll: one state on the pill, the rows frozen, no toast, and recovery without a reload', async ({ page, request, uniqueName }) => {
  const topic = uniqueName('gone');
  expect((await request.post(`${CONSOLE}new/topic`, { form: { name: topic, partitions: '1' } })).ok()).toBe(true);
  expect((await request.post(`${CONSOLE}topics/${topic}/produce`, { form: { partition: '0', partition_ctx: '0', value: 'before-the-stop' } })).ok()).toBe(true);

  await page.goto(`${CONSOLE}topics/${topic}`);
  await expect(page.locator('tr[id^="msg-"]')).toHaveCount(1);
  await expect(page.locator('#live-pill')).not.toHaveClass(/disconnected/);
  // Marks this document, so that "without a reload" can be told; counts every toast.
  await page.evaluate(() => {
    (window as any).__marker = 'same-document';
    (window as any).__toasts = 0;
    new MutationObserver((ms) => ms.forEach((m) => m.addedNodes.forEach((n) => {
      if ((n as HTMLElement).classList?.contains('toast')) (window as any).__toasts++;
    }))).observe(document.getElementById('toasts')!, { childList: true });
  });

  await kill();

  // The pill says so, once, with the last time anything was heard.
  const pill = page.locator('#live-pill');
  await expect(pill).toHaveClass(/disconnected/, { timeout: 15_000 });
  // Traf-10's top bar: the address and "not reachable", the pill red-tinted.
  await expect(pill.locator('.pill-state')).toHaveText('not reachable');
  await expect(pill).toHaveClass(/\bbad\b/);
  await expect(pill).toHaveCSS('background-color', 'rgb(248, 227, 224)');
  await expect(pill).toHaveAttribute('title', /Not reachable\. The last answer was at \d{1,2}:\d{2}/);
  // What was on screen stays, frozen under the veil.
  await expect(page.locator('#tape-and-table.disconnected')).toBeVisible();
  await expect(page.locator('tr[id^="msg-"]')).toHaveCount(1);
  await expect(page.locator('tr[id^="msg-"]')).toContainText('before-the-stop');
  // Several polls fail while we look: still no toast, ever.
  await page.waitForTimeout(8_000);
  expect(await page.evaluate(() => (window as any).__toasts)).toBe(0);
  await expect(page.locator('.toast')).toHaveCount(0);

  // The broker returns on the same address and data: the page recovers by itself.
  await up();
  await expect(pill).not.toHaveClass(/disconnected/, { timeout: 20_000 });
  await expect(page.locator('#tape-and-table.disconnected')).toHaveCount(0);
  expect((await request.post(`${CONSOLE}topics/${topic}/produce`, { form: { partition: '0', partition_ctx: '0', value: 'after-the-return' } })).ok()).toBe(true);
  await expect(page.locator('tr[id^="msg-"]')).toHaveCount(2, { timeout: 15_000 });
  expect(await page.evaluate(() => (window as any).__marker)).toBe('same-document'); // no reload happened.
  expect(await page.evaluate(() => (window as any).__toasts)).toBe(0);
});

// The pill is the shell's, on every page (A7): a page with nothing live of its own (the
// Topics list, Cluster, Connect, a form, a page that says "not found") learns from the
// shell's heartbeat that the broker is gone, says so on the pill, raises no error, and
// comes back by itself. The verification of 82b8752 found the pill green on 10 of 19
// page kinds 8 s after the process was killed.
test('every kind of page says the broker is gone, and comes back, without a reload or an error', async ({ context, request, uniqueName }) => {
  test.setTimeout(240_000);
  const topic = uniqueName('every');
  expect((await request.post(`${CONSOLE}new/topic`, { form: { name: topic, partitions: '1' } })).ok()).toBe(true);
  // A group with a member, so a group's own page (two regions that poll) is among them.
  const holdBin = join(ROOT, 'e2e', '.tmp', 'bin', `hold-every-${PORT}`);
  execFileSync('go', ['build', '-o', holdBin, './e2e/hold'], { cwd: ROOT, env: { ...process.env, GOWORK: 'off' } });
  const hold = spawn(holdBin, [`127.0.0.1:${PORT}`, 'every-hold', `${topic}-group`, topic],
    { cwd: ROOT, stdio: ['ignore', 'pipe', 'inherit'], env: { ...process.env, E2E_SASL_USER: '', E2E_TLS: '' } });
  await new Promise<void>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('the held consumer never joined its group')), 20_000);
    hold.stdout!.on('data', (b) => { if (String(b).includes('ready')) { clearTimeout(t); resolve(); } });
    hold.on('exit', () => reject(new Error('the held consumer exited')));
  });
  hold.kill('SIGTERM'); // its committed offsets keep the group; its page is what is wanted.
  const kinds = ['traffic', 'traffic?view=clients', 'trace', 'topics', `topics/${topic}`, `topics/${topic}?tab=partitions`, `topics/${topic}?tab=config`,
    'groups', `groups/${topic}-group`, `groups/${topic}-group?tab=members`, 'groups/nobody-here', 'topics/nothing-here', 'schemas', 'new/schema',
    'access', 'cluster', 'cluster?view=transactions', 'connect', 'new/topic', ''];
  const pages = [];
  const errors: string[] = [];
  for (const k of kinds) {
    const p = await context.newPage();
    p.on('pageerror', (e) => errors.push(`${k}: ${e.message || String(e)}`));
    await p.goto(CONSOLE + k);
    await expect(p.locator('#live-pill')).not.toHaveClass(/disconnected/);
    await p.evaluate(() => { (window as any).__marker = 'same-document'; });
    pages.push(p);
  }
  await kill();
  for (const [i, p] of pages.entries()) {
    await p.bringToFront();
    await expect(p.locator('#live-pill'), `${kinds[i]} while the broker is gone`).toHaveClass(/disconnected/, { timeout: 12_000 });
    await expect(p.locator('#live-pill .pill-state')).toHaveText('not reachable');
    await expect(p.locator('html')).toHaveAttribute('data-connection-lost', '');
    await expect(p.locator('.toast')).toHaveCount(0);
  }
  // A page whose regions poll keeps failing while the broker is gone: several polls
  // in front, and still no error and no toast.
  for (const k of [`topics/${topic}`, `groups/${topic}-group`]) {
    const p = pages[kinds.indexOf(k)];
    await p.bringToFront();
    await p.waitForTimeout(8_000);
    await expect(p.locator('.toast')).toHaveCount(0);
  }
  await up();
  for (const [i, p] of pages.entries()) {
    await p.bringToFront();
    await expect(p.locator('#live-pill'), `${kinds[i]} once the broker is back`).not.toHaveClass(/disconnected/, { timeout: 12_000 });
    expect(await p.evaluate(() => (window as any).__marker)).toBe('same-document');
  }
  expect(errors).toEqual([]);
  for (const p of pages) await p.close();
});

test('at a phone width the pill still says so, and Retry asks at once', async ({ page, request, uniqueName }) => {
  const topic = uniqueName('narrow');
  expect((await request.post(`${CONSOLE}new/topic`, { form: { name: topic, partitions: '1' } })).ok()).toBe(true);
  await page.setViewportSize({ width: 390, height: 800 });
  await page.goto(`${CONSOLE}topics/${topic}`);
  await expect(page.locator('#tape-and-table[data-live]')).toBeVisible();
  await kill();
  const pill = page.locator('#live-pill');
  await expect(pill).toHaveClass(/disconnected/, { timeout: 15_000 });
  await expect(pill).toBeVisible();
  await expect(pill.locator('.pill-addr')).toBeHidden();
  await up();
  await page.locator('#pill-retry').click();
  await expect(pill).not.toHaveClass(/disconnected/, { timeout: 5_000 });
});

// Traf-10: Traffic loses the broker. The header says when it was last good and offers
// Retry now; a red callout says what the rows are; the rows stay, dimmed, under a line
// that says nothing newer is known; and the page comes back by itself.
test('Traffic when the broker is lost: the pill, the callout, the frozen line, Retry now, and recovery', async ({ page, request, uniqueName }) => {
  // An app's request, so there are rows to freeze (the console's own are left out).
  const topic = uniqueName('frozen');
  expect((await request.post(`${CONSOLE}new/topic`, { form: { name: topic, partitions: '1' } })).ok()).toBe(true);
  execFileSync('go', ['run', './e2e/put', `127.0.0.1:${PORT}`, topic, '2026-10-10T12:00:00Z', 'x'],
    { cwd: ROOT, env: { ...process.env, GOWORK: 'off', E2E_SASL_USER: '', E2E_TLS: '' } });
  await page.goto(`${CONSOLE}traffic`);
  await expect(page.locator('#traffic-table .tf-rows > .r[data-row]').first()).toBeVisible();
  const callout = page.locator('#lost-callout');
  await expect(callout).toBeHidden();
  await kill();

  await expect(page.locator('html')).toHaveAttribute('data-connection-lost', '', { timeout: 15_000 });
  await expect(callout).toBeVisible();
  await expect(callout).toHaveClass(/bad/);
  await expect(callout.locator('.b')).toHaveText('The console lost the broker');
  await expect(callout).toContainText('The rows below are the last ones received and are no longer updating. The console retries every 5 s on its own.');
  await expect(callout.locator('[data-lastgood]')).toHaveText(/^\d{1,2}:\d{2}:\d{2}/);
  const header = page.locator('#traffic-controls');
  await expect(header.locator('.pill.bad')).toContainText('disconnected · last good');
  await expect(header.locator('button', { hasText: 'Retry now' })).toBeVisible();
  await expect(header.locator('[data-live-state]')).toBeHidden();
  // Traf-10's top bar: the address and "not reachable" in a red-tinted pill (the
  // board's .pill.bad: badbg, bad ink), no Retry inside it or beside it (Retry now is
  // the page header's), and the jump field is still the board's 420.
  await page.setViewportSize({ width: 1440, height: 900 });
  const top = page.locator('header.topbar #live-pill');
  await expect(top.locator('.pill-addr')).toHaveText(/^127\.0\.0\.1:\d+$/);
  await expect(top.locator('.pill-state')).toHaveText('not reachable');
  await expect(top).toHaveCSS('background-color', 'rgb(248, 227, 224)');
  await expect(top).toHaveCSS('color', 'rgb(179, 37, 27)');
  await expect(top.locator('.dot')).toHaveCSS('background-color', 'rgb(179, 37, 27)');
  await expect(page.locator('header.topbar #pill-retry')).toHaveCount(0);
  expect((await page.locator('header.topbar #palette-open').boundingBox())!.width).toBe(420);
  await expect(page.locator('.tf-frozen')).toContainText('Nothing newer than this line is known.');
  expect(await axeProblems(page, true), 'the lost page, accessible').toEqual([]);
  // Traf-10 draws it as the row under the header, not above it.
  const below = await page.evaluate(() => {
    const head = document.querySelector('#traffic-table .r.h > span')!.getBoundingClientRect();
    const frozen = document.querySelector('.tf-frozen')!.getBoundingClientRect();
    const first = document.querySelector('#traffic-table .tf-rows > .r[data-row]')!.getBoundingClientRect();
    return { headBottom: head.bottom, frozenTop: frozen.top, frozenBottom: frozen.bottom, firstTop: first.top };
  });
  expect(below.frozenTop).toBeGreaterThanOrEqual(below.headBottom - 1);
  expect(below.firstTop).toBeGreaterThanOrEqual(below.frozenBottom - 1);
  // The rows are dimmed, and only the rows: the header and the frozen line are not.
  await expect(page.locator('#traffic-table .tf-rows')).toHaveCSS('opacity', '0.55');
  await expect(page.locator('#traffic-table')).toHaveCSS('opacity', '1');
  await expect(page.locator('#traffic-table .r.h')).toHaveCSS('opacity', '1');
  await expect(page.locator('.tf-frozen')).toHaveCSS('opacity', '1');
  await expect(page.locator('#traffic-table .tf-rows > .r[data-row]').first()).toBeVisible(); // still there.
  // What is counted from the broker is not drawn while it cannot be reached: the board
  // has no summary and no connected-clients panel.
  await expect(page.locator('#traffic-summary')).toBeHidden();
  await expect(page.locator('#insp-default')).toBeHidden();
  // The key strip is the board's: r, j k, jump.
  const strip = page.locator('#keys');
  await expect(strip.locator('.keys-lost')).toHaveText([/retry now$/, /move$/, /jump or act$/]);
  await expect(strip.locator('.keys-live')).not.toHaveCount(0);
  for (const k of await strip.locator('.keys-live').all()) await expect(k).toBeHidden();

  await up();
  // r is Retry now.
  await page.keyboard.press('r');
  await expect(page.locator('html')).not.toHaveAttribute('data-connection-lost', '', { timeout: 20_000 });
  await expect(callout).toBeHidden();
  await expect(header.locator('[data-live-state]')).toBeVisible();
  await expect(page.locator('#traffic-summary')).toBeVisible();
  await expect(strip.locator('.keys-lost').first()).toBeHidden();
});

// Traf-09 and Traf-10 together: a broker that has just started has nothing to show, and
// when the console loses it the page does not say "up and listening".
test('Traf-09: nothing has reached the broker, and Traf-10 over it', async ({ page }) => {
  await kill();
  await up(); // a fresh recorder: only the console's own requests, which are left out.
  await page.goto(`${CONSOLE}traffic`);
  const empty = page.locator('#traffic-empty');
  await expect(empty).toBeVisible();
  await expect(empty.locator('.h2')).toHaveText('Nothing has reached this broker yet');
  await expect(empty.locator('.up-only')).toBeVisible();
  expect(await axeProblems(page), 'the empty page, accessible').toEqual([]);
  // The board draws no filter, no kinds, no connected clients and no "select a row".
  await expect(page.locator('.filterrow')).toBeHidden();
  await expect(page.locator('.seg')).toBeHidden();
  await expect(page.locator('#inspector-panel')).toBeHidden();
  await expect(page.locator('#traffic-note')).toBeHidden();
  await expect(page.locator('#traffic-summary')).toContainText('0 clients connected');
  await expect(page.locator('#traffic-summary')).toContainText('0 requests so far');
  // The keys: c connects an app, and does.
  await expect(page.locator('#keys')).toContainText('connect an app');
  await expect(page.locator('#keys')).toContainText('pause');
  await expect(page.locator('#keys')).not.toContainText('errors only');
  await page.keyboard.press('c');
  await expect(page).toHaveURL(/\/connect$/);

  // Now the console loses it: the empty card must not claim the broker is listening.
  await page.goto(`${CONSOLE}traffic`);
  await expect(empty).toBeVisible();
  await kill();
  await expect(page.locator('html')).toHaveAttribute('data-connection-lost', '', { timeout: 15_000 });
  await expect(empty.locator('.up-only')).toBeHidden();
  await expect(empty.locator('.lost-only')).toContainText('cannot reach the broker');
  await expect(page.locator('#keys .keys-lost').first()).toBeVisible();
  await up();
  await page.keyboard.press('r');
  await expect(page.locator('html')).not.toHaveAttribute('data-connection-lost', '', { timeout: 20_000 });
  await expect(empty.locator('.up-only')).toBeVisible();
});

// The Clients view is a table of its own and says the same as the others when the broker
// is lost: the frozen line under the header, then the rows (Traf-10).
test('Traf-06 and Traf-10: the Clients view draws the frozen line under its header when the broker is lost', async ({ page, request, uniqueName }) => {
  await kill();
  await up();
  const topic = uniqueName('clients');
  expect((await request.post(`${CONSOLE}new/topic`, { form: { name: topic, partitions: '1' } })).ok()).toBe(true);
  const holdBin = join(ROOT, 'e2e', '.tmp', 'bin', `hold-disc-${PORT}`);
  execFileSync('go', ['build', '-o', holdBin, './e2e/hold'], { cwd: ROOT, env: { ...process.env, GOWORK: 'off' } });
  const hold = spawn(holdBin, [`127.0.0.1:${PORT}`, 'disc-hold', `${topic}-group`, topic],
    { cwd: ROOT, stdio: ['ignore', 'pipe', 'inherit'], env: { ...process.env, E2E_SASL_USER: '', E2E_TLS: '' } });
  try {
    await new Promise<void>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('the held consumer never joined its group')), 20_000);
      hold.stdout!.on('data', (b) => { if (String(b).includes('ready')) { clearTimeout(t); resolve(); } });
      hold.on('exit', () => reject(new Error('the held consumer exited')));
    });
    await page.goto(`${CONSOLE}traffic?view=clients`);
    const table = page.locator('#clients-table');
    await expect(table.locator('.tf-rows > .r[data-row]', { hasText: 'disc-hold' })).toBeVisible();
    await expect(table.locator('.tf-frozen')).toBeHidden();
    await kill();
    await expect(page.locator('html')).toHaveAttribute('data-connection-lost', '', { timeout: 15_000 });
    const line = table.locator('.tf-frozen');
    await expect(line).toBeVisible();
    await expect(line).toContainText('Nothing newer than this line is known.');
    const below = await table.evaluate((t) => {
      const head = t.querySelector('.r.h > span')!.getBoundingClientRect();
      const frozen = t.querySelector('.tf-frozen')!.getBoundingClientRect();
      const first = t.querySelector('.tf-rows > .r[data-row]')!.getBoundingClientRect();
      return { headBottom: head.bottom, frozenTop: frozen.top, frozenBottom: frozen.bottom, firstTop: first.top };
    });
    expect(below.frozenTop).toBeGreaterThanOrEqual(below.headBottom - 1);
    expect(below.firstTop).toBeGreaterThanOrEqual(below.frozenBottom - 1);
    await expect(line).toHaveCSS('opacity', '1');
    await expect(table.locator('.tf-rows')).toHaveCSS('opacity', '0.55');
  } finally {
    hold.kill('SIGTERM');
    await up();
  }
});
