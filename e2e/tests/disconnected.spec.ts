import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { mkdirSync, rmSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { test, expect } from '../fixtures/console';
import { PREFIX } from '../playwright.config';

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
  await expect(pill.locator('.pill-state')).toContainText('Not connected');
  await expect(pill.locator('.pill-state')).toContainText(/last heard \d{1,2}:\d{2}/);
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
  await expect(page.locator('#traffic-table tbody tr').first()).toBeVisible();
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
  await expect(page.locator('.frozen-line')).toContainText('Nothing newer than this line is known.');
  await expect(page.locator('#traffic-live .tbl-wrap')).toHaveCSS('opacity', '0.55');
  await expect(page.locator('#traffic-table tbody tr').first()).toBeVisible(); // still there.

  await up();
  await header.locator('button', { hasText: 'Retry now' }).click();
  await expect(page.locator('html')).not.toHaveAttribute('data-connection-lost', '', { timeout: 20_000 });
  await expect(callout).toBeHidden();
  await expect(header.locator('[data-live-state]')).toBeVisible();
});
