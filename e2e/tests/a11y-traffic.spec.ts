import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { createRequire } from 'node:module';
import { test, expect } from '../fixtures/console';
import type { Page } from '@playwright/test';
import { kafkaAddr } from '../addr';
import { BASE_URL } from '../playwright.config';

// The held consumers read a topic of their own: the seed's topics are other specs' (a topic's
// dock lists the requests that touched it, and the first of them is the seed's).
const HOLD_TOPIC = 'a11y-hold-topic';


// Every state of the Traffic pages under axe-core, at 1440, 960 and 390, in the light,
// dark and high-contrast themes: no violation of any impact but "minor" (serious,
// critical and moderate all fail). The states are the boards': the requests, the
// errors, a filter, the clients, a group's story (classic and KIP-848), and the
// inspector on a request, a folded row, a client that reports metrics and one that does
// not, and the page paused. (The empty broker and the lost one are in disconnected.spec.ts,
// which stops the broker.)

const AXE = createRequire(import.meta.url).resolve('axe-core/axe.min.js');
const holds: ChildProcess[] = [];
const GROUP = 'a11y-hold-group';
const KGROUP = 'a11y-848-group';

test.describe.configure({ mode: 'serial' });

async function holdOne(client: string, group: string, env: Record<string, string>) {
  const p = spawn(`e2e/.tmp/bin/hold-a11y-${process.pid}`, [kafkaAddr(), client, group, HOLD_TOPIC],
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
  execFileSync('go', ['build', '-o', `e2e/.tmp/bin/hold-a11y-${process.pid}`, './e2e/hold'], { cwd: '..', env: { ...process.env, GOWORK: 'off' } });
  await holdOne('a11y-hold', GROUP, { HOLD_TELEMETRY: '1' });
  await holdOne('a11y-quiet', 'a11y-quiet-group', { HOLD_NO_METRICS: '1' });
  await holdOne('a11y-848-a', KGROUP, { HOLD_KIP848: '1' });
  await holdOne('a11y-848-b', KGROUP, { HOLD_KIP848: '1' });
});
test.afterAll(() => { holds.forEach((p) => p.kill('SIGTERM')); });

async function violations(page: Page): Promise<string[]> {
  await page.addScriptTag({ path: AXE });
  const res = await page.evaluate(async () => (window as any).axe.run(document, { resultTypes: ['violations'] }));
  return (res.violations as any[])
    .filter((v) => v.impact !== 'minor')
    .map((v) => `${v.impact} ${v.id}: ${v.help} (${v.nodes.length}) e.g. ${v.nodes.slice(0, 4).map((n: any) => n.target.join(' ')).join(' | ')}`);
}

type State = { name: string; path: string; act?: (page: Page) => Promise<void> };
const rows = '#traffic-table .tf-rows > .r[data-row]';
const states: State[] = [
  { name: 'requests', path: 'traffic' },
  { name: 'errors', path: 'traffic?kind=errors' },
  { name: 'a topic filter', path: 'traffic?topic=seed-orders' },
  { name: 'a client filter', path: 'traffic?client=a11y-hold' },
  { name: 'nothing matches', path: 'traffic?q=no-such-thing-at-all' },
  { name: 'clients', path: 'traffic?view=clients' },
  { name: 'a classic group', path: `traffic?group=${GROUP}&kind=groups` },
  { name: 'a KIP-848 group', path: `traffic?group=${KGROUP}` },
  { name: 'a request open', path: 'traffic', act: async (p) => { await p.locator(rows).first().click(); await expect(p.locator('#inspector [data-inspector-kind=request]')).toBeVisible(); } },
  { name: 'a refusal open', path: 'traffic?kind=errors&q=doze-seed', act: async (p) => { await p.locator(rows).first().click(); await expect(p.locator('#inspector [data-inspector-kind=request]')).toBeVisible(); } },
  { name: 'a folded row open', path: 'traffic', act: async (p) => { await p.locator(rows, { has: p.locator('.tag') }).first().click(); await expect(p.locator('#fold-sentence')).toBeVisible(); } },
  { name: 'a client that reports open', path: 'traffic?view=clients', act: async (p) => { await p.locator('#clients-table .tf-rows > .r[data-row]', { hasText: 'a11y-hold' }).click(); await expect(p.locator('#client-inspector')).toBeVisible(); } },
  { name: 'a client that does not report open', path: 'traffic?view=clients', act: async (p) => { await p.locator('#clients-table .tf-rows > .r[data-row]', { hasText: 'a11y-quiet' }).click(); await expect(p.locator('#no-metrics')).toBeVisible(); } },
  { name: 'paused', path: 'traffic', act: async (p) => { await p.keyboard.press('p'); await expect(p.locator('#paused-banner')).toBeVisible(); } },
];

for (const theme of ['light', 'dark', 'contrast']) {
  for (const width of [1440, 960, 390]) {
    test(`Traffic, every state, ${theme} at ${width}: no accessibility violation`, async ({ browser, baseURL }) => {
      test.setTimeout(240_000);
      const ctx = await browser.newContext({ baseURL, viewport: { width, height: 900 } });
      await ctx.addInitScript((t) => { try { localStorage.setItem('theme', t); } catch { /* none */ } }, theme);
      const page = await ctx.newPage();
      const problems: string[] = [];
      for (const s of states) {
        await page.goto(s.path);
        await page.waitForLoadState('load');
        await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
        if (s.act) await s.act(page);
        await page.waitForTimeout(300);
        for (const v of await violations(page)) problems.push(`${s.name}: ${v}`);
      }
      expect(problems, problems.join('\n')).toEqual([]);
      await ctx.close();
    });
  }
}
