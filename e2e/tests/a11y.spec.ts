import { createRequire } from 'node:module';
import { test, expect } from '../fixtures/console';
import type { Browser, Page } from '@playwright/test';
import { PAGES, PARTIALS, ROUTES, STATES } from '../pages';

// The accessibility sweep (A12): axe-core over every page of the console's route table
// (console/routes.go, read from the binary's own source as the route gate reads it),
// at 1440, 960 and 390, in light, dark and high contrast, and over the states that are
// not pages (the inspector on each kind, the palette, the shortcuts, a confirm, the
// More menu, the open composer, the phone's menu). No critical, serious or moderate
// violation. A GET route the table gains fails the sweep until it is either a page
// visited below or named a partial with what it is.

const AXE = createRequire(import.meta.url).resolve('axe-core/axe.min.js');

test('every GET route is a page the sweep visits or a partial it names', () => {
  const missing = ROUTES.filter((r) => r.method === 'GET' && !(r.path in PAGES) && !(r.path in PARTIALS)).map((r) => r.path);
  expect(missing, 'routes the sweep does not know').toEqual([]);
  const gone = [...Object.keys(PAGES), ...Object.keys(PARTIALS)].filter((p) => !ROUTES.some((r) => r.method === 'GET' && r.path === p));
  expect(gone, 'routes the sweep names that the table no longer has').toEqual([]);
});

async function axe(page: Page): Promise<string[]> {
  if (!(await page.evaluate(() => 'axe' in window))) await page.addScriptTag({ path: AXE });
  const res = await page.evaluate(async () => (window as any).axe.run(document, { resultTypes: ['violations'] }));
  return (res.violations as any[])
    .filter((v) => ['moderate', 'serious', 'critical'].includes(v.impact))
    .map((v) => `${v.impact} ${v.id}: ${v.help} (${v.nodes.length}) e.g. ${v.nodes.slice(0, 4).map((n: any) => n.target.join(' ')).join(' | ')}`);
}

async function settle(page: Page) {
  await page.waitForLoadState('load');
  await page.mouse.move(0, 0);
  await page.waitForTimeout(250); // fades and pop-ins: axe reads settled colours.
}

async function open(browser: Browser, baseURL: string, width: number, theme: string) {
  const height = { 1440: 900, 960: 700, 390: 844 }[width]!;
  const ctx = await browser.newContext({ baseURL, viewport: { width, height } });
  await ctx.addInitScript((t) => { try { localStorage.setItem('theme', t); } catch { /* private window */ } }, theme);
  return ctx.newPage();
}

for (const width of [1440, 960, 390]) {
  for (const theme of ['light', 'dark', 'contrast']) {
    test(`no moderate, serious or critical violation on any page or state, ${width} ${theme}`, async ({ browser, baseURL }) => {
      test.setTimeout(300_000);
      const page = await open(browser, baseURL!, width, theme);
      const problems: string[] = [];
      for (const path of Object.values(PAGES).flat()) {
        await page.goto(path);
        await settle(page);
        await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
        for (const v of await axe(page)) problems.push(`${path || '(home)'}: ${v}`);
      }
      for (const [name, path, go] of STATES) {
        await page.goto(path);
        await settle(page);
        await go(page, width);
        await settle(page);
        for (const v of await axe(page)) problems.push(`${name} (${path}): ${v}`);
      }
      await page.context().close();
      expect(problems, problems.join('\n')).toEqual([]);
    });
  }
}
