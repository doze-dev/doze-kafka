import { createRequire } from 'node:module';
import { test, expect } from '../fixtures/console';
import type { Page } from '@playwright/test';

// The accessibility sweep: axe-core, through Playwright, over every page of the
// console in light and dark (and the high-contrast theme), and over the states
// that are not pages (the inspector open on each kind, the palette, the help
// overlay, a confirm). Zero serious or critical violations. Ported from
// doze-aws/e2e/audit/crawl.ts, which crawls by links: so does this, from the home
// page, so a page that is linked and not listed below is still visited.

const AXE = createRequire(import.meta.url).resolve('axe-core/axe.min.js');
const TRACE = '0af7651916cd43dd8448eb211c80319c';

// Pages that no link reaches (forms, a trace, an inspector's address), and the
// ones whose shape matters. Everything else is found by following links.
const SEEDS = [
  '', 'topics', 'topics/seed-orders', 'topics/seed-orders?partition=1', 'topics/seed-orders?q=ord&all=1', 'topics/seed-empty', 'topics/seed-txn',
  'new/topic', 'groups', 'groups/g-lag', 'groups/g-timeline', 'cluster', 'access', 'traffic', 'traffic?kind=errors',
  'schemas', 'schemas/seed-events-value', 'new/schema',
  `trace?id=${TRACE}`, 'trace', 'connect',
  // The tabs and views under a page.
  'topics/seed-orders?tab=partitions', 'topics/seed-orders?tab=readers', 'topics/seed-orders?tab=config', 'topics/seed-events?tab=schema',
  'groups/g-lag?tab=members', 'groups/g-lag?tab=timeline', 'groups/g-reset-latest?tab=offsets',
  'cluster?view=apis', 'cluster?view=config', 'cluster?view=transactions',
  'schemas/seed-events-value?tab=compat', 'schemas/seed-events-value?tab=register',
  'access?principal=User:ANONYMOUS',
  // The inspector open on each kind (the address carries it).
  'topics/seed-orders?open=5', 'traffic?open=request:no-such-row', 'cluster?open=transaction:seed-open', 'topics/seed-events?open=subject:seed-events-value',
  // Pages that are errors still have to be accessible.
  'topics/no-such-topic', 'groups/no-such-group', 'schemas/no-such-subject',
];

function shape(path: string): string {
  const u = new URL(path, 'http://x/');
  const segs = u.pathname.split('/').filter(Boolean).map((s, i) => (i === 0 ? s : '*'));
  const q = [...u.searchParams.keys()].filter((k) => ['open', 'errors', 'all', 'q', 'partition', 'tab', 'view', 'kind'].includes(k)).sort();
  return segs.join('/') + (q.length ? '?' + q.join('&') : '');
}

async function axe(page: Page): Promise<string[]> {
  await page.addScriptTag({ path: AXE });
  const res = await page.evaluate(async () => (window as any).axe.run(document, { resultTypes: ['violations'] }));
  return (res.violations as any[])
    .filter((v) => v.impact === 'serious' || v.impact === 'critical')
    .map((v) => `${v.impact} ${v.id}: ${v.help} (${v.nodes.length}) e.g. ${v.nodes.slice(0, 4).map((n: any) => n.target.join(' ')).join(' | ')}`);
}

async function settle(page: Page) {
  await page.waitForLoadState('load');
  await page.waitForTimeout(200);
}

for (const theme of ['light', 'dark', 'contrast']) {
  test(`no serious or critical accessibility violation on any page, ${theme} theme`, async ({ browser, baseURL }) => {
    test.setTimeout(240_000);
    const ctx = await browser.newContext({ baseURL, viewport: { width: 1440, height: 900 } });
    await ctx.addInitScript((t) => { try { localStorage.setItem('theme', t); } catch { /* private window */ } }, theme);
    const page = await ctx.newPage();
    const problems: string[] = [];
    const seen = new Set<string>();
    const perShape = new Map<string, number>();
    const queue = [...SEEDS];
    let visited = 0;
    while (queue.length && visited < 120) {
      const path = queue.shift()!;
      if (seen.has(path)) continue;
      seen.add(path);
      const sh = shape(path);
      if ((perShape.get(sh) ?? 0) >= 2) continue;
      perShape.set(sh, (perShape.get(sh) ?? 0) + 1);
      await page.goto(path);
      await settle(page);
      await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
      visited++;
      for (const v of await axe(page)) problems.push(`${path || '(home)'} [${theme}]: ${v}`);
      // Follow the console's own links (not static files, downloads or inspector fetches).
      const base = new URL(baseURL!);
      for (const href of await page.$$eval('a[href]', (as) => as.map((a) => (a as HTMLAnchorElement).href))) {
        const u = new URL(href);
        if (u.origin !== base.origin || !u.pathname.startsWith(base.pathname) || /\/static\/|\/message\/raw|download/.test(u.pathname)) continue;
        queue.push(u.pathname.slice(base.pathname.length) + u.search);
      }
    }
    expect(visited, 'the crawl reached the console').toBeGreaterThan(SEEDS.length - 8);
    expect(problems, problems.join('\n')).toEqual([]);
    await ctx.close();
  });
}

test('the palette, the help overlay and a confirm are accessible too', async ({ page, openPalette }) => {
  const problems: string[] = [];
  await page.goto('topics/seed-orders');
  await openPalette();
  await page.waitForTimeout(400); // the overlay fades in; axe must read its settled colours.
  problems.push(...(await axe(page)).map((v) => `palette: ${v}`));
  await page.keyboard.press('Escape');
  await page.keyboard.press('?');
  await expect(page.locator('#help')).toBeVisible();
  await page.waitForTimeout(400);
  problems.push(...(await axe(page)).map((v) => `help: ${v}`));
  await page.keyboard.press('Escape');
  await page.getByRole('button', { name: 'More actions' }).click();
  await page.getByRole('button', { name: 'Purge' }).first().click();
  await expect(page.locator('#confirm')).toBeVisible();
  await page.waitForTimeout(400);
  problems.push(...(await axe(page)).map((v) => `confirm: ${v}`));
  await page.locator('#confirm-no').click();
  expect(problems, problems.join('\n')).toEqual([]);
});
