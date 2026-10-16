import { createRequire } from 'node:module';
import { test, expect } from '../fixtures/console';
import type { Browser, Page } from '@playwright/test';
import { goOutput } from '../design/lib/goprog';

// The accessibility sweep (A12): axe-core over every page of the console's route table
// (console/routes.go, read from the binary's own source as the route gate reads it),
// at 1440, 960 and 390, in light, dark and high contrast, and over the states that are
// not pages (the inspector on each kind, the palette, the shortcuts, a confirm, the
// More menu, the open composer, the phone's menu). No critical, serious or moderate
// violation. A GET route the table gains fails the sweep until it is either a page
// visited below or named a partial with what it is.

const AXE = createRequire(import.meta.url).resolve('axe-core/axe.min.js');
const TRACE = '0af7651916cd43dd8448eb211c80319c';

// Every page, by route; a route's pages are its states (tabs, views, an error).
const PAGES: Record<string, string[]> = {
  '/': [''],
  '': [],
  '/topics': ['topics'],
  '/new/topic': ['new/topic'],
  '/topics/{name}': ['topics/seed-orders', 'topics/seed-orders?partition=1', 'topics/seed-orders?q=ord&all=1', 'topics/seed-empty', 'topics/seed-txn',
    'topics/seed-orders?tab=partitions', 'topics/seed-orders?tab=readers', 'topics/seed-orders?tab=config', 'topics/seed-events?tab=schema',
    'topics/seed-orders?open=5', 'topics/seed-events?open=subject:seed-events-value', 'topics/no-such-topic'],
  '/trace': ['trace', `trace?id=${TRACE}`],
  '/traffic': ['traffic', 'traffic?kind=errors', 'traffic?view=clients', 'traffic?open=request:no-such-row'],
  '/schemas': ['schemas'],
  '/new/schema': ['new/schema'],
  '/schemas/{subject}': ['schemas/seed-events-value', 'schemas/seed-events-value?tab=compat', 'schemas/seed-events-value?tab=register', 'schemas/no-such-subject'],
  '/groups': ['groups'],
  '/groups/{id}': ['groups/g-lag', 'groups/g-lag?tab=members', 'groups/g-lag?tab=timeline', 'groups/g-reset-latest?tab=offsets', 'groups/g-timeline', 'groups/no-such-group'],
  '/connect': ['connect'],
  '/cluster': ['cluster', 'cluster?view=apis', 'cluster?view=config', 'cluster?view=transactions', 'cluster?open=transaction:seed-open'],
  '/access': ['access', 'access?principal=User:ANONYMOUS'],
  '/access/check': ['access/check?principal=User:alice&operation=WRITE&resource=TOPIC&name=seed-orders'],
  '/new/acl': ['new/acl'],
};
// The routes that are not pages, each with what it is; each is drawn inside a page
// above, or a state below, and is swept there.
const PARTIALS: Record<string, string> = {
  '/static/': 'the stylesheet, scripts and fonts',
  '/live/topics': 'Tp-Home\'s table, swept on topics',
  '/new/topic/shell': 'the create form\'s command, swept on new/topic',
  '/topics/{name}/tape': 'the tape and table, swept on a topic',
  '/topics/{name}/message': 'a record in the inspector, swept on topics/seed-orders?open=5',
  '/topics/{name}/message/raw': 'the stored bytes, a download',
  '/trace/live': 'the trace, swept on trace?id=',
  '/traffic/live': 'Traffic\'s rows, swept on traffic',
  '/traffic/request': 'a request in the inspector, swept in the states',
  '/traffic/new': 'a count, as JSON',
  '/live/groups': 'Gr-Home\'s table, swept on groups',
  '/groups/{id}/live': 'a group\'s tabs, swept on groups/g-lag',
  '/new/acl/preview': 'the create form\'s command, swept on new/acl',
  '/inspect/{kind}': 'the inspector\'s body, swept on every ?open= page',
  '/dock': 'the wire, swept on every topic and group',
  '/confirm/{id}': 'a confirm\'s words, swept in the states',
  '/api/palette': 'the palette\'s catalogue, as JSON (the palette is swept in the states)',
  '/api/counts': 'the rail\'s counts, as JSON',
  '/api/list/{section}': 'the list pane\'s numbers, as JSON',
};

type Route = { method: string; path: string };
const ROUTES: Route[] = JSON.parse(goOutput('./e2e/routes'));

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

// The states that are not pages, each opened from a page.
const STATES: [string, string, (p: Page, width: number) => Promise<void>][] = [
  ['the palette', 'topics/seed-orders', async (p) => { await p.keyboard.press('ControlOrMeta+k'); await expect(p.locator('#palette')).toBeVisible(); await p.locator('#pal-q').fill('ord'); }],
  ['the shortcuts', 'traffic', async (p) => { await p.keyboard.press('?'); await expect(p.locator('#help')).toBeVisible(); }],
  ['the More menu', 'topics/seed-orders', async (p) => { await p.locator('main [data-more]').click(); await expect(p.locator('main .menu')).toBeVisible(); }],
  ['a confirm', 'topics/seed-orders', async (p) => { await p.locator('main [data-more]').click(); await p.locator('main .menu button', { hasText: 'Purge' }).click(); await expect(p.locator('#confirm')).toBeVisible(); }],
  ['the open composer', 'topics/seed-orders', async (p) => { await p.locator('#compose-head').click(); await expect(p.locator('#compose-body')).toBeVisible(); }],
  ['a request in the inspector', 'traffic', async (p) => { await p.locator('#traffic-rows tr[data-inspector-open], main tr[data-inspector-open]').first().click(); await expect(p.locator('#inspector-title')).toBeVisible(); }],
  ['the phone\'s menu', 'topics', async (p, width) => { if (width > 640) return; await p.locator('#menu-btn').click(); await expect(p.locator('nav.rail')).toBeVisible(); }],
];

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
