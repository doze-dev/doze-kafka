import { expect, type Page } from '@playwright/test';
import { goOutput } from './design/lib/goprog';

// The console's pages and states, for the sweeps that visit all of them (the
// accessibility sweep, the console-error sweep). PAGES is keyed by the route table's
// GET routes (console/routes.go); a route that is not a page is in PARTIALS with what
// it is. tests/a11y.spec.ts fails when the table and these disagree.

const TRACE = '0af7651916cd43dd8448eb211c80319c';

// Every page, by route; a route's pages are its states (tabs, views, an error).
export const PAGES: Record<string, string[]> = {
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
  '/access/check': ['access/check?principal=User:alice&operation=write&resource=topic&name=seed-orders'],
  '/new/acl': ['new/acl'],
};
// The routes that are not pages, each with what it is; each is drawn inside a page
// above, or a state below, and is swept there.
export const PARTIALS: Record<string, string> = {
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

export type Route = { method: string; path: string };
export const ROUTES: Route[] = JSON.parse(goOutput('./e2e/routes'));

// The states that are not pages, each opened from a page.
export const STATES: [string, string, (p: Page, width: number) => Promise<void>][] = [
  ['the palette', 'topics/seed-orders', async (p) => { await p.keyboard.press('ControlOrMeta+k'); await expect(p.locator('#palette')).toBeVisible(); await p.locator('#pal-q').fill('ord'); }],
  ['the shortcuts', 'traffic', async (p) => { await p.keyboard.press('?'); await expect(p.locator('#help')).toBeVisible(); }],
  ['the More menu', 'topics/seed-orders', async (p) => { await p.locator('main [data-more]').click(); await expect(p.locator('main .menu')).toBeVisible(); }],
  ['a confirm', 'topics/seed-orders', async (p) => { await p.locator('main [data-more]').click(); await p.locator('main .menu button', { hasText: 'Purge' }).click(); await expect(p.locator('#confirm')).toBeVisible(); }],
  ['the open composer', 'topics/seed-orders', async (p) => { await p.locator('#compose-head').click(); await expect(p.locator('#compose-body')).toBeVisible(); }],
  ['a request in the inspector', 'traffic', async (p) => { await p.locator('#traffic-table .tf-rows > .r[data-row], #traffic-rows tr[data-inspector-open], main tr[data-inspector-open]').first().click(); await expect(p.locator('#inspector-title')).toBeVisible(); }],
  ['the phone\'s menu', 'topics', async (p, width) => { if (width > 640) return; await p.locator('#menu-btn').click(); await expect(p.locator('nav.rail')).toBeVisible(); }],
];
