// Screenshots of the console at the widths and themes the design draws, for the review
// that sits them beside the boards. Not part of the suite: run it by hand.
//
//   node shots.mjs <console-base-url> <out-dir> [pageName...]
//
// Pages are named below; with none named, all are taken, at 1440 and 390, light and dark.
import { chromium } from '@playwright/test';
import { mkdirSync } from 'node:fs';

const [base, out, ...only] = process.argv.slice(2);
if (!base || !out) {
  console.error('usage: node shots.mjs <console-base-url> <out-dir> [page...]');
  process.exit(2);
}
mkdirSync(out, { recursive: true });

const PAGES = {
  traffic: 'traffic',
  'traffic-open': 'traffic?open=request:__first',
  topics: 'topics',
  'topic-messages': 'topics/seed-orders?partition=0',
  'topic-message': 'topics/seed-orders?partition=0&open=5',
  'topic-partitions': 'topics/seed-orders?tab=partitions',
  'topic-readers': 'topics/seed-orders?tab=readers',
  'topic-config': 'topics/seed-orders?tab=config',
  groups: 'groups',
  group: 'groups/g-lag',
  'group-members': 'groups/g-lag?tab=members',
  schemas: 'schemas',
  schema: 'schemas/seed-events-value',
  cluster: 'cluster',
  access: 'access',
  trace: 'trace',
  connect: 'connect',
  'not-found': 'topics/seed-ordres',
};
const SIZES = { '1440': [1440, 900], '960': [960, 700], '390': [390, 844] };

const abs = (p) => new URL(p, base.endsWith('/') ? base : base + '/').toString();
const browser = await chromium.launch();
const names = only.length ? only : Object.keys(PAGES);
for (const theme of ['light', 'dark']) {
  for (const [label, [w, h]] of Object.entries(SIZES)) {
    if (label === '960' && theme === 'dark') continue;
    const ctx = await browser.newContext({ viewport: { width: w, height: h }, colorScheme: theme });
    const page = await ctx.newPage();
    for (const name of names) {
      let path = PAGES[name];
      if (!path) continue;
      if (path.includes('__first')) {
        await page.goto(abs('traffic'));
        const key = await page.evaluate(() => (document.querySelector('#traffic-table tbody tr') || {}).id?.replace(/^rq-/, ''));
        if (!key) continue;
        path = `traffic?open=request:${key}`;
      }
      await page.goto(abs(path));
      await page.waitForTimeout(600);
      await page.screenshot({ path: `${out}/${name}-${label}-${theme}.png` });
    }
    await ctx.close();
  }
}
await browser.close();
