import { test, expect } from '../fixtures/console';
import type { Page } from '@playwright/test';

// A10: every key in every page's strip does what the strip says, on that page. The
// strip is read from the page (it is keys.go's, drawn by the server), and each hint it
// shows is pressed and its effect asserted. A hint with a label this spec does not
// know fails: a new hint comes with the effect that proves it.

test.use({ permissions: ['clipboard-read', 'clipboard-write'] });

const PAGES = [
  '', 'topics', 'topics/seed-orders', 'topics/seed-orders?tab=partitions', 'topics/seed-orders?tab=config',
  'groups', 'groups/g-lag', 'groups/g-lag?tab=offsets', 'schemas', 'schemas/seed-events-value',
  'trace', 'traffic', 'traffic?view=clients', 'access', 'access/check?principal=User:ANONYMOUS&operation=read&resource=topic&name=seed-orders',
  'new/acl', 'cluster', 'connect', 'new/topic', 'new/schema',
];

type Hint = { keys: string[]; label: string };

async function hints(page: Page): Promise<Hint[]> {
  return page.evaluate(() => Array.from(document.querySelectorAll<HTMLElement>('#keys > span')).filter((s) => s.offsetParent !== null).map((s) => ({
    keys: Array.from(s.querySelectorAll('.kbd')).map((k) => (k.textContent || '').trim()),
    label: Array.from(s.childNodes).filter((n) => n.nodeType === Node.TEXT_NODE).map((n) => n.textContent).join('').trim(),
  })));
}

/** A fresh copy of the page, nothing focused, nothing open. */
async function fresh(page: Page, path: string) {
  await page.goto(path);
  await page.evaluate(() => document.fonts.ready);
  await page.evaluate(() => (document.activeElement as HTMLElement | null)?.blur());
}

async function press(page: Page, keys: string[]) {
  for (const k of keys) {
    const key = k === '↵' ? 'Enter' : k === 'esc' ? 'Escape' : k === '⌘K' ? 'ControlOrMeta+k' : k === '⌘↵' ? 'ControlOrMeta+Enter' : k === 'tab' ? 'Tab' : k;
    await page.keyboard.press(key);
  }
}

const SECTION: Record<string, RegExp> = { topics: /\/topics$/, groups: /\/groups$/, schemas: /\/schemas$/, cluster: /\/cluster$/, traffic: /\/traffic$/, access: /\/access$/ };

/** Opens something with j then Enter: a page, or the inspector. */
async function opens(page: Page, path: string) {
  const before = page.url();
  await press(page, ['j']);
  await expect(page.locator('.cursor'), `${path}: j puts the cursor on a row`).toHaveCount(1);
  await press(page, ['↵']);
  await expect.poll(async () => page.url() !== before || await page.locator('#inspector:not([hidden])').count() > 0,
    { message: `${path}: Enter opens the row` }).toBe(true);
}

async function effect(page: Page, path: string, h: Hint) {
  const where = `${path}: ${h.keys.join(' ')} ${h.label}`;
  switch (h.label) {
    case 'move': {
      await press(page, ['j']);
      const first = await page.locator('.cursor').evaluate((e) => e.outerHTML.slice(0, 200));
      const rows = await page.evaluate(() => document.querySelectorAll('.cursor').length);
      expect(rows, where).toBe(1);
      await press(page, ['j']);
      const second = await page.locator('.cursor').evaluate((e) => e.outerHTML.slice(0, 200));
      await press(page, ['k']);
      const back = await page.locator('.cursor').evaluate((e) => e.outerHTML.slice(0, 200));
      // With one row, j stays on it; with more, j moves on and k comes back.
      if (second !== first) expect(back, where).toBe(first);
      return;
    }
    case 'open': case 'open subject': case 'open client': case 'follow':
      return opens(page, where);
    case 'close panel': case 'close':
      await opens(page, where);
      await press(page, ['esc']);
      await expect(page.locator('#inspector'), where).toBeHidden();
      return;
    case 'filter':
      await press(page, ['/']);
      await expect(page.locator('input[data-filter-target]:focus'), where).toHaveCount(1);
      return;
    case 'trace id':
      await press(page, ['/']);
      await expect(page.locator('#tid'), where).toBeFocused();
      return;
    case 'jump or act':
      await press(page, ['⌘K']);
      await expect(page.locator('#palette'), where).toBeVisible();
      return;
    case 'all keys':
      await press(page, ['?']);
      await expect(page.locator('#help'), where).toBeVisible();
      return;
    case 'create topic': case 'create a topic':
      await press(page, ['c']);
      await expect(page, where).toHaveURL(/\/new\/topic$/);
      return;
    case 'register schema':
      await press(page, ['c']);
      await expect(page, where).toHaveURL(/\/new\/schema$/);
      return;
    case 'create ACL':
      await press(page, ['c']);
      await expect(page, where).toHaveURL(/\/new\/acl$/);
      return;
    case 'connect an app':
      await press(page, ['c']);
      await expect(page, where).toHaveURL(/\/connect$/);
      return;
    case 'copy bootstrap': {
      await press(page, ['c']);
      await expect(page.locator('.toast'), where).toContainText('Copied');
      const copied = await page.evaluate(() => navigator.clipboard.readText());
      expect(copied, where).toMatch(/^[\w.\-]+:\d+$/); // the bootstrap address, host:port.
      return;
    }
    case 'check':
      await press(page, ['↵']);
      await expect(page, where).toHaveURL(/\/access\/check\?/);
      await expect(page.locator('#check-answer'), where).toBeVisible();
      return;
    case 'back':
      await press(page, ['esc']);
      await expect(page, where).toHaveURL(/\/access$/);
      return;
    case 'next field':
      // From a field of the form, Tab is the next field of the form.
      await page.locator('main form input, main form select').first().focus();
      await press(page, ['tab']);
      await expect.poll(() => page.evaluate(() => { const a = document.activeElement as HTMLElement | null; return !!a && /^(INPUT|SELECT)$/.test(a.tagName) && !!a.closest('main form') && a !== document.querySelector('main form input, main form select'); }), { message: where }).toBe(true);
      return;
    case 'create':
      // Nothing is filled in: sending asks the browser to check the form, which puts
      // the focus on the first field it lacks (the principal). It sends nothing.
      await press(page, ['ControlOrMeta+Enter']);
      await expect(page.locator('#ap'), where).toBeFocused();
      await expect(page, where).toHaveURL(/\/new\/acl$/);
      return;
    case 'produce':
      await press(page, ['p']);
      await expect(page.locator('html'), where).toHaveAttribute('data-dock', 'open');
      return;
    case 'pause': {
      const toggle = page.locator('[data-live-toggle]').first();
      await press(page, ['p']);
      await expect(toggle, where).toHaveAttribute('aria-pressed', 'true');
      await press(page, ['p']);
      await expect(toggle, where).toHaveAttribute('aria-pressed', 'false');
      return;
    }
    case 'errors only':
      await press(page, ['e']);
      await expect(page, where).toHaveURL(/kind=errors/);
      return;
    case 'topics': case 'groups': case 'schemas': case 'cluster': case 'traffic': case 'access':
      await press(page, h.keys);
      await expect(page, where).toHaveURL(SECTION[h.label]);
      return;
    case 'cancel':
      if (await page.locator('main [data-esc]').count()) {
        await press(page, ['esc']);
        await expect(page, where).toHaveURL(/\/access$/);
      }
      return; // a confirm's own cancel: confirms.spec and keys.spec press it.
    case 'move between buttons': case 'press the focused button':
      return; // a confirm's own keys: confirms.spec and keys.spec press them.
  }
  throw new Error(`${where}: this spec does not know what "${h.label}" should do; add its effect`);
}

// Sy-03's strip at 1100 px and below.
test('every key in the strip of a topic at 960 does what it says', async ({ page }) => {
  await page.setViewportSize({ width: 960, height: 700 });
  const path = 'topics/seed-orders';
  await fresh(page, path);
  const shown = await hints(page);
  expect(shown.map((h) => h.label)).toEqual(['move', 'open', 'close', 'jump or act']);
  for (const h of shown) {
    await fresh(page, path);
    await effect(page, path, h);
  }
});

for (const path of PAGES) {
  test(`every key in the strip of /${path} does what it says`, async ({ page }) => {
    await fresh(page, path);
    const shown = await hints(page);
    expect(shown.length, `${path} has a strip`).toBeGreaterThan(0);
    for (const h of shown) {
      await fresh(page, path);
      await effect(page, path, h);
    }
  });
}
