import { test, expect } from '../fixtures/console';
import type { Page } from '@playwright/test';

/** What sticks out past the viewport without a scroller of its own, and how far the document itself scrolls sideways. */
async function overflow(page: Page) {
  return page.evaluate(() => {
    const vw = window.innerWidth;
    const stray = Array.from(document.querySelectorAll('body *')).filter((e) => {
      const r = e.getBoundingClientRect();
      if (r.width === 0 || r.right <= vw + 1 || (e as HTMLElement).offsetParent === null) return false;
      for (let p = e.parentElement; p; p = p.parentElement) { const o = getComputedStyle(p).overflowX; if (o === 'auto' || o === 'scroll' || o === 'hidden') return false; }
      return true;
    }).slice(0, 3).map((e) => e.className || e.tagName);
    return { doc: document.documentElement.scrollWidth - vw, stray };
  });
}

// Each section's home page at the three widths the design draws: 1440, 960 and 390.
// (The two narrower ones are the ones that can break; 1440 is every other spec.)
const HOMES = ['topics', 'groups', 'schemas', 'traffic', 'cluster', 'access'];
for (const width of [960, 390]) {
  test(`every section's home page fits at ${width}px, in every theme`, async ({ page }) => {
    await page.setViewportSize({ width, height: 800 });
    for (const theme of ['light', 'dark', 'contrast']) {
      await page.addInitScript((t) => { try { localStorage.setItem('theme', t); } catch { /* private window */ } }, theme);
      for (const path of HOMES) {
        await page.goto(path);
        await page.waitForTimeout(150);
        await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
        expect(await overflow(page), `${path} at ${width}px in the ${theme} theme`).toEqual({ doc: 0, stray: [] });
      }
    }
  });
}

// Fitting is not enough: a home that fits by cutting its table shows nothing (the
// verification of 82b8752 found the name columns 0 px wide at 390, in a card that hid
// what it cut, with the list that holds the names hidden too). At 390 each home's rows
// are cards that show every value whole; at 960 its table does; nothing is cut by a box
// that hides it.
const HOME_TABLES: [string, string][] = [['topics', '#topics-table'], ['groups', '#groups-table'], ['schemas', '#schemas-table'], ['access', '#acl-table'], ['cluster', '#cluster-identity'], ['trace', '#trace-recent']];
for (const width of [960, 390]) {
  test(`at ${width}px every home shows its names and every value whole`, async ({ page }) => {
    await page.setViewportSize({ width, height: 844 });
    for (const [path, table] of HOME_TABLES) {
      await page.goto(path);
      const cut = await page.locator(table).evaluate((t, width) => {
        const out: string[] = [];
        const rows = Array.from(t.querySelectorAll(':scope > .r:not(.h), :scope > a.r'));
        if (!rows.length) out.push('no rows');
        for (const r of rows.slice(0, 6)) {
          const name = r.firstElementChild as HTMLElement;
          if (!name || name.getBoundingClientRect().width < 20) out.push(`a name is ${name?.getBoundingClientRect().width} px wide`);
          // At 390 no value is cut: each cell holds its whole text (no ellipsis, no hidden overflow).
          if (width === 390) for (const c of Array.from(r.children) as HTMLElement[]) {
            if (c.scrollWidth > c.clientWidth + 1) out.push(`"${c.textContent?.trim().slice(0, 30)}" is cut`);
          }
        }
        if (t.scrollWidth > t.clientWidth + 1) out.push(`the card is ${t.scrollWidth} px of content in ${t.clientWidth}`);
        return out;
      }, width);
      expect(cut, `${path} at ${width}px`).toEqual([]);
    }
  });
}

// Sy-04's top bar: the menu, the logo, the page, the jump key and the broker's dot; the
// theme switch (Sy-07) is at the foot of the menu, and the wire is the wide page's.
test('at 390 the top bar has the dot, the menu has the theme switch, and a topic draws no wire', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('topics/seed-orders');
  const dot = page.locator('#live-pill .dot');
  await expect(dot).toBeVisible();
  expect((await dot.boundingBox())!.width).toBe(7);
  // The address is not drawn, and is still read: visually hidden, not removed.
  await expect(page.locator('#live-pill .pill-addr')).toHaveText(/^127\.0\.0\.1:\d+$/);
  await expect(page.locator('#live-pill .pill-addr')).not.toHaveCSS('display', 'none');
  await expect(page.locator('header .theme-seg')).toBeHidden();
  await expect(page.locator('#wire-dock')).toBeHidden();
  // Cards: each record bordered, with no line beyond its key and offset, its value and its age.
  const card = page.locator('#messages-table tbody tr:not(.txnmark)').first();
  const lines = await card.evaluate((tr) => Array.from(tr.children).filter((c) => (c as HTMLElement).offsetParent !== null).map((c) => (c as HTMLElement).className));
  expect(lines).toEqual(['cell-offset', 'cell-key', 'cell-val', 'cell-age num mut'].map((c) => expect.stringContaining(c.split(' ')[0])));
  await expect(card).toHaveCSS('border-top-width', '1px');
  await page.locator('#menu-btn').click();
  const sw = page.locator('nav.rail .rail-theme');
  await expect(sw).toBeVisible();
  await sw.locator('[data-mode="dark"]').click();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
  await sw.locator('[data-mode="light"]').click();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');
});

// Below ~640px the list and the detail become two screens; nothing may need a
// sideways scroll at 420 or 360px.
for (const width of [420, 360]) {
  test(`no page overflows sideways at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 760 });
    for (const path of ['topics', 'topics/seed-orders', 'topics/seed-orders?open=5', 'topics/seed-orders?q=ord&all=1', 'groups', 'groups/g-lag', 'cluster', 'new/topic']) {
      await page.goto(path);
      await page.waitForTimeout(250);
      const over = await page.evaluate(() => {
        const vw = window.innerWidth;
        const stray = Array.from(document.querySelectorAll('body *')).filter((e) => {
          const r = e.getBoundingClientRect();
          if (r.width === 0 || r.right <= vw + 1 || (e as HTMLElement).offsetParent === null) return false;
          for (let p = e.parentElement; p; p = p.parentElement) { const o = getComputedStyle(p).overflowX; if (o === 'auto' || o === 'scroll' || o === 'hidden') return false; }
          return true;
        }).slice(0, 3).map((e) => e.className || e.tagName);
        return { doc: document.documentElement.scrollWidth - vw, stray };
      });
      expect(over, `${path} at ${width}px`).toEqual({ doc: 0, stray: [] });
    }
  });
}

test('a topic opened on a phone is its own screen, with a way back to the list', async ({ page }) => {
  await page.setViewportSize({ width: 420, height: 800 });
  await page.goto('topics');
  // The list is one tap away (Sy-03): the home page's table is the list here.
  await expect(page.locator('.list')).toBeHidden();
  await page.locator('#topics-table a.r', { hasText: 'seed-orders' }).first().click();
  await expect(page.locator('.idband h1')).toHaveText('seed-orders');
  await expect(page.locator('.list')).toBeHidden();
  await page.locator('.backrow a').click();
  await expect(page).toHaveURL(/\/topics$/);
  await expect(page.locator('#topics-table')).toBeVisible();
});

test('an open composer joins the page instead of covering a small screen', async ({ page }) => {
  await page.setViewportSize({ width: 360, height: 700 });
  await page.goto('topics/seed-orders');
  await page.locator('#compose-head').click();
  const dock = await page.locator('#compose-panel').evaluate((el) => getComputedStyle(el).position);
  expect(dock).toBe('static');
});
