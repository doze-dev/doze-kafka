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
