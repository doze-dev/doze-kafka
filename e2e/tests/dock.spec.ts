import { test, expect } from '../fixtures/console';

// The seed's own requests are the traffic: client "doze-seed" produced to
// seed-orders and created the others. A topic's dock shows the requests that
// touched that topic, and no others.

test('a topic ends with the requests that touched it, and only those', async ({ page }) => {
  await page.goto('topics/seed-orders');
  const dock = page.locator('#wire-dock');
  await expect(dock).toBeVisible();
  await expect(dock.locator('.r').first()).toContainText('doze-seed');
  for (const row of await dock.locator('.r').all()) {
    await expect(row).not.toContainText('seed-txn');
  }
  // A row opens its request in the inspector.
  await dock.locator('.r').first().click();
  await expect(page.locator('#inspector')).toBeVisible();
  await expect(page.locator('#inspector .inspector-h')).toHaveAttribute('data-inspector-kind', 'request');
  await page.keyboard.press('Escape');
});

test('a group ends with its own requests, and has a pause control', async ({ page }) => {
  await page.goto('groups/g-timeline');
  const dock = page.locator('#wire-dock');
  await expect(dock).toBeVisible();
  await expect(dock.locator('[data-live-toggle]')).toBeVisible();
  await dock.locator('[data-live-toggle]').click();
  await expect(dock.locator('[data-live-toggle]')).toHaveAttribute('aria-pressed', 'true');
  // Its poll asks for the dock with the control, by the group's name, once.
  await expect(dock).toHaveAttribute('data-live', /\/dock\?group=g-timeline&t=1$/);
});

// The dock pages by its cursor: Older is the rows after the last one shown, and the
// page it lands on keeps polling that window (a refresh does not put the newest rows
// back); Newest returns to the head.
test('Older pages the dock in place, the older page survives its own refresh, and Newest goes back', async ({ page }) => {
  await page.goto('topics/seed-orders');
  const dock = page.locator('#wire-dock');
  const ids = () => dock.locator('.r').evaluateAll((rows) => rows.map((r) => r.id));
  await expect(dock.locator('.r')).toHaveCount(8);
  await expect(dock).toHaveAttribute('data-live', /\/dock\?topic=seed-orders$/);
  await expect(dock.locator('.dh .note')).toHaveText('requests that touched seed-orders');
  await expect(dock.locator('[data-dock-newest]')).toHaveCount(0);
  const first = await ids();

  await dock.locator('[data-dock-older]').click();
  await expect(dock).toHaveAttribute('data-live', /\/dock\?after=[^&]+&topic=seed-orders$/);
  await expect(dock.locator('.dh .note')).toHaveText('older requests that touched seed-orders');
  const second = await ids();
  expect(second.length).toBeGreaterThan(0);
  expect(second.filter((id) => first.includes(id))).toEqual([]);
  // The cursor is the last row of the page before.
  const live = await dock.getAttribute('data-live');
  expect(new URL(live!, 'http://x').searchParams.get('after')).toBe(first[first.length - 1].replace(/^wd-/, ''));
  // Two polls later (3 s each) it is still the older page.
  await page.waitForTimeout(7_000);
  expect(await ids()).toEqual(second);
  await expect(dock.locator('.dh .note')).toHaveText('older requests that touched seed-orders');
  // A row of the older page opens in the inspector like any other.
  await dock.locator('.r').first().click();
  await expect(page.locator('#inspector .inspector-h')).toHaveAttribute('data-inspector-kind', 'request');
  await page.keyboard.press('Escape');

  await dock.locator('[data-dock-newest]').click();
  await expect(dock).toHaveAttribute('data-live', /\/dock\?topic=seed-orders$/);
  await expect(dock.locator('[data-dock-newest]')).toHaveCount(0);
  await expect(dock.locator('.r')).toHaveCount(8);
  expect((await ids()).filter((id) => second.includes(id))).toEqual([]);
});

// The dock shows the latest eight; the rest are on the Traffic page, filtered to the
// same topic. The link used to carry "topic%3dseed-orders", which is no filter: it
// landed on every request the broker holds.
test("the dock's link to Traffic lands on that topic's requests only", async ({ page }) => {
  await page.goto('topics/seed-orders');
  const link = page.locator('#wire-dock .dock-more a');
  await expect(link).toHaveText('Traffic page');
  await expect(link).toHaveAttribute('href', /\/traffic\?topic=seed-orders$/);
  await link.click();
  await expect(page).toHaveURL(/\/traffic\?topic=seed-orders$/);
  // The page says it is filtered, and to what.
  const pill = page.locator('#filter-pills .pill').first();
  await expect(pill).toContainText('topic');
  await expect(pill).toContainText('seed-orders');
  // Every row is a request that named the topic. The unfiltered page holds requests
  // that did not (the seed made other topics with the same client).
  const rows = page.locator('#traffic-table tbody tr');
  await expect(rows.first()).toBeVisible();
  const targets = await rows.locator('.cell-topic').allTextContents();
  expect(targets.length).toBeGreaterThan(8);
  for (const t of targets) expect(t).toContain('seed-orders');
  await page.goto('traffic');
  const all = await page.locator('#traffic-table tbody tr .cell-topic').allTextContents();
  expect(all.some((t) => !t.includes('seed-orders'))).toBe(true);
});
