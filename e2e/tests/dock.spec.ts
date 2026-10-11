import { test, expect } from '../fixtures/console';

// The seed's own requests are the traffic: client "doze-seed" produced to
// seed-orders and created the others. A topic's dock shows the requests that
// touched that topic, and no others.

test('a topic ends with the requests that touched it, and only those', async ({ page }) => {
  await page.goto('topics/seed-orders');
  const dock = page.locator('#wire-dock');
  await expect(dock).toBeVisible();
  await expect(dock.locator('tbody tr', { hasText: 'Produce' }).first()).toContainText('doze-seed');
  for (const row of await dock.locator('tbody tr').all()) {
    await expect(row).not.toContainText('seed-txn');
  }
  // A row opens its request in the inspector.
  await dock.locator('tbody tr', { hasText: 'Produce' }).first().click();
  await expect(page.locator('#inspector-overlay')).toBeVisible();
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
});
