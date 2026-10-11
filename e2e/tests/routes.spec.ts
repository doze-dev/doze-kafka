import { test, expect } from '../fixtures/console';

// Routes only a person reaches by waiting or by a link inside a panel. The route gate
// (route-gate.ts) holds the console's route table to what the suite requests; these
// are the two it found nobody driving.

test('a group page refreshes its live region on its own', async ({ page }) => {
  const poll = page.waitForResponse((r) => /\/groups\/g-lag\/live/.test(r.url()), { timeout: 15_000 });
  await page.goto('groups/g-lag');
  await poll;
  await expect(page.locator('#group-live')).toBeVisible();
});

test("a record's value downloads from the drawer, byte for byte", async ({ page }) => {
  await page.goto('topics/seed-orders?partition=0&open=5');
  const link = page.locator('#drawer a[href*="/message/raw"]');
  await expect(link).toBeVisible();
  const [download] = await Promise.all([page.waitForEvent('download'), link.click()]);
  expect(download.suggestedFilename()).toBeTruthy();
});
