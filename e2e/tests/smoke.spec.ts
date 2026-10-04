import { test, expect } from '../fixtures/console';

// Validates the harness itself: the binary builds, boots, is seeded, and the
// console shell renders.
//
// NOTE: baseURL is http://host:port/_console/. Playwright resolves a leading-slash
// goto() against the origin, not baseURL's path, so always goto() a path relative to
// baseURL with no leading slash (goto('') for home, goto('topics/x') for a topic).
test('the console shell renders and the broker is seeded', async ({ page, request }) => {
  await page.goto('');
  await expect(page).toHaveTitle(/doze-kafka/);
  await expect(page.locator('.rail')).toBeVisible();
  await expect(page.locator('.rail a.on')).toHaveText(/Topics/);
  await expect(page.locator('.listpane .li[data-name="seed-orders"]')).toBeVisible();
  const health = await request.get('../healthz');
  expect(health.status()).toBe(200);
});

test('the cluster page lists the broker, its APIs and its settings', async ({ page }) => {
  await page.goto('cluster');
  await expect(page.locator('h1')).toHaveText('Cluster');
  await expect(page.locator('.apilist')).toContainText('Produce');
  await expect(page.locator('.apilist')).toContainText('IncrementalAlterConfigs');
  await expect(page.getByText('log.retention.ms')).toBeVisible();
  await expect(page.getByText('no authentication')).toBeVisible();
});
