import { test, expect } from '../fixtures/console';

// ACLs are enforced; the page that lists them says so before anything else, and how a
// resource with no ACL is treated.
test('the Access page says the ACLs are enforced, and is in the rail and the palette', async ({ page, openPalette }) => {
  await page.goto('');
  await page.locator('.rail a.nav', { hasText: 'Access' }).click();
  await expect(page).toHaveURL(/\/access$/);
  await expect(page.locator('.rail a.nav.on')).toHaveText(/Access/);
  await expect(page.locator('main .callout').first()).toContainText('These are enforced');
  await expect(page.locator('main .callout').first()).toContainText('open to everyone');
  // The harbour world stores ACLs (seed/harbour.go); their count is asserted in harbour.spec.ts.
  await expect(page.locator('#acl-table .r:not(.h)').first()).toBeVisible();

  await page.goto('topics');
  await openPalette();
  await page.keyboard.type('Access');
  await expect(page.locator('.pal-item', { hasText: 'Access' })).toBeVisible();
});
