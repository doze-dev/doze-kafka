import { test, expect } from '../fixtures/console';

// ACLs are enforced; the page that lists them says so before anything else, and how a
// resource with no ACL is treated.
test('the Access page says the ACLs are enforced, and is in the rail and behind g a, not in the palette', async ({ page, openPalette }) => {
  await page.goto('');
  await page.locator('.rail a.nav', { hasText: 'Access' }).click();
  await expect(page).toHaveURL(/\/access$/);
  await expect(page.locator('.rail a.nav.on')).toHaveText(/Access/);
  await expect(page.locator('main .callout').first()).toContainText('These are enforced');
  await expect(page.locator('main .callout').first()).toContainText('open to everyone');
  // The harbour world stores ACLs (seed/harbour.go); their count is asserted in harbour.spec.ts.
  await expect(page.locator('#acl-table .r:not(.h)').first()).toBeVisible();

  // Sy-05 has no Go to group: the section is the rail's and the chord's.
  await page.goto('topics');
  await openPalette();
  await page.keyboard.type('Access');
  await expect(page.locator('.pal-item', { hasText: /^Access$/ })).toHaveCount(0);
  await page.keyboard.press('Escape');
  await page.keyboard.press('g');
  await page.keyboard.press('a');
  await expect(page).toHaveURL(/\/access$/);
});
