import { test, expect } from '../fixtures/console';

// ACLs are enforced; the page that lists them says so before anything else, and how a
// resource with no ACL is treated.
test('the Access page says the ACLs are enforced, and is in the rail and the palette', async ({ page, openPalette }) => {
  await page.goto('');
  await page.locator('.rail .ri', { hasText: 'Access' }).click();
  await expect(page).toHaveURL(/\/access$/);
  await expect(page.locator('.rail .ri.on')).toHaveText(/Access/);
  await expect(page.locator('.honest')).toContainText('These are enforced');
  await expect(page.locator('.honest')).toContainText('open to everyone');
  await expect(page.locator('.cpanel')).toContainText('None stored');

  await page.goto('topics');
  await openPalette();
  await page.keyboard.type('Access');
  await expect(page.locator('.pal-item', { hasText: 'Access' })).toBeVisible();
});
