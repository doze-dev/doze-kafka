import { test, expect } from '../fixtures/console';

// ACLs are stored and never enforced; the page that lists them says so before anything
// else, so a stored entry is not mistaken for security.
test('the Access page says the ACLs are not enforced, and is in the rail and the palette', async ({ page, openPalette }) => {
  await page.goto('');
  await page.locator('.rail .ri', { hasText: 'Access' }).click();
  await expect(page).toHaveURL(/\/access$/);
  await expect(page.locator('.rail .ri.on')).toHaveText(/Access/);
  await expect(page.locator('.honest.warn')).toContainText('These are not enforced');
  await expect(page.locator('.honest.warn')).toContainText('not security');
  await expect(page.locator('.cpanel')).toContainText('None stored');

  await page.goto('topics');
  await openPalette();
  await page.keyboard.type('Access');
  await expect(page.locator('.pal-item', { hasText: 'Access' })).toBeVisible();
});
