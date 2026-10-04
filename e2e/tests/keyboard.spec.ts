import { test, expect } from '../fixtures/console';

test('single keys act alone: with a modifier held, or typing in a field, they do nothing', async ({ page }) => {
  await page.goto('topics');
  for (const combo of ['Meta+c', 'Control+c', 'Alt+c', 'Meta+j', 'Control+m']) await page.keyboard.press(combo);
  await expect(page).toHaveURL(/\/topics$/);
  await page.locator('.listpane .filter input').focus();
  await page.keyboard.type('cmj');
  await expect(page).toHaveURL(/\/topics$/);
  await expect(page.locator('.listpane .filter input')).toHaveValue('cmj');
  await page.locator('.listpane .filter input').fill('');
  await page.locator('body').click({ position: { x: 700, y: 400 } });
  await page.keyboard.press('c');
  await expect(page).toHaveURL(/new\/topic/);
});

test('j, k and Enter walk the list; "/" filters; Enter on a focused button is the button\'s own', async ({ page }) => {
  await page.goto('topics');
  await page.keyboard.press('j');
  await expect(page.locator('.listpane .li.cursor')).toHaveCount(1);
  await page.keyboard.press('/');
  await expect(page.locator('.listpane .filter input')).toBeFocused();
  await page.keyboard.type('seed-orders');
  await page.keyboard.press('Escape');
  await page.locator('body').click({ position: { x: 700, y: 400 } });
  await page.goto('topics/seed-orders');
  await page.keyboard.press('j'); await page.keyboard.press('j');
  await page.getByRole('button', { name: 'Purge' }).focus();
  await page.keyboard.press('Enter'); // opens Purge's confirmation, not the list cursor's topic.
  await expect(page.locator('#confirm')).toBeVisible();
  await expect(page).toHaveURL(/topics\/seed-orders/);
});

test('while a dialog is open nothing else answers the keyboard, and dialogs do not stack', async ({ page }) => {
  await page.goto('topics/seed-orders');
  await page.locator('#msg-5').click();
  await expect(page.locator('#drawer-title')).toHaveAttribute('data-offset', '5');
  await page.keyboard.press('c'); await page.keyboard.press('m'); await page.keyboard.press('j');
  await expect(page).toHaveURL(/seed-orders/);
  await expect(page.locator('html')).not.toHaveAttribute('data-dock', 'open');
  await page.keyboard.press('Escape');
  await page.getByRole('button', { name: 'Delete' }).first().click();
  await expect(page.locator('#confirm')).toBeVisible();
  await page.keyboard.press('Meta+k');
  await page.keyboard.press('c');
  await expect(page.locator('#palette')).toBeHidden();
  await expect(page).toHaveURL(/seed-orders/);
  await page.keyboard.press('Escape');
  await expect(page.locator('#confirm')).toBeHidden();
});

test('Copy works on a page that is not a secure context', async ({ page }) => {
  await page.addInitScript(() => {
    Object.defineProperty(Navigator.prototype, 'clipboard', { get: () => undefined });
    Object.defineProperty(window, 'isSecureContext', { get: () => false });
  });
  const errors: string[] = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.goto('topics/seed-orders');
  await page.locator('.copychip .copy-btn').click();
  await expect(page.locator('.toast')).toBeVisible();
  expect(errors).toEqual([]);
});
