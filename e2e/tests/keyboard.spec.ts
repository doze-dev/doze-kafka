import { test, expect } from '../fixtures/console';

test('single keys act alone: with a modifier held, or typing in a field, they do nothing', async ({ page }) => {
  await page.goto('topics');
  for (const combo of ['Meta+c', 'Control+c', 'Alt+c', 'Meta+j', 'Control+m']) await page.keyboard.press(combo);
  await expect(page).toHaveURL(/\/topics$/);
  await page.locator('.list input[data-filter-target]').focus();
  await page.keyboard.type('cmj');
  await expect(page).toHaveURL(/\/topics$/);
  await expect(page.locator('.list input[data-filter-target]')).toHaveValue('cmj');
  await page.locator('.list input[data-filter-target]').fill('');
  await page.locator('body').click({ position: { x: 700, y: 400 } });
  await page.keyboard.press('c');
  await expect(page).toHaveURL(/new\/topic/);
});

test('j, k and Enter walk the rows; "/" filters; Enter on a focused button is the button\'s own', async ({ page }) => {
  await page.goto('topics');
  await page.keyboard.press('j');
  await expect(page.locator('main .r.cursor')).toHaveCount(1); // the home table's rows.
  await page.keyboard.press('/');
  await expect(page.locator('.list input[data-filter-target]')).toBeFocused();
  await page.keyboard.type('seed-orders');
  await expect(page.locator('.list .li:not([hidden])')).toHaveCount(1);
  await page.keyboard.press('Escape'); // clears the filter, as the keys board says.
  await expect(page.locator('.list input[data-filter-target]')).toHaveValue('');
  await page.goto('topics/seed-orders');
  await page.locator('body').click({ position: { x: 700, y: 400 } });
  await page.keyboard.press('j'); await page.keyboard.press('j');
  await page.getByRole('button', { name: 'More actions' }).click();
  await page.getByRole('button', { name: 'Purge' }).focus();
  await page.keyboard.press('Enter'); // opens Purge's confirmation, not the row under the cursor.
  await expect(page.locator('#confirm')).toBeVisible();
  await expect(page).toHaveURL(/topics\/seed-orders/);
});

test('while a dialog is open nothing else answers the keyboard, and dialogs do not stack', async ({ page }) => {
  await page.goto('topics/seed-orders');
  await page.getByRole('button', { name: 'More actions' }).click();
  await page.getByRole('button', { name: 'Delete' }).first().click();
  await expect(page.locator('#confirm')).toBeVisible();
  await page.keyboard.press('Meta+k');
  await page.keyboard.press('c');
  await expect(page.locator('#palette')).toBeHidden();
  await expect(page).toHaveURL(/seed-orders/);
  await page.keyboard.press('Escape');
  await expect(page.locator('#confirm')).toBeHidden();
});

test('the inspector is the page\'s own: its keys keep working beside it, and Esc closes it', async ({ page }) => {
  await page.goto('topics/seed-orders');
  await page.locator('#msg-5').click();
  await expect(page.locator('#inspector-title')).toHaveAttribute('data-offset', '5');
  // Not a dialog: it does not hold the keyboard. j still moves the page's rows.
  await page.locator('body').click({ position: { x: 700, y: 560 } });
  await page.keyboard.press('j');
  await expect(page.locator('tr.cursor')).toHaveCount(1);
  await page.keyboard.press('Escape');
  await expect(page.locator('#inspector')).toBeHidden();
});

test('Copy works on a page that is not a secure context', async ({ page }) => {
  await page.addInitScript(() => {
    Object.defineProperty(Navigator.prototype, 'clipboard', { get: () => undefined });
    Object.defineProperty(window, 'isSecureContext', { get: () => false });
  });
  const errors: string[] = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.goto('topics/seed-orders?tab=config');
  await page.locator('.copychip .copy-btn').click();
  await expect(page.locator('.toast')).toBeVisible();
  expect(errors).toEqual([]);
});
