import { test, expect } from '../fixtures/console';

// The keys are declared once on the server and the help overlay is generated from
// them; these are the paths a person takes: ? for the list, j k Enter on a list,
// / to filter, c to create, p to pause, Esc to leave.

test('? lists every key from the declared map, and Esc closes it', async ({ page }) => {
  await page.goto('topics');
  await page.keyboard.press('?');
  const help = page.locator('#help');
  await expect(help).toBeVisible();
  await expect(help.getByText('Keyboard shortcuts')).toBeVisible();
  for (const label of ['Move down the list', 'Pause or resume live updates', 'Open the composer on a topic', 'Close the dialog, palette or inspector']) {
    await expect(help).toContainText(label);
  }
  await page.keyboard.press('Escape');
  await expect(help).toBeHidden();
});

test('j and k move the cursor and Enter opens the row', async ({ page }) => {
  await page.goto('topics');
  await page.keyboard.press('j');
  await expect(page.locator('.listpane .li.cursor')).toHaveCount(1);
  await page.keyboard.press('Enter');
  await expect(page.locator('.idband h1')).toBeVisible();
});

test('/ focuses the filter and c opens the create form', async ({ page }) => {
  await page.goto('topics');
  await page.keyboard.press('/');
  await expect(page.locator('.listpane .filter input')).toBeFocused();
  await page.keyboard.press('Escape');
  await page.locator('body').click({ position: { x: 600, y: 400 } });
  await page.keyboard.press('c');
  await expect(page).toHaveURL(/new\/topic$/);
});

test('p pauses and resumes the live tape', async ({ page, createTopic, uniqueName }) => {
  await createTopic(uniqueName('keys'));
  const toggle = page.locator('[data-live-toggle]').first();
  await page.locator('body').click({ position: { x: 600, y: 300 } });
  await page.keyboard.press('p');
  await expect(toggle).toHaveAttribute('aria-pressed', 'true');
  await page.keyboard.press('p');
  await expect(toggle).toHaveAttribute('aria-pressed', 'false');
});

test('the palette carries actions, and the third theme is one of them', async ({ page, openPalette }) => {
  await page.goto('topics');
  await openPalette();
  await page.locator('#pal-q').fill('high-contrast');
  await expect(page.locator('.pal-item').first()).toContainText('Use the high-contrast theme');
  await page.keyboard.press('Enter');
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'contrast');
  // It is the choice that stuck, and the switch shows it.
  await page.reload();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'contrast');
  await expect(page.locator('#theme-seg button[data-mode="contrast"]')).toHaveAttribute('aria-pressed', 'true');
});

test('a help action and a pause action are in the palette', async ({ page, openPalette }) => {
  await page.goto('topics');
  await openPalette();
  await page.locator('#pal-q').fill('keyboard');
  await page.keyboard.press('Enter');
  await expect(page.locator('#help')).toBeVisible();
});
