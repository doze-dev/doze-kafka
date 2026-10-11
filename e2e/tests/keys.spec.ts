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
  // Sy-06: three columns, as the board names them.
  await expect(help.locator('.cap')).toHaveText(['Go to', 'In a list or table', 'On this page']);
  for (const label of ['Topics', 'Jump to anything', 'Next row', 'Open the row on the right', 'Close the panel, clear the filter', 'Next tab', 'Pause or resume the live view', 'Produce (on a topic)', 'This help']) {
    await expect(help).toContainText(label);
  }
  await expect(help).toContainText('never act on data without asking first');
  await page.keyboard.press('Escape');
  await expect(help).toBeHidden();
});

test('j and k move the cursor and Enter opens the row', async ({ page }) => {
  await page.goto('topics');
  await page.keyboard.press('j');
  await expect(page.locator('main .r.cursor')).toHaveCount(1);
  await page.keyboard.press('Enter');
  await expect(page.locator('.idband h1')).toBeVisible();
  await expect(page).toHaveURL(/\/topics\/[^/]+$/);
});

test('/ focuses the filter and c opens the create form', async ({ page }) => {
  await page.goto('topics');
  await page.keyboard.press('/');
  await expect(page.locator('.list input[data-filter-target]')).toBeFocused();
  await page.keyboard.press('Escape');
  await expect(page.locator('.list input[data-filter-target]')).not.toBeFocused();
  await page.locator('body').click({ position: { x: 600, y: 400 } });
  await page.keyboard.press('c');
  await expect(page).toHaveURL(/new\/topic$/);
});

test('p pauses and resumes the live view on Traffic, and is produce on a topic', async ({ page, createTopic, uniqueName }) => {
  await page.goto('traffic');
  const toggle = page.locator('[data-live-toggle]').first();
  await page.locator('body').click({ position: { x: 700, y: 120 } });
  await page.keyboard.press('p');
  await expect(toggle).toHaveAttribute('aria-pressed', 'true');
  await expect(toggle).toHaveText('Resume');
  await page.keyboard.press('p');
  await expect(toggle).toHaveAttribute('aria-pressed', 'false');
  await expect(toggle).toHaveText('Pause');
  // On a topic the same key opens the composer: pause and produce are never both on one page.
  await createTopic(uniqueName('keys'));
  await page.locator('body').click({ position: { x: 700, y: 300 } });
  await page.keyboard.press('p');
  await expect(page.locator('html')).toHaveAttribute('data-dock', 'open');
  await expect(page.locator('[data-live-toggle]').first()).toHaveAttribute('aria-pressed', 'false');
});

test('g then a letter goes to a section, and the chord lapses', async ({ page }) => {
  await page.goto('topics');
  await page.locator('body').click({ position: { x: 700, y: 400 } });
  await page.keyboard.press('g');
  await page.keyboard.press('g');
  await expect(page).toHaveURL(/\/groups$/);
  await page.keyboard.press('g');
  await page.keyboard.press('t');
  await expect(page).toHaveURL(/\/topics$/);
  await page.keyboard.press('g');
  await page.keyboard.press('r');
  await expect(page).toHaveURL(/\/traffic$/);
  await page.keyboard.press('g');
  await page.keyboard.press('x'); // not a section: nothing happens, and the next key is its own.
  await expect(page).toHaveURL(/\/traffic$/);
});

// Sy-05 lists no theme rows; the theme is the top bar's words (Sy-07).
test('the theme is the top bar\'s control, not a palette row, and the choice sticks', async ({ page, openPalette }) => {
  await page.goto('topics');
  await openPalette();
  await page.locator('#pal-q').fill('high-contrast');
  await expect(page.locator('.pal-item', { hasText: /theme/i })).toHaveCount(0);
  await page.keyboard.press('Escape');
  await page.locator('#theme-seg button[data-mode="contrast"]').click();
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
