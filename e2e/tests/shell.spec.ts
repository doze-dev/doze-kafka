import { test, expect } from '../fixtures/console';

test('the theme choice sticks, and "system" follows the OS', async ({ page }) => {
  await page.emulateMedia({ colorScheme: 'dark' });
  await page.goto('');
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
  await page.locator('#theme-seg [data-mode="light"]').click();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');
  await expect(page.locator('body')).toHaveCSS('background-color', 'rgb(255, 255, 255)');
  await page.reload();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');
  await page.locator('#theme-seg [data-mode="system"]').click();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
  await page.emulateMedia({ colorScheme: 'light' });
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');
});

test('the fonts are the vendored Plex, served as fonts', async ({ page }) => {
  const fonts: string[] = [];
  page.on('response', (r) => { if (r.url().endsWith('.woff2')) fonts.push(`${r.status()} ${r.headers()['content-type']}`); });
  await page.goto('topics/seed-orders');
  await page.evaluate(() => document.fonts.ready);
  expect(await page.evaluate(() => document.fonts.check('14px "IBM Plex Sans Variable"') && document.fonts.check('500 13px "IBM Plex Mono"'))).toBe(true);
  expect(fonts.length).toBeGreaterThan(0);
  for (const f of fonts) expect(f).toBe('200 font/woff2');
});

test('the palette finds a topic and a group, and Enter goes there', async ({ page, openPalette }) => {
  await page.goto('');
  await openPalette();
  await page.locator('#pal-q').fill('seed-orders');
  await expect(page.locator('.pal-item.sel .pal-name')).toHaveText('seed-orders');
  await page.keyboard.press('Enter');
  await expect(page.locator('.idband h1')).toHaveText('seed-orders');
  await openPalette();
  await page.locator('#pal-q').fill('g-lag');
  await page.keyboard.press('Enter');
  await expect(page.locator('.idband h1')).toHaveText('g-lag');
});

// Back and Forward used to restore a cached snapshot into <body>, which detached the
// confirm dialog, the palette and the theme buttons the script held; everything that
// used them was dead afterwards.
test('Back and Forward leave the dialogs, palette and live updates working', async ({ page, openPalette }) => {
  await page.goto('topics/seed-orders');
  await page.locator('.rail a[href$="/groups"]').click();
  await expect(page.locator('.rail a.on')).toHaveText(/Consumer groups/);
  await page.goBack();
  await expect(page.locator('.idband h1')).toHaveText('seed-orders');
  await page.goForward();
  // Wait for the page Forward lands on before going Back: with no wait the second
  // navigation raced the history reload of the first (flaky on main, 5 in 16).
  await expect(page.locator('.rail a.on')).toHaveText(/Consumer groups/);
  await page.waitForLoadState('networkidle');
  await page.goBack();
  await expect(page.locator('.idband h1')).toHaveText('seed-orders');
  await page.getByRole('button', { name: 'Delete' }).first().click();
  await expect(page.locator('#confirm')).toBeVisible();
  await page.locator('#confirm-no').click();
  await expect(page.locator('#confirm')).toBeHidden();
  await openPalette();
  await page.keyboard.press('Escape');
  await expect(page.locator('#tape-and-table')).toHaveCount(1);
});

test('the rail collapses to icons and stays that way', async ({ page }) => {
  await page.goto('');
  await page.locator('#rail-toggle').click();
  await expect(page.locator('html')).toHaveAttribute('data-rail', 'slim');
  await expect(page.locator('.rail .ri-nm').first()).toBeHidden();
  await page.reload();
  await expect(page.locator('html')).toHaveAttribute('data-rail', 'slim');
  await page.locator('#rail-toggle').click();
  await expect(page.locator('html')).not.toHaveAttribute('data-rail', 'slim');
});

test('an unknown topic or group is a 404 page inside the shell, with a suggestion', async ({ page }) => {
  const missing = await page.goto('topics/seed-ordres');
  expect(missing?.status()).toBe(404);
  await expect(page.locator('.rail')).toBeVisible();
  await expect(page.locator('.errorpanel')).toContainText('seed-ordres');
  await expect(page.locator('.didyoumean a', { hasText: 'seed-orders' })).toBeVisible();
  await page.locator('.didyoumean a', { hasText: 'seed-orders' }).click();
  await expect(page.locator('.idband h1')).toHaveText('seed-orders');
  const group = await page.goto('groups/g-lagg');
  expect(group?.status()).toBe(404);
  await expect(page.locator('.didyoumean a', { hasText: 'g-lag' })).toBeVisible();
  const nowhere = await page.goto('no/such/page');
  expect(nowhere?.status()).toBe(404);
});
