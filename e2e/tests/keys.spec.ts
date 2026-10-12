import { test, expect } from '../fixtures/console';
import type { Page } from '@playwright/test';

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

// Sy-06: the key sheet is a dialog over the page. While it is open the page's keys
// are off: only the sheet's own act (esc and ? close it, Tab stays inside it, the
// button that has focus answers Enter), and ⌘K swaps it for the palette, since one
// thing is open at a time. The sheet used to let every single key through to the
// page behind it: ? then p paused the tail under the sheet, j and k moved a cursor
// nobody could see, c left for the create form, g t for another section.
//
// One test per page, every key of the sheet on it. A key is pressed, the page is
// given a moment to do what it would (a boosted navigation is not instant), and
// then nothing may have changed.

async function openSheet(page: Page) {
  await page.keyboard.press('?');
  await expect(page.locator('#help')).toBeVisible();
}

/** Presses keys with the sheet open and holds that the sheet is still there and `unchanged` holds. */
async function inert(page: Page, keys: string[], unchanged: () => Promise<void>) {
  for (const k of keys) await page.keyboard.press(k);
  await page.waitForTimeout(250);
  await expect(page.locator('#help'), `after ${keys.join(' ')}`).toBeVisible();
  await expect(page.locator('#palette'), `after ${keys.join(' ')}`).toBeHidden();
  await unchanged();
}

test('behind the key sheet a list page ignores j, k, Enter, /, c and every g chord', async ({ page }) => {
  await page.goto('topics');
  await openSheet(page);
  const still = async () => {
    await expect(page).toHaveURL(/\/topics$/);
    await expect(page.locator('.cursor')).toHaveCount(0);
    await expect(page.locator('.list input[data-filter-target]')).not.toBeFocused();
  };
  for (const keys of [['j'], ['k'], ['/'], ['c'], ['g', 'r'], ['g', 'g'], ['g', 's'], ['g', 'a'], ['g', 'c'], [']'], ['['], ['p'], ['e'], ['u'], ['m'], ['r']]) {
    await inert(page, keys, still);
  }
  // Tab is the sheet's: focus stays inside it.
  for (let i = 0; i < 4; i++) {
    await page.keyboard.press('Tab');
    expect(await page.evaluate(() => !!document.activeElement?.closest('#help'))).toBe(true);
  }
  // ? is the sheet's too: it closes it. Then the page has its keys back.
  await page.keyboard.press('?');
  await expect(page.locator('#help')).toBeHidden();
  await page.keyboard.press('j');
  await expect(page.locator('main .r.cursor')).toHaveCount(1);
  // Enter with the sheet open is the focused control's (the sheet's esc button), not
  // "open the row under the cursor".
  await openSheet(page);
  await expect(page.locator('#help-close')).toBeFocused();
  await page.keyboard.press('Enter');
  await expect(page.locator('#help')).toBeHidden();
  await page.waitForTimeout(250);
  await expect(page).toHaveURL(/\/topics$/);
  // g t, from somewhere it would go.
  await page.goto('groups');
  await openSheet(page);
  await inert(page, ['g', 't'], async () => { await expect(page).toHaveURL(/\/groups$/); });
  await page.keyboard.press('Escape');
  await page.keyboard.press('g');
  await page.keyboard.press('t');
  await expect(page).toHaveURL(/\/topics$/);
});

test('behind the key sheet a topic ignores p, ], [, j, k and ⌘↵; esc closes the sheet before the panel', async ({ page, setEditor }) => {
  await page.goto('topics/seed-orders');
  await page.locator('#msg-5').click();
  await expect(page.locator('#inspector-title')).toHaveAttribute('data-offset', '5');
  await openSheet(page);
  const still = async () => {
    await expect(page).toHaveURL(/\/topics\/seed-orders\?open=5$/);
    await expect(page.locator('html')).not.toHaveAttribute('data-dock', 'open');
    await expect(page.locator('.cursor')).toHaveCount(0);
    await expect(page.locator('#inspector-title')).toHaveAttribute('data-offset', '5');
  };
  for (const keys of [['p'], [']'], ['['], ['j'], ['k'], ['c'], ['/'], ['g', 'g']]) await inert(page, keys, still);
  // esc is the sheet's first: the panel behind it stays open, and the next esc is the page's.
  await page.keyboard.press('Escape');
  await expect(page.locator('#help')).toBeHidden();
  await expect(page.locator('#inspector')).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(page.locator('#inspector')).toBeHidden();
  // The keys are the page's again: ] moves to the next tab.
  await page.keyboard.press(']');
  await expect(page).toHaveURL(/tab=/);
  await page.goto('topics/seed-empty');
  // ⌘↵ sends from the composer. With the sheet open it sends nothing.
  await page.keyboard.press('p');
  await expect(page.locator('html')).toHaveAttribute('data-dock', 'open');
  await setEditor('{"behind":"the sheet"}');
  await page.evaluate(() => (document.activeElement as HTMLElement | null)?.blur());
  await openSheet(page);
  await inert(page, ['Meta+Enter'], async () => {});
  await inert(page, ['Control+Enter'], async () => {});
  await page.keyboard.press('Escape');
  await expect(page.locator('#toasts .toast')).toHaveCount(0);
  await expect(page.locator('.tape-strip .tick')).toHaveCount(0);
});

test('⌘K from the key sheet swaps it for the palette: one thing is open at a time', async ({ page }) => {
  await page.goto('topics');
  await openSheet(page);
  await page.keyboard.press('ControlOrMeta+k');
  await expect(page.locator('#palette')).toBeVisible();
  await expect(page.locator('#help')).toBeHidden();
  // In the palette every key of the sheet is text in its field, and the page does nothing.
  await page.keyboard.type('pejkcumr][?/gt');
  await expect(page.locator('#pal-q')).toHaveValue('pejkcumr][?/gt');
  await page.waitForTimeout(250);
  await expect(page).toHaveURL(/\/topics$/);
  await expect(page.locator('#help')).toBeHidden();
  await expect(page.locator('.cursor')).toHaveCount(0);
});

test('behind a confirm no key of the sheet acts, the sheet itself included', async ({ page }) => {
  await page.goto('topics/seed-orders');
  await page.getByRole('button', { name: 'More actions' }).click();
  await page.getByRole('button', { name: 'Delete' }).first().click();
  await expect(page.locator('#confirm')).toBeVisible();
  for (const keys of [['?'], ['p'], [']'], ['['], ['j'], ['k'], ['c'], ['/'], ['e'], ['u'], ['m'], ['r'], ['g', 't'], ['g', 'r'], ['ControlOrMeta+k']]) {
    for (const k of keys) await page.keyboard.press(k);
    await page.waitForTimeout(150);
    await expect(page.locator('#confirm'), `after ${keys.join(' ')}`).toBeVisible();
    await expect(page.locator('#help'), `after ${keys.join(' ')}`).toBeHidden();
    await expect(page.locator('#palette'), `after ${keys.join(' ')}`).toBeHidden();
    await expect(page).toHaveURL(/\/topics\/seed-orders$/);
    await expect(page.locator('html')).not.toHaveAttribute('data-dock', 'open');
    await expect(page.locator('.cursor')).toHaveCount(0);
  }
  await page.keyboard.press('Escape');
  await expect(page.locator('#confirm')).toBeHidden();
});
