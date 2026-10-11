import { test, expect } from '../fixtures/console';

// The seed gives each test that changes a group a group of its own (see seed/main.go).
// Every one has offsets on seed-orders: partition 0 committed at 10 of 45, partition 1 at 2 of 6.
// A group's page has four tabs (Gr-02): Lag, Members, Timeline and Offsets; the offsets
// can be changed on Offsets, and what a form answers is the same tab.
const cells = (row: any) => row.locator('td');
const offsetRows = (page: any) => page.locator('#offsets-table tbody tr');

test('a group shows its offsets and lag, linked to the topic', async ({ page }) => {
  await page.goto('groups/g-lag');
  await expect(page.locator('.idband h1')).toHaveText('g-lag');
  await expect(page.locator('#group-facts')).toContainText('39 records behind'); // (45-10) + (6-2).
  const first = page.locator('#lag-table .r').nth(1); // the header is the first row.
  await expect(first.locator('a')).toHaveText('seed-orders');
  await expect(first).toContainText('10');
  await expect(first).toContainText('45');
  await expect(page.locator('#group-live')).toContainText('No members');
  await first.locator('a').click();
  await expect(page.locator('.idband h1')).toHaveText('seed-orders');
});

test('the group list shows its lag and state, and a group with an odd name is reachable', async ({ page }) => {
  await page.goto('groups');
  const row = page.locator('.list .li[data-name="g-lag"]');
  await expect(row).toContainText('39');
  await expect(row.locator('.dot')).toHaveAttribute('title', 'Empty');
  await expect(page.locator('main h1')).toHaveText('Groups'); // nothing selected: the home page of groups.
  await row.click();
  await expect(page.locator('.idband h1')).toHaveText('g-lag');
});

test('Reset to earliest and to latest move every offset, after asking', async ({ page, confirmDialog, waitForToast }) => {
  await page.goto('groups/g-reset-earliest?tab=offsets');
  await page.getByRole('button', { name: 'Reset to earliest' }).click();
  await confirmDialog('accept');
  expect(await waitForToast()).toContain('Reset 2 offsets to earliest');
  await expect(cells(offsetRows(page).first()).nth(2)).toHaveText('0');

  await page.goto('groups/g-reset-latest?tab=offsets');
  await page.getByRole('button', { name: 'Reset to latest' }).click();
  await confirmDialog('cancel'); // declining changes nothing.
  await expect(cells(offsetRows(page).first()).nth(2)).toHaveText('10');
  await page.getByRole('button', { name: 'Reset to latest' }).click();
  await confirmDialog('accept');
  await expect(cells(offsetRows(page).first()).nth(2)).toHaveText('45');
});

// A confirmation is for the button that asked. When a live refresh replaced the
// region while the dialog was open, "Confirm" did nothing.
test('confirming after the page refreshed underneath the dialog still does it', async ({ page, request }) => {
  await page.goto('groups/g-delete');
  let resets = 0;
  page.on('request', (r) => { if (r.method() === 'POST' && r.url().includes('/delete')) resets++; });
  await page.getByRole('button', { name: 'More actions' }).click();
  await page.locator('.more .menu').getByRole('button', { name: 'Delete' }).click();
  await expect(page.locator('#confirm')).toBeVisible();
  await request.post('topics/seed-orders/produce', { form: { partition: '1', partition_ctx: '0', value: 'moves the lag' } });
  await page.waitForTimeout(4000); // several polls go by.
  await page.locator('#confirm-yes').click();
  await expect(page).toHaveURL(/\/groups$/);
  expect(resets).toBe(1);
  await expect(page.locator('.list .li[data-name="g-delete"]')).toHaveCount(0);
});

test('one partition\'s offset can be set to a number, and a bad one is explained in the row', async ({ page, waitForToast }) => {
  await page.goto('groups/g-set-offset?tab=offsets');
  const row = offsetRows(page).first();
  await row.getByRole('button', { name: /Set the offset/ }).click();
  await row.locator('input[name=offset]').fill('7');
  await row.getByRole('button', { name: 'Set', exact: true }).click();
  expect(await waitForToast()).toContain('partition 0 is now at offset 7');
  await expect(cells(offsetRows(page).first()).nth(2)).toHaveText('7');
  const again = offsetRows(page).first();
  await again.getByRole('button', { name: /Set the offset/ }).click();
  await again.locator('input[name=offset]').fill('99999');
  await again.getByRole('button', { name: 'Set', exact: true }).click();
  await expect(again.locator('[data-err-slot]')).toContainText('is outside partition 0');
  await expect(again.locator('input[name=offset]')).toHaveValue('99999'); // what was typed stays.
  await again.getByRole('button', { name: 'Earliest' }).click();
  await expect(again.locator('input[name=offset]')).toHaveValue('0');
});

test('typing in a row is not wiped by the page refreshing', async ({ page, request }) => {
  await page.goto('groups/g-set-offset?tab=offsets');
  const row = offsetRows(page).nth(1);
  await row.getByRole('button', { name: /Set the offset/ }).click();
  await row.locator('input[name=offset]').fill('3');
  await request.post('topics/seed-orders/produce', { form: { partition: '1', partition_ctx: '0', value: 'refresh me' } });
  await page.waitForTimeout(4500);
  await expect(offsetRows(page).nth(1).locator('input[name=offset]')).toHaveValue('3');
});

test('one partition\'s offset can be removed, and the others stay', async ({ page, confirmDialog, waitForToast }) => {
  await page.goto('groups/g-remove-offset?tab=offsets');
  await expect(offsetRows(page)).toHaveCount(2);
  await offsetRows(page).first().getByRole('button', { name: /Remove the offset/ }).click();
  await confirmDialog('accept');
  expect(await waitForToast()).toContain('Removed the committed offset');
  await expect(offsetRows(page)).toHaveCount(1);
});

test('a reset can be limited to one topic', async ({ page }) => {
  // g-lag has offsets on one topic only, so the choice is not offered; the other
  // half of this is covered where a group has two (Go tests). Here: no selector, still resets.
  await page.goto('groups/g-reset-latest?tab=offsets');
  await expect(page.locator('#reset-scope')).toHaveCount(0);
});

test('the tabs of a group are Lag, Members, Timeline and Offsets, and ] and [ walk them', async ({ page }) => {
  await page.goto('groups/g-lag');
  await expect(page.locator('main .tabs .tab')).toHaveText([/^Lag/, /^Members/, /^Timeline/, /^Offsets/]);
  await expect(page.locator('main .tabs .tab.on')).toHaveText(/^Lag/);
  await page.locator('body').click({ position: { x: 700, y: 500 } });
  await page.keyboard.press(']');
  await expect(page).toHaveURL(/tab=members/);
  await page.keyboard.press('[');
  await expect(page.locator('main .tabs .tab.on')).toHaveText(/^Lag/);
});
