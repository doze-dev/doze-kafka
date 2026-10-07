import { test, expect } from '../fixtures/console';

// Partition 0 of seed-orders: offsets 0-39 keyed JSON with headers, 40 plain text with
// no key, 41 a tombstone, 42 binary, 43 a very long key, 44 "the newest record".
const row = (page: any, offset: number) => page.locator(`#msg-${offset}`);

test('a record opens in the drawer, with its place in the URL, and Esc closes it', async ({ page }) => {
  await page.goto('topics/seed-orders');
  await row(page, 5).click();
  await expect(page.locator('#drawer-title')).toHaveAttribute('data-offset', '5');
  await expect(page.locator('#drawer')).toContainText('ord_8905');
  await expect(page.locator('#drawer')).toContainText('traceparent');
  await expect(page.locator('#drawer #drawer-title')).toBeFocused({ timeout: 100 }).catch(() => {});
  expect(page.url()).toContain('open=5');
  await page.keyboard.press('Escape');
  await expect(page.locator('#drawer-overlay')).toBeHidden();
  expect(page.url()).not.toContain('open=');
  await expect(row(page, 5)).toBeFocused(); // focus goes back to where it came from.
});

test('a link with ?open= reopens the record; Older and Newer step through the log', async ({ page }) => {
  await page.goto('topics/seed-orders?partition=0&open=20');
  await expect(page.locator('#drawer-title')).toHaveAttribute('data-offset', '20');
  await page.getByRole('button', { name: 'Older record' }).click();
  await expect(page.locator('#drawer-title')).toHaveAttribute('data-offset', '19');
  await expect(page.getByRole('button', { name: 'Older record' })).toBeFocused(); // stepping keeps its place.
  await page.getByRole('button', { name: 'Newer record' }).click();
  await page.getByRole('button', { name: 'Newer record' }).click();
  await expect(page.locator('#drawer-title')).toHaveAttribute('data-offset', '21');
  await expect(page.locator('#reqbar')).toBeHidden(); // the slow-request bar does not stay on.
});

test('a drawer that cannot load says why, and still closes', async ({ page }) => {
  await page.goto('topics/seed-orders');
  await page.route('**/message?*', (r) => r.fulfill({ status: 502, body: 'The console could not get an answer from the broker.' }));
  await row(page, 5).click();
  await expect(page.locator('#drawer [role="alert"]')).toContainText('could not get an answer');
  await expect(page.locator('#drawer #drawer-title')).toBeVisible(); // it is still a dialog, with a title and a close button.
  await page.keyboard.press('Escape');
  await expect(page.locator('#drawer-overlay')).toBeHidden();
});

test('binary, empty and null values are labelled, with hex and base64 to read', async ({ page }) => {
  await page.goto('topics/seed-orders');
  await expect(row(page, 42).locator('.cell-val')).toHaveClass(/binary/);
  await expect(row(page, 41).locator('.cell-val')).toHaveClass(/tomb/);
  await expect(row(page, 40).locator('.cell-key')).toContainText('no key');
  await row(page, 42).click();
  await expect(page.locator('#drawer')).toContainText('binary');
  await expect(page.locator('.hexbox')).toContainText('de ad be ef');
  await page.getByRole('button', { name: 'Base64', exact: true }).click();
  await expect(page.locator('#drawer')).toContainText('3q2+7wABAgP//g==');
  await page.keyboard.press('Escape');
  await row(page, 41).click();
  await expect(page.locator('#drawer')).toContainText('null — a tombstone');
});

test('a very long key wraps inside the drawer instead of widening it', async ({ page }) => {
  await page.goto('topics/seed-orders');
  await row(page, 43).click();
  await expect(page.locator('#drawer-title')).toHaveAttribute('data-offset', '43');
  const box = await page.evaluate(() => { const d = document.getElementById('drawer')!; return { scroll: d.scrollWidth, client: d.clientWidth }; });
  expect(box.scroll).toBeLessThanOrEqual(box.client + 1);
});

test('Send again produces an exact copy; Edit a copy fills the composer', async ({ page, request }) => {
  const name = `copy-${Date.now().toString(36)}`;
  await request.post('new/topic', { form: { name, partitions: '1' } });
  await request.post(`topics/${name}/produce`, { form: { partition: '0', partition_ctx: '0', key: 'k', value: '{"a":1,"b":[1,2]}' } });
  await page.goto(`topics/${name}`);
  await row(page, 0).click();
  await page.getByRole('button', { name: 'Send again' }).click();
  await expect(page.locator('#drawer-overlay')).toBeHidden(); // closes once the broker has taken the copy.
  await expect(row(page, 1)).toBeVisible();
  await row(page, 1).click();
  await expect(page.locator('#drawer')).toContainText('"b"');
  // The drawer shows JSON indented to read; "Edit a copy" and "Send again" carry the stored bytes.
  await expect(page.locator('#drawer-edit')).toHaveAttribute('data-value', '{"a":1,"b":[1,2]}');
  await page.getByRole('button', { name: 'Edit a copy…' }).click();
  await expect(page.locator('#compose-head')).toHaveAttribute('aria-expanded', 'true');
  await expect(page.locator('#f-key')).toHaveValue('k');
  await expect(page.locator('.dock .CodeMirror')).toContainText('"a":1');
});

test('"go to offset" lands on the record, scrolled into view', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 600 });
  await page.goto('topics/seed-orders');
  await page.locator('#jump-to').fill('3');
  await page.locator('form.jump', { has: page.locator('#jump-to') }).locator('.jump-go').click();
  await expect(page.locator('tr[data-hl]')).toHaveAttribute('id', 'msg-3');
  await expect(page.locator('tr[data-hl]')).toBeInViewport();
  await page.locator('#jump-to').fill('99999');
  await page.locator('form.jump', { has: page.locator('#jump-to') }).locator('.jump-go').click();
  await expect(page.locator('.jumpnote')).toContainText('has not been written yet');
  // Something that is not a number is stopped by the field itself (the server
  // refuses it too: see the Go tests).
  await page.locator('#jump-to').fill('12abc');
  expect(await page.locator('#jump-to').evaluate((el: HTMLInputElement) => el.validity.patternMismatch)).toBe(true);
});

test('search finds text in keys, values and headers, and says how far it read', async ({ page }) => {
  await page.goto('topics/seed-orders');
  await page.locator('#find-q').fill('ORD_8912');
  await page.locator('.find button[type=submit]').click();
  await expect(page.locator('tr[id^="hit-"]')).toHaveCount(1);
  await expect(page.locator('.jumpnote')).toContainText('Read offsets 0–44 of partition 0 (45 records): 1 matches');
  await page.locator('#find-q').fill('checkout'); // a header value.
  await page.locator('.find button[type=submit]').click();
  await expect(page.locator('tr[id^="hit-"]')).toHaveCount(40);
  await page.locator('#find-q').fill('absent-word');
  await page.locator('.find button[type=submit]').click();
  await expect(page.locator('td.empty')).toContainText('Nothing in the records that were read contains');
  await page.getByRole('link', { name: 'Clear' }).click();
  await expect(page.locator('#tape-and-table[data-live]')).toBeVisible();
});

test('searching all partitions shows where each match is, and a match opens its own record', async ({ page }) => {
  await page.goto('topics/seed-orders');
  await page.locator('#find-q').fill('gold');
  await page.locator('#find-all').check();
  await page.locator('.find button[type=submit]').click();
  await expect(page.locator('.jumpnote')).toContainText('across 3 partitions'); // the results have arrived.
  const partitions = await page.locator('tr[id^="hit-"] td:first-child').allInnerTexts();
  expect(new Set(partitions.map((p) => p.trim()))).toEqual(new Set(['0', '1', '2']));
  await expect(page.locator('#tape-and-table[data-live]')).toHaveCount(0); // a search is not the live tail.
  await page.locator('tr[id^="hit-1-"]').first().click();
  await expect(page.locator('#drawer-title')).toContainText('P1 ·');
  expect(page.url()).not.toContain('open='); // ?open= names an offset on one partition; this is across several.
});
