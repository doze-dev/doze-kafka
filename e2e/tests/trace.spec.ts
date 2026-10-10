import { test, expect } from '../fixtures/console';

// The seed writes one trace across two topics (see seed/main.go): a record in
// seed-trace-a and, a moment later, one in seed-trace-b, both carrying the same
// traceparent. g-trace has read the first and not the second. seed-orders' own records
// carry a different traceparent, and its "plain text" record carries none.

const TRACE = '0af7651916cd43dd8448eb211c80319c';

test('a record that carries a trace offers to follow it, and the trail crosses topics in order', async ({ page }) => {
  await page.goto('topics/seed-trace-a');
  await page.locator('#tape-and-table tbody tr').first().click();
  const drawer = page.locator('#drawer');
  await expect(drawer).toBeVisible();
  const follow = drawer.getByRole('link', { name: /Follow this message/ });
  await expect(follow).toBeVisible();
  await expect(drawer).toContainText(TRACE);
  await follow.click();

  await expect(page).toHaveURL(/\/trace\?/);
  await expect(page.locator('.idband h1')).toHaveText('Trace');
  await expect(page.locator('.copychip')).toContainText(TRACE);
  // The scan finishes: the list is there and the region stops polling.
  const rows = page.locator('#trace-list li');
  await expect(rows).toHaveCount(2);
  await expect(page.locator('#trace-live')).not.toHaveAttribute('data-live', /.+/);

  await expect(rows.nth(0)).toContainText('seed-trace-a');
  await expect(rows.nth(1)).toContainText('seed-trace-b');
  await expect(rows.nth(0)).toContainText('this message'); // where the person started.
  await expect(rows.nth(1)).not.toContainText('this message');
  await expect(rows.nth(0)).toContainText('start');
  await expect(rows.nth(1)).toContainText('key checkout-1');
  // Which group has read what: g-trace committed past the first and not the second.
  await expect(rows.nth(0).locator('.tr-cons')).toContainText('g-trace · read');
  await expect(rows.nth(1).locator('.tr-cons')).not.toContainText('g-trace');
  // The honest limit is on the page, not in a manual.
  await expect(page.locator('.honest', { hasText: 'invisible here' })).toBeVisible();
  await expect(page.locator('#trace-partial')).toHaveCount(0);

  // A row links back to its record.
  await rows.nth(1).getByRole('link', { name: 'seed-trace-b' }).click();
  await expect(page.locator('.idband h1')).toHaveText('seed-trace-b');
});

test('a record with no trace header offers nothing to follow', async ({ page }) => {
  // seed-orders partition 0, offset 40: "plain text, no key", written with no headers.
  await page.goto('topics/seed-orders?partition=0&jump=40');
  await page.locator('#msg-40').click();
  await expect(page.locator('#drawer')).toBeVisible();
  await expect(page.locator('#drawer')).toContainText('No headers on this record');
  await expect(page.locator('#drawer-trace')).toHaveCount(0);
});

test('the span can be widened, and the choice is in the address', async ({ page }) => {
  await page.goto(`trace?id=${TRACE}`);
  await expect(page.locator('.seg-link[aria-current]')).toHaveText('5 minutes');
  await page.getByRole('link', { name: '3 hours' }).click();
  await expect(page).toHaveURL(/window=3h/);
  await expect(page.locator('.seg-link[aria-current]')).toHaveText('3 hours');
  await expect(page.locator('#trace-list li')).toHaveCount(2);
});

test('a trace nothing carries says so', async ({ page }) => {
  await page.goto('trace?id=ffffffffffffffffffffffffffffffff');
  await expect(page.locator('.honest', { hasText: 'No record with this trace id was found' })).toBeVisible();
});
