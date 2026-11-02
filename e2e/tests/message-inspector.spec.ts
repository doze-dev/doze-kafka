import { execFileSync } from 'node:child_process';
import { test, expect } from '../fixtures/console';
import { kafkaAddr } from '../addr';

// Tp-06: the message inspector. Partition 0 of seed-orders: offsets 0-39 keyed JSON with a
// traceparent and a source header, 40 plain text with no key, 41 a tombstone, 42 binary.

test('a record shows its fields as the board names them, and says what it does not know', async ({ page }) => {
  await page.goto('topics/seed-orders?partition=0&open=5');
  const f = page.locator('#inspector .tp-fields');
  await expect(f).toContainText('Timestamp');
  await expect(f.locator('time.when').first()).toHaveAttribute('data-style', 'datetimems');
  await expect(f).toContainText('CreateTime');
  await expect(f.locator('> span').nth(4)).toHaveText('Key');
  await expect(f).toContainText('ord_8905');
  await expect(f).toContainText(/value \d+ B · headers \d+ B/);
  // Who wrote it is the ledger's answer or "not known": never empty, never a guess.
  const who = f.locator('> span').nth(9);
  await expect(f.locator('> span').nth(8)).toHaveText('Written by');
  await expect(who).not.toHaveText('');
  await expect(page.getByRole('link', { name: 'Follow this message' })).toBeVisible();
  await expect(page.locator('#inspector')).toContainText('traces it from produce to every fetch and commit');
});

test('Written by is the client the recorder saw produce the record; a record it did not see is not known', async ({ page, createTopic, uniqueName }) => {
  const name = uniqueName('wb');
  await createTopic(name);
  // A client's own connection is in the recorder (the console's own is left out of it).
  execFileSync('go', ['run', './e2e/put', kafkaAddr(), name, '2026-03-14T23:59:07Z', 'hello'], { cwd: '..', env: { ...process.env, GOWORK: 'off' } });
  await page.request.post(`topics/${name}/produce`, { form: { partition: '0', value: 'from the console', acks: '1' } });
  const who = async (offset: number) => {
    await page.goto(`topics/${name}?partition=0&open=${offset}`);
    await expect(page.locator('#inspector .tp-fields > span').nth(8)).toHaveText('Written by');
    return page.locator('#inspector .tp-fields > span').nth(9);
  };
  await expect(await who(0)).toHaveText('doze-put');
  const other = await who(1);
  await expect(other).toHaveText('not known');
  await expect(other.locator('.mut')).toHaveAttribute('title', /not in the recorder/);
});

test('the Copy menu opens in flow under the actions, copies, and Esc closes only the menu', async ({ page, context }) => {
  await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  await page.goto('topics/seed-orders?partition=0&open=5');
  const menu = page.locator('#inspector .tp-copymenu');
  await expect(menu).toBeHidden();
  const btn = page.locator('#inspector-copy');
  await btn.click();
  await expect(btn).toHaveAttribute('aria-expanded', 'true');
  await expect(menu.getByRole('menuitem')).toHaveCount(5);
  const acts = await page.locator('#inspector .inspector-acts').boundingBox();
  const m = await menu.boundingBox();
  expect(m!.y).toBeGreaterThan(acts!.y + acts!.height - 1); // under the actions, not over them.
  await menu.getByRole('menuitem', { name: /^Copy key/ }).click();
  expect(await page.evaluate(() => navigator.clipboard.readText())).toBe('ord_8905');
  // Copying closes the menu; a click on the button while it is still closing would toggle it
  // shut again, so wait until it is gone before opening it a second time.
  await expect(menu).toBeHidden();
  await btn.click();
  await expect(menu).toBeVisible();
  await menu.getByRole('menuitem', { name: 'Copy as kafka-console-producer' }).focus();
  await page.keyboard.press('Escape');
  await expect(menu).toBeHidden();
  await expect(page.locator('#inspector')).toBeVisible(); // the first Esc closed the menu, not the inspector.
  await btn.click();
  await page.locator('#inspector .inspector-h').click(); // a click elsewhere closes it too.
  await expect(menu).toBeHidden();
});

test('the console-producer item is a command where the tool can say the record, and a reason where it cannot', async ({ page, context }) => {
  await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  await page.goto('topics/seed-orders?partition=0&open=5');
  await page.locator('#inspector-copy').click();
  const item = page.getByRole('menuitem', { name: 'Copy as kafka-console-producer' });
  await expect(item).toBeEnabled();
  await item.click();
  const cmd = await page.evaluate(() => navigator.clipboard.readText());
  expect(cmd).toMatch(/^printf '.*\\t.*\\n' \| kafka-console-producer\.sh --bootstrap-server '.+' --topic 'seed-orders' --property parse\.key=true --property parse\.headers=true$/);
  expect(cmd).toContain('traceparent:');

  await page.goto('topics/seed-orders?partition=0&open=41'); // a tombstone
  await page.locator('#inspector-copy').click();
  const none = page.getByRole('menuitem', { name: 'Copy as kafka-console-producer' });
  await expect(none).toBeDisabled();
  await expect(none).toHaveAttribute('title', /tombstone/);
});

test('with a record open the table gives up Time and Hdr, and gets them back when it closes', async ({ page }) => {
  await page.goto('topics/seed-orders?partition=0');
  await expect(page.locator('#messages-table thead th:visible')).toHaveCount(6);
  await page.locator('#msg-5').click();
  await expect(page.locator('#inspector-title')).toBeVisible();
  await expect(page.locator('#messages-table thead th:visible')).toHaveText(['Offset', 'P', 'Key', 'Value']);
  await expect(page.locator('#inspector-panel')).toHaveCSS('width', '400px');
  await page.keyboard.press('Escape');
  await expect(page.locator('#messages-table thead th:visible')).toHaveCount(6);
});

test('a record without a trace id says there is nothing to follow', async ({ page }) => {
  await page.goto('topics/seed-orders?partition=0&open=40');
  await expect(page.locator('#inspector')).toContainText('carries no trace id');
  await expect(page.getByRole('link', { name: 'Follow this message' })).toHaveCount(0);
});
