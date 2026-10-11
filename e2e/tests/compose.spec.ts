import { test, expect } from '../fixtures/console';

test('the composer sends text, JSON, a key and headers', async ({ page, createTopic, uniqueName, setEditor, waitForToast, waitForLive }) => {
  await createTopic(uniqueName('compose'));
  await page.locator('#compose-head').click();
  await page.locator('#f-key').fill('order-1');
  await page.getByRole('button', { name: '+ Add header' }).click();
  await page.getByLabel('Header key').fill('source');
  await page.getByLabel('Header value', { exact: true }).fill('e2e');
  await setEditor('{"id": 1}');
  await expect(page.locator('.cm-valid')).toContainText('valid JSON');
  await page.getByRole('button', { name: 'Send message' }).click();
  await waitForToast();
  await waitForLive((t) => t.includes('order-1'));
  await page.locator('tr[id^="msg-"]').first().click();
  await expect(page.locator('#inspector')).toContainText('source');
  await expect(page.locator('#inspector')).toContainText('e2e');
});

test('invalid JSON is flagged as it is typed, and a null value needs no body', async ({ page, createTopic, uniqueName, setEditor, waitForToast }) => {
  await createTopic(uniqueName('json'));
  await page.locator('#compose-head').click();
  await setEditor('{"id": ');
  await expect(page.locator('.cm-valid')).toHaveClass(/bad/);
  await page.getByLabel('null value (tombstone)').check();
  await expect(page.locator('.cm-valid')).toContainText('null value');
  await page.getByRole('button', { name: 'Send message' }).click();
  expect(await waitForToast()).toContain('Sent to partition 0');
});

test('a bad header is reported next to the composer and nothing is sent', async ({ page, createTopic, uniqueName, setEditor }) => {
  await createTopic(uniqueName('badhdr'));
  await page.locator('#compose-head').click();
  await page.getByRole('button', { name: '+ Add header' }).click();
  await page.getByLabel('Header key').fill('trace');
  await page.getByLabel('Header value', { exact: true }).fill('!!not base64!!');
  await page.getByRole('button', { name: 'Header value is base64' }).click();
  await setEditor('x');
  await page.getByRole('button', { name: 'Send message' }).click();
  await expect(page.locator('.dock [data-err-slot]')).toBeVisible();
  await expect(page.locator('tr[id^="msg-"]')).toHaveCount(0);
});

// Holding the key repeats it, and each repeat used to send a record.
test('holding Cmd+Enter sends once', async ({ page, createTopic, uniqueName, setEditor, waitForLive }) => {
  await createTopic(uniqueName('held'));
  await page.locator('#compose-head').click();
  await setEditor('held');
  await page.locator('.dock .cm-editor').click();
  await page.keyboard.down('Meta');
  for (let i = 0; i < 8; i++) await page.keyboard.down('Enter');
  await page.keyboard.up('Enter');
  await page.keyboard.up('Meta');
  await waitForLive((t) => t.includes('held'));
  await page.waitForTimeout(500);
  await expect(page.locator('tr[id^="msg-"]')).toHaveCount(1);
});

test('the composer stays open across a change of partition and closes on another topic', async ({ page, createTopic, uniqueName }) => {
  const a = uniqueName('stay'), b = uniqueName('other');
  await createTopic(b);
  await createTopic(a, 2);
  await page.locator('#compose-head').click();
  await expect(page.locator('.dock-form')).toBeVisible();
  await page.locator('.ptab', { hasText: 'P1' }).click();
  await expect(page).toHaveURL(/partition=1/);
  await expect(page.locator('.dock-form')).toBeVisible();
  await page.locator(`.listpane .li[data-name="${b}"]`).click();
  await expect(page.locator('.idband h1')).toHaveText(b);
  await expect(page.locator('.dock-form')).toBeHidden();
  await expect(page.locator('#compose-head')).toHaveAttribute('aria-expanded', 'false');
});

test('an open composer never covers the messages', async ({ page }) => {
  for (const height of [900, 700, 560]) {
    await page.setViewportSize({ width: 1280, height });
    await page.goto('topics/seed-orders');
    await page.locator('#compose-head').click();
    const r = await page.evaluate(() => {
      const wb = document.querySelector('.detail.workbench')!;
      wb.scrollTop = wb.scrollHeight;
      const t = document.querySelector('.tbl-wrap')!.getBoundingClientRect(), d = document.querySelector('.dock')!.getBoundingClientRect();
      return { visible: Math.min(t.bottom, d.top) - t.top, overlap: t.bottom - d.top };
    });
    expect(r.visible, `rows visible at ${height}px`).toBeGreaterThan(100);
    expect(r.overlap, `table under the composer at ${height}px`).toBeLessThanOrEqual(1);
  }
});

test('sending to another partition keeps the view and says where it went', async ({ page, createTopic, uniqueName, setEditor, waitForToast }) => {
  await createTopic(uniqueName('part'), 3);
  await page.locator('#compose-head').click();
  await page.locator('#f-partition').selectOption('2');
  await setEditor('elsewhere');
  await page.getByRole('button', { name: 'Send message' }).click();
  const toast = await waitForToast();
  expect(toast).toContain('partition 2');
  expect(toast).toContain('you are viewing partition 0');
  await expect(page.locator('.ptab.on')).toContainText('P0');
  await expect(page.locator('.secrow .sec-title')).toContainText('partition 0');
});
