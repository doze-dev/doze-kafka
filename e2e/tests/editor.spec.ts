import { test, expect } from '../fixtures/console';

// The composer's value is a plain textarea (Tp-10): a person types into it, and the note
// under it says what the text is as it is typed.

test('typing JSON says where a parse fails, and fixing it says it is valid', async ({ page, createTopic, uniqueName }) => {
  await createTopic(uniqueName('ed'));
  await page.locator('#compose-head').click();
  const value = page.locator('#f-value');
  await value.click();
  await page.keyboard.type('{"a": 1,');
  await expect(page.locator('#compose-valid')).toHaveClass(/bad/);
  await expect(page.locator('#compose-valid')).toContainText('Not valid JSON');
  await page.keyboard.type('"b": 2}');
  await expect(page.locator('#compose-valid')).toContainText('Valid JSON, 15 B');
  await expect(page.locator('#compose-valid')).not.toHaveClass(/bad/);
  expect(await value.inputValue()).toBe('{"a": 1,"b": 2}');
});

test('Text is sent as typed and is not checked as JSON; Null needs no body', async ({ page, createTopic, uniqueName }) => {
  await createTopic(uniqueName('ed'));
  await page.locator('#compose-head').click();
  await page.locator('#f-value').fill('just some text');
  await expect(page.locator('#compose-valid')).toHaveClass(/bad/); // JSON is the default mode.
  await page.getByRole('button', { name: 'Text', exact: true }).click();
  await expect(page.locator('#compose-valid')).toContainText('Text, 14 B. Sent as typed.');
  await expect(page.locator('#compose-valid')).not.toHaveClass(/bad/);
  await page.getByRole('button', { name: 'Null (tombstone)' }).click();
  await expect(page.locator('#f-value')).toBeDisabled();
});

test('Tab leaves the value instead of trapping the keyboard', async ({ page, createTopic, uniqueName }) => {
  await createTopic(uniqueName('ed'));
  await page.locator('#compose-head').click();
  await page.locator('#f-value').click();
  await page.keyboard.press('Tab');
  expect(await page.evaluate(() => document.activeElement?.id)).not.toBe('f-value');
});
