import { test, expect } from '../fixtures/console';

// The composer's editor is CodeMirror 6. These tests type into it the way a person
// does; the others set its value from script.

test('typing JSON shows the line a parse fails on, and fixing it clears it', async ({ page, createTopic, uniqueName }) => {
  await createTopic(uniqueName('ed'));
  await page.locator('#compose-head').click();
  const content = page.locator('.dock .cm-content');
  await content.click();
  await page.keyboard.type('{"a": 1,');
  await page.keyboard.press('Enter');
  await page.keyboard.type('"b": ');
  await expect(page.locator('.cm-valid')).toHaveClass(/bad/);
  await expect(page.locator('.cm-valid')).toContainText('line 2');
  await expect(page.locator('.dock .cm-lint-marker-error')).toHaveCount(1);
  await page.keyboard.type('2}');
  await expect(page.locator('.cm-valid')).toContainText('valid JSON');
  await expect(page.locator('.dock .cm-lint-marker-error')).toHaveCount(0);
});

test('brackets and quotes close themselves, and the value reaches the form', async ({ page, createTopic, uniqueName }) => {
  await createTopic(uniqueName('ed'));
  await page.locator('#compose-head').click();
  await page.locator('.dock .cm-content').click();
  await page.keyboard.type('{"k');
  // The closing quote and brace were added.
  await expect(page.locator('.dock .cm-content')).toContainText('{"k"}');
  expect(await page.locator('#f-value').inputValue()).toBe('{"k"}');
});

test('Format prettifies valid JSON and plain text is not an error', async ({ page, createTopic, uniqueName, setEditor }) => {
  await createTopic(uniqueName('ed'));
  await page.locator('#compose-head').click();
  await setEditor('{"a":1,"b":[1,2]}');
  await page.getByRole('button', { name: 'Format' }).click();
  expect(await page.locator('#f-value').inputValue()).toBe('{\n  "a": 1,\n  "b": [\n    1,\n    2\n  ]\n}');
  await setEditor('just some text');
  await expect(page.locator('.cm-valid')).toContainText('plain text');
  await expect(page.locator('.cm-valid')).not.toHaveClass(/bad/);
});

test('Tab leaves the editor instead of trapping the keyboard', async ({ page, createTopic, uniqueName }) => {
  await createTopic(uniqueName('ed'));
  await page.locator('#compose-head').click();
  await page.locator('.dock .cm-content').click();
  await page.keyboard.press('Tab');
  const stillIn = await page.evaluate(() => !!document.activeElement?.closest('.cm-editor'));
  expect(stillIn).toBe(false);
});
