import { test, expect } from '../fixtures/console';

// A topic's settings are on its Config tab (Tp-18): the ones the broker acts on as a
// form, then everything else it reports with where each value comes from.

test('a topic is created with its own retention, shown and marked as the topic\'s own', async ({ page, uniqueName }) => {
  const name = uniqueName('own');
  await page.goto('new/topic');
  await page.locator('#f-name').fill(name);
  await page.locator('[id="cfg-retention.ms"]').fill('7d');
  await page.locator('[id="cfg-segment.bytes"]').fill('64MB');
  await page.getByRole('button', { name: 'Create topic' }).click();
  await expect(page.locator('.idband h1')).toHaveText(name);
  await expect(page.locator('#topic-facts')).toContainText('kept 7 d');
  await page.getByRole('link', { name: 'Config' }).click();
  await expect(page.locator('[id="cfg-retention.ms"]')).toHaveValue('7d');
  await expect(page.locator('[id="cfg-segment.bytes"]')).toHaveValue('64MB');
  await expect(page.locator('[id="cfg-retention.bytes"]')).toHaveValue(''); // not this topic's own: it follows the broker.
  await expect(page.locator('[id="cfg-retention.bytes"]')).toHaveAttribute('placeholder', /broker default/);
});

test('settings are changed from the topic page, and a refusal is explained beside the form', async ({ page, createTopic, uniqueName, waitForToast }) => {
  const name = uniqueName('edit');
  await createTopic(name);
  await page.goto(`topics/${name}?tab=config`);
  await page.locator('[id="cfg-retention.ms"]').fill('12h');
  await page.locator('[id="cfg-max.message.bytes"]').fill('1MB');
  await page.locator('[id="cfg-message.timestamp.type"]').selectOption('LogAppendTime');
  await page.locator('#topic-config').getByRole('button', { name: 'Save settings' }).click();
  expect(await waitForToast()).toContain('Topic settings saved');
  await expect(page.locator('[id="cfg-retention.ms"]')).toHaveValue('12h');
  await expect(page.locator('[id="cfg-max.message.bytes"]')).toHaveValue('1MB');

  await page.locator('[id="cfg-segment.bytes"]').fill('10');
  await page.locator('#topic-config').getByRole('button', { name: 'Save settings' }).click();
  await expect(page.locator('#topic-config [data-err-slot]')).toContainText('minimum of 1024');
  await expect(page.locator('[id="cfg-segment.bytes"]')).toHaveValue('10');
  await page.reload();
  await expect(page.locator('[id="cfg-retention.ms"]')).toHaveValue('12h'); // the refused change did not undo the earlier one.
  await expect(page.locator('[id="cfg-segment.bytes"]')).not.toHaveValue('10');
});

test('a topic that is not compacted says nothing about compaction, and the other keys are listed with their source', async ({ page, request, uniqueName }) => {
  const name = uniqueName('compact');
  await request.post('new/topic', { form: { name, partitions: '1' } });
  await page.goto(`topics/${name}?tab=config`);
  await expect(page.locator('#topic-facts')).not.toContainText('compacted');
  await expect(page.locator('#topic-config-others')).toContainText('cleanup.policy');
  await expect(page.locator('#topic-config-others .pill').first()).toHaveText('default');
  await expect(page.locator('#topic-config')).toContainText('Other Kafka topic settings are stored and reported, and change nothing here.');
});
