import { test, expect } from '../fixtures/console';

test('a topic is created with its own retention, shown and marked as the topic\'s own', async ({ page, uniqueName }) => {
  const name = uniqueName('own');
  await page.goto('new/topic');
  await page.locator('#f-name').fill(name);
  await page.locator('[id="cfg-retention.ms"]').fill('7d');
  await page.locator('[id="cfg-segment.bytes"]').fill('64MB');
  await page.getByRole('button', { name: 'Create topic' }).click();
  await expect(page.locator('.idband h1')).toHaveText(name);
  const summary = page.locator('#topic-config .cfgsum');
  await expect(summary).toContainText('keep records for 7d this topic');
  await expect(summary).toContainText('segment size 64 MB this topic');
  await expect(summary).toContainText('keep at most unlimited');
  await expect(summary).not.toContainText('keep at most unlimited this topic');
});

test('settings are changed from the topic page, and a refusal is explained beside the form', async ({ page, createTopic, uniqueName, waitForToast }) => {
  await createTopic(uniqueName('edit'));
  await page.locator('#topic-config summary').click();
  await page.locator('[id="cfg-retention.ms"]').fill('12h');
  await page.locator('[id="cfg-max.message.bytes"]').fill('1MB');
  await page.locator('[id="cfg-message.timestamp.type"]').selectOption('LogAppendTime');
  await page.locator('#topic-config').getByRole('button', { name: 'Save settings' }).click();
  expect(await waitForToast()).toContain('Topic settings saved');
  await expect(page.locator('#topic-config')).toHaveJSProperty('open', true);
  await expect(page.locator('#topic-config .cfgsum')).toContainText('keep records for 12h this topic');
  await expect(page.locator('#topic-config .cfgsum')).toContainText('largest batch 1 MB this topic');

  await page.locator('[id="cfg-segment.bytes"]').fill('10');
  await page.locator('#topic-config').getByRole('button', { name: 'Save settings' }).click();
  await expect(page.locator('#topic-config [data-err-slot]')).toContainText('minimum of 1024');
  await expect(page.locator('[id="cfg-segment.bytes"]')).toHaveValue('10');
  await page.reload();
  await expect(page.locator('#topic-config .cfgsum')).toContainText('keep records for 12h'); // the refused change did not undo the earlier one.
  await expect(page.locator('#topic-config .cfgsum')).not.toContainText('segment size 10 B');
});

test('a topic that asks for compaction is told it is not compacted', async ({ page, request, uniqueName }) => {
  // The console cannot set cleanup.policy; an admin client would. Altering through
  // the same broker the console shows is how one is set here.
  const name = uniqueName('compact');
  await request.post('new/topic', { form: { name, partitions: '1' } });
  await page.goto(`topics/${name}`);
  await expect(page.locator('#topic-config .cfgsum')).not.toContainText('compaction');
  await page.locator('#topic-config summary').click();
  await expect(page.locator('#topic-config')).toContainText('cleanup.policy');
  await expect(page.locator('#topic-config')).toContainText('Other Kafka topic settings are stored and reported, and change nothing here.');
});
