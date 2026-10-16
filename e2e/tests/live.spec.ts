import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { test, expect } from '../fixtures/console';
import { kafkaAddr } from '../addr';

// A page that says it is live is live (A7, verification of 82b8752 A4 and A5): the
// Groups home's "live" pill and "refreshes every 3 s" are true, the Topics home and
// the list on the left move with the rail's count, and a topic's sentence moves with
// its tape. Nothing here reloads the page: a marker on the document proves it.

async function mark(page: import('@playwright/test').Page) {
  await page.evaluate(() => { (window as any).__marker = 'same-document'; });
}
async function same(page: import('@playwright/test').Page) {
  expect(await page.evaluate(() => (window as any).__marker)).toBe('same-document');
}

test('the Groups home is live: a group that joins appears, in the summary and the list, without a reload', async ({ page, uniqueName }) => {
  const group = uniqueName('live-group');
  await page.goto('groups');
  await expect(page.locator('main .idband .pill')).toHaveText('live');
  await expect(page.locator('#groups-live')).toContainText('refreshes every 3 s');
  // A kind chosen in the filter stays chosen through a poll.
  await page.locator('#groups-live .seg button', { hasText: 'empty' }).click();
  const before = await page.locator('#groups-summary').innerText();
  await mark(page);

  const bin = `e2e/.tmp/bin/hold-live-${process.pid}`;
  execFileSync('go', ['build', '-o', bin, './e2e/hold'], { cwd: '..', env: { ...process.env, GOWORK: 'off' } });
  const hold: ChildProcess = spawn(bin, [kafkaAddr(), `${group}-client`, group, 'seed-orders'], { cwd: '..', stdio: ['ignore', 'pipe', 'inherit'] });
  try {
    await new Promise<void>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('the held consumer never joined its group')), 20_000);
      hold.stdout!.on('data', (b) => { if (String(b).includes('ready')) { clearTimeout(t); resolve(); } });
      hold.on('exit', () => reject(new Error('the held consumer exited')));
    });
    // In the table (hidden by the "empty" filter, which is still chosen), the summary
    // and the list on the left, within a poll or two.
    const row = page.locator(`#groups-table > a.r[data-name="${group}"]`);
    await expect(row).toHaveCount(1, { timeout: 10_000 });
    await expect(page.locator('#groups-live .seg button.on')).toHaveText('empty');
    await expect(row).toBeHidden();
    await expect(page.locator('#groups-summary')).not.toHaveText(before);
    await expect(page.locator(`.list .li[data-name="${group}"]`)).toHaveCount(1, { timeout: 10_000 });
    await same(page);
  } finally {
    hold.kill('SIGTERM');
  }
});

test('the Topics home and the list beside it move with the rail; a topic\'s sentence moves with its tape', async ({ page, request, uniqueName }) => {
  const topic = uniqueName('live-topic');
  expect((await request.post(`new/topic`, { form: { name: topic, partitions: '1' } })).ok()).toBe(true);
  await page.goto('topics');
  const row = page.locator('#topics-table > a.r', { hasText: topic });
  await expect(row.locator('span').nth(2)).toHaveText('0');
  await expect(page.locator(`.list .li[data-name="${topic}"] [data-n]`)).toHaveText('0');
  await mark(page);
  expect((await request.post(`topics/${topic}/produce`, { form: { partition: '0', partition_ctx: '0', value: 'one' } })).ok()).toBe(true);
  await expect(row.locator('span').nth(2)).toHaveText('1', { timeout: 10_000 });
  await expect(page.locator(`.list .li[data-name="${topic}"] [data-n]`)).toHaveText('1', { timeout: 10_000 });
  await same(page);

  // A topic made elsewhere joins the list on its own.
  const other = uniqueName('live-other');
  expect((await request.post(`new/topic`, { form: { name: other, partitions: '1' } })).ok()).toBe(true);
  await expect(page.locator(`.list .li[data-name="${other}"]`)).toHaveCount(1, { timeout: 10_000 });
  await same(page);

  await page.goto(`topics/${topic}`);
  await expect(page.locator('#topic-facts')).toContainText('1 message,');
  await mark(page);
  expect((await request.post(`topics/${topic}/produce`, { form: { partition: '0', partition_ctx: '0', value: 'two' } })).ok()).toBe(true);
  await expect(page.locator('tr[id^="msg-"]')).toHaveCount(2, { timeout: 10_000 });
  await expect(page.locator('#topic-facts')).toContainText('2 messages,');
  await expect(page.locator(`.list .li[data-name="${topic}"] [data-n]`)).toHaveText('2', { timeout: 10_000 });
  await same(page);
});
