import { test, expect } from '../fixtures/console';

// Tp-16 to Tp-30, the functional boards (slice 4). Each test makes its own topic, so none waits on
// another's data; the seeded seed-txn (an open transaction held by seed-open) is read, never written.
// No test waits on time: a page that polls is asserted with expect's retries.

async function topic(request: any, name: string, extra: Record<string, string> = {}, partitions = '1') {
  const r = await request.post('new/topic', { form: { name, partitions, ...extra }, maxRedirects: 0 });
  expect([200, 303]).toContain(r.status());
}
async function send(request: any, name: string, form: Record<string, string>) {
  const r = await request.post(`topics/${name}/produce`, { form: { partition: '0', partition_ctx: '0', ...form } });
  expect(r.status(), await r.text()).toBe(200);
}

test('Tp-16: the Partitions tab shows leader, offsets, size, segments, the active segment, and names the open transaction holding the last stable offset', async ({ page }) => {
  await page.goto('topics/seed-txn?tab=partitions');
  const head = page.locator('#partitions-table .r.h');
  for (const h of ['Part.', 'Leader', 'Start', 'End', 'High mark', 'Last stable', 'Size', 'Segments', 'Active segment', 'Newest write']) await expect(head).toContainText(h);
  const row = page.locator('#partitions-table a.r').first();
  await expect(row).toContainText('p0');
  await expect(row).toContainText('broker');
  await expect(row).toContainText('.log ·'); // the active segment's file and its size.
  await expect(page.locator('#partitions-table')).toContainText('All');
  const why = page.locator('#held-p0');
  await expect(why).toContainText("Why p0's last stable offset is behind its end");
  await expect(why).toContainText('seed-open');
  await expect(why).toContainText('has one open on p0 from');
  await expect(why.getByRole('link', { name: 'the committed-only view' })).toHaveAttribute('href', /partition=0&committed=1/);
  await expect(why.getByRole('link', { name: 'the open transactions' })).toHaveAttribute('href', /cluster\?view=transactions/);
  await expect(page.locator('.callout', { hasText: 'Segments' })).toContainText('segment.bytes');
});

test('Tp-17: add partitions asks the total, previews the new ones, warns that keyed ordering changes, and shows the broker\'s two refusals', async ({ page, request, uniqueName, confirmDialog }) => {
  const name = uniqueName('grow');
  await topic(request, name, {}, '3');
  await page.goto(`topics/${name}?tab=partitions&add=1`);
  const box = page.locator('#topic-addparts');
  await expect(box.getByLabel('Total partitions')).toHaveValue('4');
  await box.getByLabel('Total partitions').fill('6');
  const preview = box.locator('#addparts-preview');
  await expect(preview).toContainText('now 3, so p3, p4 and p5 are created empty');
  await expect(preview).toContainText('Keyed ordering changes');
  await expect(preview).toContainText('With 6, a key can land on a different partition than before');
  await expect(preview).toContainText('Who picks them up');
  await expect(box.getByRole('button', { name: 'Add 3 partitions' })).toBeVisible();

  for (const [total, words] of [['2', 'fewer'], ['3', 'already has 3 partitions']] as const) {
    await box.getByLabel('Total partitions').fill(total);
    await expect(preview).toContainText('Send it and the broker answers');
    await box.getByRole('button', { name: 'Add partitions' }).click();
    await confirmDialog('accept');
    const err = box.locator('[data-err-slot]');
    await expect(err).toContainText('INVALID_PARTITIONS');
    await expect(err).toContainText(words);
  }
  await box.getByLabel('Total partitions').fill('5');
  await box.getByRole('button', { name: 'Add 2 partitions' }).click();
  await confirmDialog('accept');
  await expect(page.locator('#partitions-table a.r')).toHaveCount(5);
  await expect(page.locator('#partitions-table a.r').nth(4)).toContainText('p4');
});

test('Tp-18: the Config tab lists every key the broker reports, what acts and what is only kept, with filters', async ({ page, request, uniqueName }) => {
  const name = uniqueName('cfg');
  await topic(request, name);
  await page.goto(`topics/${name}?tab=config`);
  await expect(page.locator('#topic-config-head')).toContainText('all 33');
  await expect(page.locator('#topic-config-head')).toContainText('overrides 0');
  await expect(page.locator('#topic-config-head')).toContainText('compaction 4');
  await expect(page.locator('#topic-config-others [id^="cfg-row-"]')).toHaveCount(33);
  const acts = page.locator('[id="cfg-row-retention.ms"]');
  await expect(acts).toContainText('604800000'.length ? 'acts' : '');
  await expect(page.locator('[id="cfg-row-min.cleanable.dirty.ratio"]')).toContainText('acts');
  await expect(page.locator('[id="cfg-row-min.cleanable.dirty.ratio"]')).toContainText('on a compacted topic');
  await expect(page.locator('[id="cfg-row-flush.messages"]')).toContainText('kept');
  await expect(page.locator('[id="cfg-row-flush.messages"]')).toContainText('it changes nothing here');
  await expect(page.locator('#topic-config-head')).toContainText('stored and returned and change nothing');
  await page.getByRole('link', { name: 'compaction 4' }).click();
  await expect(page.locator('#topic-config-others [id^="cfg-row-"]')).toHaveCount(4);
  await expect(page.locator('[id="cfg-row-delete.retention.ms"]')).toBeVisible();
});

test('Tp-19: edit a key with Before, After and what it affects, save it as an override, and reset it', async ({ page, request, uniqueName }) => {
  const name = uniqueName('edit');
  await topic(request, name);
  await page.goto(`topics/${name}?tab=config`);
  const editLink = page.getByRole('link', { name: 'Edit retention.ms' });
  await expect(editLink).toHaveAttribute('href', /tab=config&edit=retention\.ms/);
  await page.goto((await editLink.getAttribute('href'))!); // a fresh page: htmx is bound to the field before it is typed into.
  const edit = page.locator('#cfg-edit');
  await expect(edit).toContainText('Type milliseconds or use');
  await edit.getByLabel('retention.ms =').fill('1d');
  await expect(edit.locator('#cfg-preview')).toContainText('86400000');
  await expect(edit.locator('#cfg-preview')).toContainText('(1d)');
  await expect(edit.locator('#cfg-preview')).toContainText('source: override on this topic');
  await expect(edit.locator('#cfg-preview')).toContainText('Only closed segments whose newest record is older than 1d are deleted');
  await expect(edit.locator('#cfg-preview')).toContainText('The active segment is never deleted');
  await expect(edit.locator('#cfg-call')).toHaveText('sends IncrementalAlterConfigs SET retention.ms=86400000');
  await edit.getByRole('button', { name: 'Save override' }).click();
  const row = page.locator('[id="cfg-row-retention.ms"]');
  await expect(row).toContainText('86400000');
  await expect(row.locator('.pill')).toHaveText('override');
  await expect(page.locator('#topic-config-head')).toContainText('overrides 1');
  await page.getByRole('link', { name: 'Edit retention.ms' }).click();
  await page.locator('#cfg-edit').getByRole('button', { name: 'Reset to default' }).click();
  await expect(page.locator('[id="cfg-row-retention.ms"] .pill')).toHaveText('default');
});

test('Tp-20: a refused change shows the broker\'s words, the valid range, and the nearest key for a name it does not know', async ({ page, request, uniqueName }) => {
  const name = uniqueName('bad');
  await topic(request, name);
  await page.goto(`topics/${name}?tab=config&edit=segment.bytes`);
  const edit = page.locator('#cfg-edit');
  await edit.getByLabel('segment.bytes =').fill('512');
  await edit.getByRole('button', { name: 'Save override' }).click();
  await expect(edit.locator('[data-err-slot]')).toContainText('INVALID_CONFIG');
  await expect(edit.locator('[data-err-slot]')).toContainText('Valid range: 1024 or more');
  await page.goto(`topics/${name}?tab=config`);
  const free = page.locator('#cfg-free');
  await free.getByLabel('Set another key').fill('retention.mss');
  await free.getByLabel('Value').fill('1d');
  await free.getByRole('button', { name: 'Save override' }).click();
  await expect(free.locator('[data-err-slot]')).toContainText('INVALID_CONFIG');
  await expect(free.locator('[data-err-slot]')).toContainText('unknown topic config');
  await expect(free.locator('[data-err-slot]')).toContainText('Did you mean retention.ms');
  await page.goto(`topics/${name}?tab=config`);
  await expect(page.locator('#topic-config-head')).toContainText('overrides 0'); // nothing was saved.
});

test('Tp-21: a compacted topic carries the cleaner card and says what it cannot know; a plain one does not', async ({ page, request, uniqueName }) => {
  const name = uniqueName('cmp');
  await topic(request, name, { 'cleanup.policy': 'compact' });
  await send(request, name, { key: 'a', value: '1' });
  await page.goto(`topics/${name}`);
  const card = page.locator('#topic-cleaner');
  await expect(card).toContainText('Cleaner');
  await expect(card).toContainText('State');
  await expect(card).toContainText('Last cleaned');
  await expect(card).toContainText('Counts begin again when the broker restarts');
  await expect(card).toContainText('are not something the broker reports');
  await expect(card).toContainText('gaps are normal here, not lost data');
  await card.getByRole('link', { name: 'the compaction keys' }).click();
  await expect(page.locator('#topic-config-others [id^="cfg-row-"]')).toHaveCount(4);
  const plain = uniqueName('plain');
  await topic(request, plain);
  await page.goto(`topics/${plain}`);
  await expect(page.locator('#topic-cleaner')).toHaveCount(0);
});

test('Tp-22: a transaction that holds the last stable offset back is named, with a way to the open transactions', async ({ page }) => {
  await page.goto('topics/seed-txn?partition=0');
  const hold = page.locator('#topic-txnhold');
  await expect(hold).toContainText('Last stable offset is');
  await expect(hold).toContainText('seed-open');
  await expect(hold).toContainText('read_committed reader of p0 waits there');
  await expect(hold).toContainText('there is no page for a single transaction');
  await hold.getByRole('link', { name: 'the Cluster page' }).click();
  await expect(page).toHaveURL(/cluster\?view=transactions/);
  await expect(page.locator('#open-transactions')).toContainText('seed-open');
});

test('Tp-23: the Readers tab lists the groups with lag, links to them, and says which will not move', async ({ page }) => {
  await page.goto('topics/seed-orders?tab=readers');
  const row = page.locator('#readers-table tbody tr', { hasText: 'g-lag' });
  await expect(row.getByRole('link', { name: 'g-lag' })).toHaveAttribute('href', /groups\/g-lag/);
  await expect(page.locator('#readers-table thead')).toContainText('p0 lag');
  await expect(page.locator('#idle-g-lag')).toContainText('has no members and an old commit, so its lag will not move until someone starts it');
  await row.getByRole('link', { name: 'g-lag' }).click();
  await expect(page).toHaveURL(/groups\/g-lag/);
});

test('Tp-26: a paused view counts the records that arrive and leaves the view alone', async ({ page, request, uniqueName }) => {
  const name = uniqueName('pz');
  await topic(request, name);
  for (let i = 0; i < 2; i++) await send(request, name, { value: `v${i}` });
  await page.goto(`topics/${name}?partition=0`);
  await expect(page.locator('#messages-table tbody tr')).toHaveCount(2);
  await page.locator('button.livetoggle').click();
  const banner = page.locator('#topic-paused');
  await expect(banner).toBeVisible();
  await expect(banner).toContainText('Paused.');
  for (let i = 0; i < 3; i++) await send(request, name, { value: `n${i}` });
  await expect(banner.locator('[data-topic-new-n]')).toHaveText('3');
  await expect(page.locator('#messages-table tbody tr')).toHaveCount(2); // new records are counted, not inserted under you.
  await banner.getByRole('button', { name: 'Resume live' }).click();
  await expect(page.locator('#messages-table tbody tr')).toHaveCount(5);
});

test('Tp-27: an empty topic says so, writes its first record from the page and offers the client command', async ({ page, request, uniqueName }) => {
  const name = uniqueName('nil');
  await topic(request, name);
  await page.goto(`topics/${name}`);
  const empty = page.locator('#topic-empty');
  await expect(empty).toContainText('Nothing has been written to this topic yet');
  await expect(empty).toContainText('Start offset 0, end offset 0. This is not a filter and nothing was deleted.');
  await expect(empty).toContainText(`--topic ${name}`);
  await empty.getByLabel('Key').fill('k1');
  await empty.getByLabel('Value').fill('hello');
  await empty.getByRole('button', { name: 'Send record' }).click();
  await expect(page.locator('#messages-table tbody tr')).toHaveCount(1);
  await expect(page.locator('#topic-empty')).toHaveCount(0);
});

test('Tp-28: a search that matches nothing says only the search found nothing, and clears', async ({ page, request, uniqueName }) => {
  const name = uniqueName('flt');
  await topic(request, name);
  for (let i = 0; i < 3; i++) await send(request, name, { key: `HRB-${i}`, value: `v${i}` });
  await page.goto(`topics/${name}?partition=0&q=HRB-2099`);
  const none = page.locator('#search-empty');
  await expect(none).toContainText('Nothing in the records that were read contains');
  await expect(none).toContainText('The topic is not empty, and nothing was deleted: only this search matched nothing.');
  await expect(none).toContainText('matched as text, ignoring case');
  await none.getByRole('link', { name: 'Show all records' }).click();
  await expect(page.locator('#messages-table tbody tr')).toHaveCount(3);
});

test('Tp-29: a topic that is not there offers the nearest names with their sizes, a way to create it', async ({ page, request, uniqueName }) => {
  const base = uniqueName('nf');
  await topic(request, `${base}.orders`, {}, '3');
  await send(request, `${base}.orders`, { value: 'x' });
  const missing = `${base}.orderz`;
  const res = await page.goto(`topics/${missing}`);
  expect(res?.status()).toBe(404);
  await expect(page.locator('#nf-count')).toContainText('topics and none has this name');
  const hit = page.locator('.suggestion', { hasText: `${base}.orders` });
  await expect(hit).toContainText('3 partitions · 1 record');
  await page.locator('#nf-create').getByRole('link', { name: `Create topic ${missing}` }).click();
  await expect(page.locator('#f-name')).toHaveValue(missing);
});

test('Tp-30: a log whose start moved says where, and that no group is behind it', async ({ page, request, uniqueName }) => {
  const name = uniqueName('mv');
  await topic(request, name);
  for (let i = 0; i < 4; i++) await send(request, name, { value: `v${i}` });
  const purged = await request.post(`topics/${name}/purge`);
  expect(purged.status()).toBeLessThan(400);
  await send(request, name, { value: 'after' });
  await page.goto(`topics/${name}?partition=0`);
  const moved = page.locator('#log-moved');
  await expect(moved).toContainText('The log on p0 now starts at offset 4');
  await expect(moved).toContainText('Offsets 0 to 3 are gone');
  await expect(moved).toContainText('does not keep a history of config changes');
  await expect(moved).toContainText('No group has a commit below the start');
});
