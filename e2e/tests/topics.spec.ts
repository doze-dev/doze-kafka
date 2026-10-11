import { test, expect } from '../fixtures/console';

test('a topic is created with its partitions and shows them', async ({ page, createTopic, uniqueName }) => {
  const name = uniqueName('made');
  await createTopic(name, 3);
  await expect(page.locator('#topic-facts')).toContainText('3 partitions');
  await expect(page.locator('.ptab')).toHaveCount(3);
  await expect(page.locator('.list .li.on')).toHaveAttribute('data-name', name);
  await expect(page.locator('#flashbar')).toContainText('Topic created');
  expect(page.url()).not.toContain('flash='); // the one-shot banner is not left in the address bar to replay.
});

test('a bad name or partition count is explained beside the form, and nothing is created', async ({ page }) => {
  await page.goto('new/topic');
  await page.locator('#f-name').fill('bad name!');
  await page.getByRole('button', { name: 'Create topic' }).click();
  await expect(page.locator('[data-err-slot]')).toContainText('invalid topic name');
  await expect(page.locator('#f-name')).toHaveValue('bad name!'); // what was typed is still there.
  await page.locator('#f-name').fill('seed-orders');
  await page.getByRole('button', { name: 'Create topic' }).click();
  await expect(page.locator('[data-err-slot]')).toContainText(`a topic named "seed-orders" already exists`);
  await page.locator('#f-name').fill('fine-name');
  await page.locator('#f-partitions').evaluate((el: HTMLInputElement) => { el.type = 'text'; });
  await page.locator('#f-partitions').fill('abc');
  await page.getByRole('button', { name: 'Create topic' }).click();
  await expect(page.locator('[data-err-slot]')).toContainText('not a number of partitions');
  await page.goto('topics');
  await expect(page.locator('.list .li[data-name="fine-name"]')).toHaveCount(0);
});

test('a sent message appears, and the live tape stays one region', async ({ page, createTopic, uniqueName, send, waitForToast, waitForLive }) => {
  await createTopic(uniqueName('live'));
  await send('{"hello":"world"}', { key: 'k1' });
  expect(await waitForToast()).toContain('Sent to partition 0');
  await waitForLive((t) => t.includes('hello'));
  for (const n of [1, 2, 3]) {
    await send(`second-${n}`);
    await waitForLive((t) => t.includes(`second-${n}`));
  }
  // Updates replace the region. They used to nest a copy inside it, so there were
  // two regions, two pollers, and twice the requests.
  await expect(page.locator('#tape-and-table')).toHaveCount(1);
  await expect(page.locator('tr[id^="msg-"]')).toHaveCount(4);
  await expect(page.locator('.counter')).toContainText('0000004');
});

test('Pause stops the tape following the log, and Resume catches up', async ({ page, request, createTopic, uniqueName, waitForLive }) => {
  const name = uniqueName('pause');
  await createTopic(name);
  await page.locator('[data-live-toggle]').click();
  await expect(page.locator('[data-live-toggle]')).toHaveAttribute('aria-pressed', 'true');
  const sent = await request.post(`topics/${name}/produce`, { form: { partition: '0', partition_ctx: '0', value: 'while-paused' } });
  expect(sent.ok()).toBe(true);
  await page.waitForTimeout(3500); // longer than a poll interval.
  await expect(page.locator('tr[id^="msg-"]')).toHaveCount(0);
  await page.locator('[data-live-toggle]').click();
  await waitForLive((t) => t.includes('while-paused'));
});

test('a poll that cannot reach the console is shown as "not updating", once, without toasts', async ({ page, createTopic, uniqueName }) => {
  await createTopic(uniqueName('offline'));
  await page.route('**/tape?*', (r) => r.abort());
  await expect(page.locator('[data-live].disconnected')).toBeVisible({ timeout: 10_000 });
  await expect(page.locator('.toast')).toHaveCount(0);
  await page.unroute('**/tape?*');
  await expect(page.locator('[data-live].disconnected')).toHaveCount(0, { timeout: 10_000 });
});

test('Purge empties a topic but keeps its settings; Delete removes it', async ({ page, createTopic, uniqueName, send, waitForToast, confirmDialog, waitForLive }) => {
  const name = uniqueName('doomed');
  await createTopic(name);
  await page.getByRole('link', { name: 'Config' }).click();
  await page.locator('[id="cfg-retention.ms"]').fill('3d');
  await page.locator('#topic-config').getByRole('button', { name: 'Save settings' }).click();
  await waitForToast();
  await page.getByRole('link', { name: 'Messages' }).click();
  await send('to be purged');
  await waitForLive((t) => t.includes('to be purged'));
  await page.getByRole('button', { name: 'More actions' }).click();
  await page.getByRole('button', { name: 'Purge', exact: true }).click();
  // The page was drawn before the send; the dialog is measured when the button is pressed.
  await expect(page.locator('#confirm-msg')).toContainText('deletes its 1 record');
  await confirmDialog('accept');
  await expect(page.locator('.tape-empty')).toBeVisible();
  await expect(page.locator('#topic-facts')).toContainText('kept 3 d'); // a purge used to drop the topic's own settings.
  await page.getByRole('button', { name: 'More actions' }).click();
  await page.getByRole('button', { name: 'Delete' }).first().click();
  await confirmDialog('accept');
  await expect(page).toHaveURL(/\/topics$/);
  await expect(page.locator(`.list .li[data-name="${name}"]`)).toHaveCount(0);
});

test('moving between topics keeps the list where it was, and nothing moves after it appears', async ({ page }) => {
  await page.goto('topics/seed-orders');
  await page.evaluate(() => {
    (window as any).__shifts = [];
    new PerformanceObserver((l) => { for (const e of l.getEntries() as any[]) if (!e.hadRecentInput) (window as any).__shifts.push(e.value); }).observe({ type: 'layout-shift' });
    const sc = document.querySelector('.list-scroll')!;
    sc.scrollTop = Math.min(20, sc.scrollHeight - sc.clientHeight);
  });
  const before = await page.evaluate(() => document.querySelector('.list-scroll')!.scrollTop);
  for (const name of ['seed-empty', 'seed-orders', 'seed-empty']) {
    // click() without Playwright's own scrolling the row into view: the list's position is what is measured.
    await page.locator(`.list .li[data-name="${name}"]`).evaluate((el: HTMLElement) => el.click());
    await expect(page.locator('.idband h1')).toHaveText(name);
    await expect(page.locator('.list .li.on')).toHaveAttribute('data-name', name);
    await page.waitForTimeout(400);
  }
  expect(await page.evaluate(() => (window as any).__shifts)).toEqual([]);
  // The workspace is replaced (the dock and the inspector are the page's), and the
  // list's scroll position is put back on the list that arrives.
  expect(await page.evaluate(() => document.querySelector('.list-scroll')!.scrollTop)).toBe(before);
  expect(await page.evaluate(() => document.querySelectorAll('[hx-trigger="load"]').length)).toBe(0);
});

test('partitions are added after a warning, the tabs grow, and a bad number is explained in place', async ({ page, createTopic, uniqueName, confirmDialog }) => {
  const name = uniqueName('grow');
  await createTopic(name, 2);
  await expect(page.locator('.ptab')).toHaveCount(2);

  await page.getByRole('link', { name: /Partitions/ }).click();
  await page.locator('#topic-addparts summary').click();
  await page.locator('#f-addparts').evaluate((el: HTMLInputElement) => { el.type = 'text'; }); // past the browser's own check.
  await page.locator('#f-addparts').fill('x');
  await page.locator('#topic-addparts').getByRole('button', { name: 'Add partitions' }).click();
  await confirmDialog('accept');
  await expect(page.locator('#topic-addparts [data-err-slot]')).toContainText('how many partitions to add');
  await expect(page.locator('#partitions-table a.r')).toHaveCount(2);

  await page.locator('#f-addparts').evaluate((el: HTMLInputElement) => { el.type = 'number'; });
  await page.locator('#f-addparts').fill('2');
  await page.locator('#topic-addparts').getByRole('button', { name: 'Add partitions' }).click();
  await expect(page.locator('#confirm')).toContainText('cannot be removed'); // the cost is said before it is paid.
  await confirmDialog('accept');
  await expect(page.locator('#flashbar')).toContainText('Topic now has 4 partitions');
  await expect(page.locator('#topic-facts')).toContainText('4 partitions');
  await expect(page.locator('.ptab')).toHaveCount(4);
});

test('go to time lands on the first message at or after it, and explains an empty or bad answer', async ({ page }) => {
  await page.goto('topics/seed-orders');
  await page.locator('#jump-at').fill('1d');
  await page.locator('form.jump', { has: page.locator('#jump-at') }).getByRole('button', { name: 'Go' }).click();
  await expect(page.locator('.jumpnote')).toContainText('first message at or after');
  await expect(page.locator('tr.hl')).toHaveCount(1); // the landing row is highlighted.

  await page.locator('#jump-at').fill('2999-01-01');
  await page.locator('form.jump', { has: page.locator('#jump-at') }).getByRole('button', { name: 'Go' }).click();
  await expect(page.locator('.jumpnote')).toContainText('No message on this partition is at or after');

  await page.locator('#jump-at').fill('whenever');
  await page.locator('form.jump', { has: page.locator('#jump-at') }).getByRole('button', { name: 'Go' }).click();
  await expect(page.locator('.jumpnote')).toContainText('That is not a time');
});

test('transactions on the tape: aborted and open records are marked, markers are dividers, and committed-only hides them', async ({ page }) => {
  await page.goto('topics/seed-txn');
  const rows = page.locator('#tape-and-table tbody tr');
  await expect(page.locator('.txnbadge-aborted')).toHaveCount(1);
  await expect(page.locator('.txnbadge-open')).toHaveCount(1);
  await expect(page.locator('tr.txnmark')).toHaveCount(2); // one abort, one commit.
  await expect(page.locator('tr.txn-aborted')).toContainText('txn-aborted-record');
  await expect(page.locator('tr.txn-open')).toContainText('txn-open-record');
  await expect(rows.filter({ hasText: 'txn-committed-record' })).not.toHaveClass(/txn-/);

  await page.getByRole('link', { name: 'Committed only' }).click();
  await expect(page.locator('#tape-and-table tbody tr:not(.txnmark)')).toHaveCount(1);
  await expect(page.locator('#tape-and-table')).toContainText('txn-committed-record');
  await expect(page.locator('#tape-and-table')).not.toContainText('txn-aborted-record');
  await expect(page.locator('tr.txnmark')).toHaveCount(0);
  await page.getByRole('link', { name: /show all/ }).click();
  await expect(page.locator('tr.txnmark')).toHaveCount(2);

  // A marker is not a record: it does not open the inspector.
  await page.locator('tr.txnmark').first().click();
  await expect(page.locator('#inspector')).toBeHidden();

  await page.goto('cluster?view=transactions');
  await expect(page.locator('#open-transactions')).toContainText('seed-open');
  await expect(page.locator('#open-transactions')).toContainText('Ongoing');
});
