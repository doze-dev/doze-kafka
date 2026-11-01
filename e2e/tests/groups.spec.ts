import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { test, expect } from '../fixtures/console';
import { kafkaAddr } from '../addr';
import { BASE_URL } from '../playwright.config';

// The seed gives each test that changes a group a group of its own (see seed/main.go).
// Every one has offsets on seed-orders: partition 0 committed at 10 of 45, partition 1 at 2 of 6.
// A group's page has four tabs (Gr-02): Lag, Members, Timeline and Offsets; the offsets
// can be changed on Offsets, and what a form answers is the same tab.
const cells = (row: any) => row.locator('td');
const offsetRows = (page: any) => page.locator('#offsets-table tbody tr');

test('a group shows its offsets and lag, linked to the topic', async ({ page }) => {
  await page.goto('groups/g-lag');
  await expect(page.locator('.idband h1')).toHaveText('g-lag');
  await expect(page.locator('#group-facts')).toContainText('39 records behind'); // (45-10) + (6-2).
  const first = page.locator('#lag-table .r').nth(1); // the header is the first row.
  await expect(first.locator('a')).toHaveText('seed-orders');
  await expect(first).toContainText('10');
  await expect(first).toContainText('45');
  await expect(page.locator('#group-live')).toContainText('No members');
  await first.locator('a').click();
  await expect(page.locator('.idband h1')).toHaveText('seed-orders');
});

test('the group list shows its lag and state, and a group with an odd name is reachable', async ({ page }) => {
  await page.goto('groups');
  const row = page.locator('.list .li[data-name="g-lag"]');
  await expect(row).toContainText('39');
  await expect(row.locator('.dot')).toHaveAttribute('title', 'Empty');
  await expect(page.locator('main h1')).toHaveText('Groups'); // nothing selected: the home page of groups.
  await row.click();
  await expect(page.locator('.idband h1')).toHaveText('g-lag');
});

// Gr-06: the Offsets tab of an Empty group is the reset form and its preview. The preview sends
// nothing; Apply asks first, then moves every partition of the scope.
const apply = (page: any) => page.getByRole('button', { name: /^Apply: reset/ });

test('the reset form previews each mode and sends nothing; Apply asks, then moves every offset', async ({ page, confirmDialog, waitForToast }) => {
  await page.goto('groups/g-reset-earliest?tab=offsets');
  await expect(page.locator('#reset-count')).toHaveText('2 partitions');
  await expect(page.locator('#reset-table .r:not(.h):not(.totalrow)').first()).toContainText('→ 0');
  // Another mode: the preview is asked for (POST .../reset/preview) and the offsets stay.
  const preview = page.waitForResponse((r: any) => r.url().includes('/groups/g-reset-earliest/reset/preview') && r.request().method() === 'POST');
  await page.locator('#reset-form input[value=latest]').check();
  expect((await preview).status()).toBe(200);
  await expect(page.locator('#reset-table .r:not(.h):not(.totalrow)').first()).toContainText('→ 45');
  await expect(page.locator('#reset-preview')).toContainText('Nothing has changed yet');
  await expect(cells(offsetRows(page).first()).nth(2)).toHaveText('10'); // the group still stands where it stood.
  await page.locator('#reset-form input[value=earliest]').check();
  await expect(page.locator('#reset-table .r:not(.h):not(.totalrow)').first()).toContainText('→ 0');

  await apply(page).click();
  await expect(page.locator('#confirm')).toContainText('first record'.length ? 'earliest retained offset' : '');
  await confirmDialog('accept');
  expect(await waitForToast()).toContain('Reset 2 offsets to earliest');
  await expect(cells(offsetRows(page).first()).nth(2)).toHaveText('0');

  await page.goto('groups/g-reset-latest?tab=offsets');
  await page.locator('#reset-form input[value=latest]').check();
  await apply(page).click();
  await confirmDialog('cancel'); // declining changes nothing.
  await expect(cells(offsetRows(page).first()).nth(2)).toHaveText('10');
  await apply(page).click();
  await confirmDialog('accept');
  await expect(cells(offsetRows(page).first()).nth(2)).toHaveText('45');
});

test('a reset to a time goes to the first record at or after it, and one to specific offsets moves only the rows typed', async ({ page, confirmDialog, waitForToast }) => {
  await page.goto('groups/g-set-offset?tab=offsets');
  // Every record of the seed is newer than a day ago: the first at or after that is the log start.
  await page.locator('#reset-at').fill('24h');
  await expect(page.locator('#reset-table .r:not(.h):not(.totalrow)').first()).toContainText('→ 0');
  await expect(page.locator('#reset-preview .note').last()).toContainText('Going to');
  await expect(page.locator('#reset-form input[type=radio][value=timestamp]')).toBeChecked();
  await apply(page).click();
  await expect(page.locator('#confirm')).toContainText('first record at or after');
  await confirmDialog('accept');
  expect(await waitForToast()).toContain('Reset 2 offsets to');
  await expect(cells(offsetRows(page).first()).nth(2)).toHaveText('0');

  // Specific offsets: the preview's After column is a field per partition; a box left empty stays.
  await page.locator('#reset-form input[value=offsets]').check();
  const box = page.locator('#reset-table input.rst-off').first();
  await expect(box).toBeVisible();
  await box.fill('3');
  await expect(page.locator('#reset-table [data-lag-after]').first()).toHaveText('42'); // 45 - 3, from the log end the row carries.
  await apply(page).click();
  await expect(page.locator('#confirm')).toContainText('Set 1 offset');
  await confirmDialog('accept');
  await expect(cells(offsetRows(page).first()).nth(2)).toHaveText('3');
  await expect(cells(offsetRows(page).nth(1)).nth(2)).toHaveText('0'); // the other row was not typed in.
});

// Gr-07: a group with members is refused, and the page says what to do. The refusal is the
// console's rule (no Kafka error is named), and the offsets stay.
test.describe('a group with a member', () => {
  test.describe.configure({ mode: 'serial' });
  const TOPIC = 'gr07-hold-topic', GROUP = 'gr07-hold-group';
  let hold: ChildProcess | undefined;
  test.beforeAll(async () => {
    await fetch(new URL('new/topic', BASE_URL), { method: 'POST', body: new URLSearchParams({ name: TOPIC, partitions: '1' }) });
    execFileSync('go', ['build', '-o', `e2e/.tmp/bin/hold-gr07-${process.pid}`, './e2e/hold'], { cwd: '..', env: { ...process.env, GOWORK: 'off' } });
    hold = spawn(`e2e/.tmp/bin/hold-gr07-${process.pid}`, [kafkaAddr(), 'depot-router', GROUP, TOPIC], { cwd: '..', stdio: ['ignore', 'pipe', 'inherit'] });
    await new Promise<void>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('the held consumer never joined its group')), 20_000);
      hold!.stdout!.on('data', (b) => { if (String(b).includes('ready')) { clearTimeout(t); resolve(); } });
      hold!.on('exit', () => reject(new Error('the held consumer exited')));
    });
  });
  test.afterAll(() => { hold?.kill('SIGTERM'); });

  test('the Offsets tab is the refusal and the way out, not a form', async ({ page }) => {
    await page.goto(`groups/${GROUP}?tab=offsets`);
    const refusal = page.locator('#reset-result .refusal');
    await expect(refusal).toContainText('Refused');
    await expect(refusal).toContainText('The group has 1 active member. Offsets can only be reset or changed while a group has no members.');
    await expect(refusal).not.toHaveAttribute('data-refusal-name', /.+/); // the console's rule, no Kafka error name.
    await expect(page.locator('#reset-steps')).toContainText('Stop the 1 consumer that uses this group');
    await expect(page.locator('#reset-steps')).toContainText('depot-router');
    await expect(page.locator('#reset-form')).toHaveCount(0);
    await page.getByRole('link', { name: 'Show the 1 member' }).click();
    await expect(page).toHaveURL(/tab=members/);
  });

  test('a reset or a preview sent to it is answered with the same refusal, in place', async ({ request }) => {
    for (const path of ['reset', 'reset/preview']) {
      const r = await request.post(`groups/${GROUP}/${path}`, { form: { to: 'earliest' } });
      expect(r.status()).toBe(400);
      expect(r.headers()['x-doze-refusal']).toBe('reset');
      expect(await r.text()).toContain('What to do');
    }
  });
});

// A confirmation is for the button that asked. When a live refresh replaced the
// region while the dialog was open, "Confirm" did nothing.
test('confirming after the page refreshed underneath the dialog still does it', async ({ page, request }) => {
  await page.goto('groups/g-delete');
  let resets = 0;
  page.on('request', (r) => { if (r.method() === 'POST' && r.url().includes('/delete')) resets++; });
  await page.getByRole('button', { name: 'More actions' }).click();
  await page.locator('.more .menu').getByRole('button', { name: 'Delete' }).click();
  await expect(page.locator('#confirm')).toBeVisible();
  // Gr-15: the offsets it removes, the records that stay, and Kafka's rule for a group with members.
  await expect(page.locator('#confirm')).toContainText('This removes the group and its 2 committed offsets on seed-orders.');
  await expect(page.locator('#confirm')).toContainText('records it had not read stay in the topic');
  await expect(page.locator('#confirm')).toContainText('Only Empty groups');
  await expect(page.locator('#confirm')).toContainText('NON_EMPTY_GROUP');
  await expect(page.locator('#confirm')).toContainText('DeleteGroups');
  await request.post('topics/seed-orders/produce', { form: { partition: '1', partition_ctx: '0', value: 'moves the lag' } });
  await page.waitForTimeout(4000); // several polls go by.
  await page.locator('#confirm-yes').click();
  await expect(page).toHaveURL(/\/groups$/);
  expect(resets).toBe(1);
  await expect(page.locator('.list .li[data-name="g-delete"]')).toHaveCount(0);
});

test('one partition\'s offset can be set to a number, and a bad one is explained in the row', async ({ page, waitForToast }) => {
  await page.goto('groups/g-set-offset?tab=offsets');
  const row = offsetRows(page).first();
  await row.getByRole('button', { name: /Set the offset/ }).click();
  await row.locator('input[name=offset]').fill('7');
  await row.getByRole('button', { name: 'Set', exact: true }).click();
  expect(await waitForToast()).toContain('partition 0 is now at offset 7');
  await expect(cells(offsetRows(page).first()).nth(2)).toHaveText('7');
  const again = offsetRows(page).first();
  await again.getByRole('button', { name: /Set the offset/ }).click();
  await again.locator('input[name=offset]').fill('99999');
  await again.getByRole('button', { name: 'Set', exact: true }).click();
  await expect(again.locator('[data-err-slot]')).toContainText('is outside partition 0');
  // The console's own rule (GRP-15): the first retained record to the log end; no Kafka error is the reason given.
  await expect(again.locator('[data-err-slot]')).toContainText('between the first retained record (0) and the end of the log (');
  await expect(again.locator('input[name=offset]')).toHaveValue('99999'); // what was typed stays.
  await again.getByRole('button', { name: 'Earliest' }).click();
  await expect(again.locator('input[name=offset]')).toHaveValue('0');
});

test('typing in a row is not wiped by the page refreshing', async ({ page, request }) => {
  await page.goto('groups/g-set-offset?tab=offsets');
  const row = offsetRows(page).nth(1);
  await row.getByRole('button', { name: /Set the offset/ }).click();
  await row.locator('input[name=offset]').fill('3');
  await request.post('topics/seed-orders/produce', { form: { partition: '1', partition_ctx: '0', value: 'refresh me' } });
  await page.waitForTimeout(4500);
  await expect(offsetRows(page).nth(1).locator('input[name=offset]')).toHaveValue('3');
});

test('one partition\'s offset can be removed, and the others stay', async ({ page, confirmDialog, waitForToast }) => {
  await page.goto('groups/g-remove-offset?tab=offsets');
  await expect(offsetRows(page)).toHaveCount(2);
  await offsetRows(page).first().getByRole('button', { name: /Remove the offset/ }).click();
  // Gr-15: what it forgets, what stays, and that the topic is not touched.
  const dialog = page.locator('#confirm');
  await expect(dialog).toContainText('Remove the offset of seed-orders partition 0');
  await expect(dialog).toContainText('read up to 10 on this one partition');
  await expect(dialog).toContainText('The other 1 stay');
  await expect(dialog).toContainText('Nothing in the topic changes');
  await expect(dialog).toContainText('OffsetDelete');
  await confirmDialog('accept');
  expect(await waitForToast()).toContain('Removed the committed offset');
  await expect(offsetRows(page)).toHaveCount(1);
});

test('a reset names its topic, and the Apply button counts its partitions', async ({ page }) => {
  // g-reset-latest has offsets on one topic only: that topic is the scope, with its partitions.
  await page.goto('groups/g-lag?tab=offsets');
  await expect(page.locator('#reset-form select[name=topic] option')).toHaveText(['seed-orders (2 partitions)']);
  await expect(page.locator('#reset-form select[name=topic]')).toHaveValue('seed-orders');
  await expect(apply(page)).toHaveText('Apply: reset 2 partitions');
});

test('the tabs of a group are Lag, Members, Timeline and Offsets, and ] and [ walk them', async ({ page }) => {
  await page.goto('groups/g-lag');
  await expect(page.locator('main .tabs .tab')).toHaveText([/^Lag/, /^Members/, /^Timeline/, /^Offsets/]);
  await expect(page.locator('main .tabs .tab.on')).toHaveText(/^Lag/);
  await page.locator('body').click({ position: { x: 700, y: 500 } });
  await page.keyboard.press(']');
  await expect(page).toHaveURL(/tab=members/);
  await page.keyboard.press('[');
  await expect(page.locator('main .tabs .tab.on')).toHaveText(/^Lag/);
});
