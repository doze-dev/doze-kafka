import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { test, expect } from '../fixtures/console';
import type { Locator, Page } from '@playwright/test';
import { kafkaAddr } from '../addr';

// One inspector, six selections (plus a group's events): a message, a request, a
// client, a member, a subject and a transaction. Each is selected, closed with Esc
// (focus goes back to its row), reopened and refreshed (the same selection is
// open), and left with Back (the previous selection, or none).

let hold: ChildProcess | undefined;

test.describe.configure({ mode: 'serial' });

test.beforeAll(async () => {
  // A consumer that stays in its group, so there is a client and a member to look at.
  execFileSync('go', ['build', '-o', `e2e/.tmp/bin/hold-${process.pid}`, './e2e/hold'], { cwd: '..', env: { ...process.env, GOWORK: 'off' } });
  hold = spawn(`e2e/.tmp/bin/hold-${process.pid}`, [kafkaAddr(), 'inspect-hold', 'inspect-hold-group', 'seed-orders'], { cwd: '..', stdio: ['ignore', 'pipe', 'inherit'] });
  await new Promise<void>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('the held consumer never joined its group')), 20_000);
    hold!.stdout!.on('data', (b) => { if (String(b).includes('ready')) { clearTimeout(t); resolve(); } });
    hold!.on('exit', () => reject(new Error('the held consumer exited')));
  });
});

test.afterAll(() => { hold?.kill('SIGTERM'); });

const overlay = (page: Page) => page.locator('#inspector');
const title = (page: Page) => page.locator('#inspector #inspector-title');

/** Opens `row`, then runs the three checks the brief names. */
async function cycle(page: Page, row: Locator, kind: string, opts: { focusBack?: boolean } = {}) {
  await row.click();
  await expect(overlay(page)).toBeVisible();
  await expect(page.locator('#inspector .inspector-h')).toHaveAttribute('data-inspector-kind', kind);
  const open = await title(page).getAttribute('data-open');
  expect(open, 'the selection names its address').toBeTruthy();
  await expect.poll(() => new URL(page.url()).searchParams.get('open')).toBe(open);

  // Esc closes it, takes the address back out, and returns focus to the row.
  await page.keyboard.press('Escape');
  await expect(overlay(page)).toBeHidden();
  expect(page.url()).not.toContain('open=');
  if (opts.focusBack !== false) await expect(row).toBeFocused();

  // Reopened and refreshed: the same selection is open.
  await row.click();
  await expect(title(page)).toHaveAttribute('data-open', open!);
  await page.reload();
  await expect(overlay(page)).toBeVisible();
  await expect(title(page)).toHaveAttribute('data-open', open!);
  await expect(page.locator('#inspector .inspector-h')).toHaveAttribute('data-inspector-kind', kind);

  // Back leaves it (the page it was opened from has no selection).
  await page.goBack();
  await expect(overlay(page)).toBeHidden();
  expect(page.url()).not.toContain('open=');
}

test('a message is selected, closed, refreshed and left with Back', async ({ page }) => {
  await page.goto('topics/seed-orders?partition=0');
  await cycle(page, page.locator('#msg-5'), 'message');
});

test('a request is selected, closed, refreshed and left with Back', async ({ page }) => {
  await page.goto('traffic');
  await cycle(page, page.locator('#traffic-table tbody tr', { hasText: 'Produce' }).filter({ hasText: 'seed-orders' }).first(), 'request');
});

test('a client is selected, closed, refreshed and left with Back', async ({ page }) => {
  await page.goto('traffic');
  const row = page.locator('#clients-list .insp-client', { hasText: 'inspect-hold' }).first().locator('a.insp-client-open');
  await expect(row).toBeVisible();
  await row.click();
  await expect(title(page)).toContainText('inspect-hold');
  await expect(overlay(page)).toContainText('Requests');
  await page.keyboard.press('Escape');
  await cycle(page, row, 'client');
});

// Enter on a link inside the inspector that opens another selection is the link's own:
// the shell's key handling for rows must not cancel it.
test('Enter on the client link inside a request inspector opens the client', async ({ page }) => {
  await page.goto('traffic');
  await page.locator('#traffic-table tbody tr', { hasText: 'inspect-hold' }).first().click();
  await expect(page.locator('#inspector .inspector-h')).toHaveAttribute('data-inspector-kind', 'request');
  const link = page.locator('#inspector a[data-inspector-open]', { hasText: 'inspect-hold' }).first();
  await expect(link).toBeVisible();
  // htmx has swapped the record in and settled it: the link is live.
  await expect(page.locator('#inspector .htmx-added, #inspector .htmx-settling')).toHaveCount(0);
  await link.focus();
  await page.keyboard.press('Enter');
  await expect(page.locator('#inspector .inspector-h')).toHaveAttribute('data-inspector-kind', 'client');
  await expect(title(page)).toContainText('inspect-hold');
});

test('a member is selected, closed, refreshed and left with Back', async ({ page }) => {
  await page.goto('groups/inspect-hold-group?tab=members');
  const row = page.locator('[id^="mb-"]').first();
  await expect(row).toContainText('inspect-hold');
  await cycle(page, row, 'member');
  await row.click();
  await expect(overlay(page)).toContainText('seed-orders');
});

test('a transaction is selected, closed, refreshed and left with Back', async ({ page }) => {
  await page.goto('cluster?view=transactions');
  await cycle(page, page.locator('#tx-seed-open'), 'transaction');
});

test('a group event is selected, closed, refreshed and left with Back', async ({ page }) => {
  await page.goto('groups/g-timeline?tab=timeline');
  await cycle(page, page.locator('#group-timeline .tl-row').first(), 'event');
});

test('a subject is selected from the neighbours sentence, closed, refreshed and left with Back', async ({ page }) => {
  await page.goto('topics/seed-events');
  const link = page.locator('#neighbours a', { hasText: 'seed-events-value' });
  await expect(link).toBeVisible();
  await cycle(page, link, 'subject');
  await link.click();
  await expect(title(page)).toHaveText('seed-events-value');
  await expect(overlay(page)).toContainText('Compatibility');
});

test('Back returns to the previous selection, and then to none', async ({ page }) => {
  await page.goto('topics/seed-orders?partition=0');
  await page.locator('#msg-5').click();
  await expect(title(page)).toHaveAttribute('data-offset', '5');
  await page.getByRole('button', { name: 'Newer record' }).click();
  await expect(title(page)).toHaveAttribute('data-offset', '6');
  await page.goBack();
  await expect(title(page)).toHaveAttribute('data-offset', '5');
  expect(page.url()).toContain('open=5');
  await page.goBack();
  await expect(overlay(page)).toBeHidden();
  expect(page.url()).not.toContain('open=');
  await page.goForward();
  await expect(title(page)).toHaveAttribute('data-offset', '5');
});

test('a selection that has gone says so, and still closes', async ({ page }) => {
  await page.goto('traffic?open=request:no-such-row');
  await expect(overlay(page)).toBeVisible();
  await expect(overlay(page)).toContainText('aged out');
  await page.locator('#inspector [data-inspector-close]').click();
  await expect(overlay(page)).toBeHidden();
});
