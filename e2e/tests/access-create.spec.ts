import { test, expect } from '../fixtures/console';
import { OwnBroker } from '../ownbroker';

// Ac-Home, Ac-03 and Ac-05 on a broker of this spec's own (ACLs it makes would change
// what every other spec reads): an ACL made with the form is enforced and listed, its
// row opens what it decides in the inspector, the checker answers by the broker's
// rules, and the keys the strip names do what it says.

const broker = new OwnBroker('access', 30);
test.describe.configure({ mode: 'serial' });
test.beforeAll(async () => { await broker.start(); });
test.afterAll(async () => { await broker.kill(); });

test('Create ACL makes an ACL with the form, the list shows it, its row opens it, and the checker answers', async ({ page, request }) => {
  const C = broker.console;
  expect((await request.post(`${C}new/topic`, { form: { name: 'ledger', partitions: '1' } })).ok()).toBe(true);
  await page.goto(`${C}access`);
  await expect(page.locator('main .callout').first()).toContainText('These are enforced');
  // c is Create ACL.
  await page.keyboard.press('c');
  await expect(page).toHaveURL(/\/new\/acl$/);
  await expect(page.locator('h1')).toHaveText('Create an ACL');
  await page.locator('#ap').fill('User:ANONYMOUS');
  await page.locator('#art').selectOption('topic');
  await page.locator('#arn').fill('ledger');
  await page.locator('#aop').selectOption('write');
  await page.getByRole('button', { name: 'DENY' }).click();
  // The command and what it will do follow the form.
  await expect(page.locator('#acl-command')).toContainText('--deny-principal User:ANONYMOUS');
  await expect(page.locator('#acl-command')).toContainText('--operation Write --topic ledger');
  await expect(page.locator('#acl-effect')).toContainText('is refused Write, with TOPIC_AUTHORIZATION_FAILED');
  await page.getByRole('button', { name: 'Create ACL' }).click();
  await expect(page).toHaveURL(/\/access/);
  await expect(page.locator('#flashbar')).toContainText('ACL created');

  // It is enforced: a produce to the topic is now refused.
  const sent = await request.post(`${C}topics/ledger/produce`, { form: { partition: '0', partition_ctx: '0', value: 'x' } });
  expect(sent.status()).toBe(403);

  // The row opens what it decides; j and Enter do the same; Esc closes it.
  const row = page.locator('#acl-table .r:not(.h)').first();
  await expect(row).toContainText('DENY');
  await page.locator('body').click({ position: { x: 5, y: 5 } });
  await page.keyboard.press('j');
  await page.keyboard.press('Enter');
  await expect(page.locator('#inspector-title')).toHaveText('DENY User:ANONYMOUS Write');
  await expect(page.locator('#acl-inspector')).toContainText('Refuses every request that needs Write on this topic by User:ANONYMOUS');
  await expect(page.locator('#acl-inspector')).toContainText('Refused');
  await page.keyboard.press('Escape');
  await expect(page.locator('#inspector')).toBeHidden();

  // / is the list's filter.
  await page.keyboard.press('/');
  await expect(page.locator('.list input[data-filter-target]')).toBeFocused();
  await page.keyboard.type('ANON');
  await expect(page.locator('.list a.li[data-name]:not([hidden])')).toHaveText([/User:ANONYMOUS/]);
  await page.keyboard.press('Escape');

  // The checker card asks the question and the checker answers it.
  await page.goto(`${C}access`);
  const card = page.locator('#acl-checker');
  await card.locator('input[name="principal"]').fill('User:ANONYMOUS');
  await card.locator('select[name="operation"]').selectOption('write');
  await card.locator('select[name="resource"]').selectOption('topic');
  await card.locator('input[name="name"]').fill('ledger');
  await card.getByRole('button', { name: 'Check' }).click();
  await expect(page.locator('#check-answer')).toContainText('DENIED');
  await expect(page.locator('#check-answer')).toContainText('TOPIC_AUTHORIZATION_FAILED');
  await expect(page.locator('#check-decided')).toContainText('DENY');
  await expect(page.locator('#check-steps')).toContainText('A matching DENY? Yes');
});
