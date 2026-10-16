import { test, expect } from '../fixtures/console';
import { PAGES, STATES } from '../pages';

// A7 and A12's quieter half: no page or state of the console logs an error, throws,
// or has a request of its own refused. Every page of the route table and every state
// the accessibility sweep opens, at 1440, with what the browser reports collected:
// console errors (an htmx selector that finds nothing, a failed fetch), uncaught
// exceptions and unhandled rejections, and any request the page makes that answers
// 4xx or 5xx. A page that is itself an error (a topic that does not exist) answers
// 404 for the document, as it must; the requests it makes after that must not fail.

test('no page or state logs an error, throws, or has a request refused', async ({ page }) => {
  test.setTimeout(300_000);
  const problems: string[] = [];
  let where = '';
  // The browser also logs every failed request as "Failed to load resource"; those are
  // the responses below, where the document's own 404 on an error page is told apart.
  page.on('console', (m) => { if (m.type() === 'error' && !m.text().startsWith('Failed to load resource')) problems.push(`${where}: console error: ${m.text()}`); });
  page.on('pageerror', (e) => problems.push(`${where}: uncaught: ${e.message}`));
  page.on('response', (r) => {
    if (r.status() < 400 || r.request().isNavigationRequest()) return;
    problems.push(`${where}: ${r.request().method()} ${new URL(r.url()).pathname}${new URL(r.url()).search} answered ${r.status()}`);
  });
  for (const path of Object.values(PAGES).flat()) {
    where = path || '(home)';
    await page.goto(path);
    await page.waitForLoadState('load');
    await page.waitForTimeout(600); // the first polls.
  }
  for (const [name, path, go] of STATES) {
    where = `${name} (${path})`;
    await page.goto(path);
    await page.waitForLoadState('load');
    await go(page, 1440);
    await page.waitForTimeout(600);
  }
  expect(problems, problems.join('\n')).toEqual([]);
});

// The forms, driven: what they send is said beside them, and nothing else is logged. A
// refusal is an answer of 4xx by design (A2), so the one the form is meant to get is
// expected; any other is not.
test('the forms log nothing when they are used', async ({ page, createTopic, uniqueName, waitForToast }) => {
  const problems: string[] = [];
  let expected = '';
  page.on('console', (m) => { if (m.type() === 'error' && !m.text().startsWith('Failed to load resource')) problems.push(`console error: ${m.text()}`); });
  page.on('pageerror', (e) => problems.push(`uncaught: ${e.message}`));
  page.on('response', (r) => {
    if (r.status() < 400 || r.request().isNavigationRequest()) return;
    const what = `${r.request().method()} ${new URL(r.url()).pathname}`;
    if (what.endsWith(expected) && expected) return;
    problems.push(`${what} answered ${r.status()}`);
  });
  // Create a topic with a name Kafka refuses, then one it takes.
  await page.goto('new/topic');
  expected = 'POST /_console/new/topic';
  await page.locator('#f-name').fill('bad name!');
  await page.getByRole('button', { name: 'Create topic' }).click();
  await expect(page.locator('[data-err-slot]')).toContainText('invalid topic name');
  expected = '';
  await createTopic(uniqueName('forms'));
  // Send from the closed composer's row.
  await page.locator('#compose-quick').getByLabel('Value').fill('{"a":1}');
  await page.locator('#compose-quick').getByRole('button', { name: 'Send' }).click();
  await waitForToast();
  // Type into the ACL form: its command and what it will do follow.
  await page.goto('new/acl');
  await page.locator('#create-acl input[name="principal"]').fill('User:alice');
  await page.locator('#create-acl input[name="name"]').fill('seed-orders');
  await page.locator('#create-acl .seg button').last().click();
  await expect(page.locator('#acl-command')).toContainText('User:alice');
  // A group's reset: the confirm opens, and is cancelled.
  for (const g of ['g-lag', 'g-reset-latest']) {
    await page.goto(`groups/${g}?tab=offsets`);
    const reset = page.locator('#reset button.btn:not([disabled])').first();
    if (await reset.count()) {
      await reset.click();
      await expect(page.locator('#confirm')).toBeVisible();
      await page.locator('#confirm-no').click();
    }
  }
  await page.waitForTimeout(400);
  expect(problems, problems.join('\n')).toEqual([]);
});
