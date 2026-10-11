import { test, expect } from '../fixtures/console';

// The seed's own requests are the traffic (see seed/main.go): client "doze-seed" created
// topics, produced to seed-orders and was refused a duplicate CreateTopics; client
// "doze-seed-consumer" joined g-timeline, read a record, committed and left.

test('the rail lists Traffic, and the page shows what the seed asked, who asked it and how it went', async ({ page }) => {
  await page.goto('topics');
  await page.locator('.rail .ri', { hasText: 'Traffic' }).click();
  await expect(page).toHaveURL(/\/traffic$/);
  await expect(page.locator('.rail .ri.on')).toHaveText(/Traffic/);
  const table = page.locator('#traffic-table');
  const produce = table.locator('tbody tr', { hasText: 'Produce' }).filter({ hasText: 'seed-orders' }).first();
  await expect(produce).toContainText('doze-seed');
  await expect(produce).toContainText('ok');
  // The refused request carries the broker's own name for the error.
  await expect(table.locator('tbody tr', { hasText: 'CreateTopics' }).filter({ hasText: 'TOPIC_ALREADY_EXISTS' })).toHaveCount(1);
  // The console's own polling is not in its own traffic.
  await expect(table.locator('tbody tr', { hasText: 'doze-kafka-console' })).toHaveCount(0);
});

test('the filters narrow the tail, and the narrowing survives a refresh of it', async ({ page }) => {
  await page.goto('traffic');
  await page.locator('input[name=topic]').fill('seed-orders');
  await page.getByRole('button', { name: 'Filter', exact: true }).click();
  await expect(page).toHaveURL(/topic=seed-orders/);
  const rows = page.locator('#traffic-table tbody tr');
  await expect(rows.first()).toBeVisible();
  for (const row of await rows.all()) await expect(row).toContainText('seed-orders');

  // The region polls with the filter in its address.
  await expect(page.locator('#traffic-live')).toHaveAttribute('data-live', /topic=seed-orders/);
  const polled = page.waitForResponse((r) => r.url().includes('/traffic/live') && r.url().includes('topic=seed-orders'));
  await polled;
  await expect(page.locator('#traffic-live')).toHaveCount(1); // one region, however many polls.

  await page.getByRole('link', { name: 'Clear' }).click();
  await expect(page.locator('input[name=topic]')).toHaveValue('');

  await page.locator('input[name=errors]').check();
  await page.getByRole('button', { name: 'Filter', exact: true }).click();
  // Other specs share this broker and fail requests of their own, so assert on the
  // seed's refusal being there and on nothing that went well being there.
  await expect(page.locator('#traffic-table tbody tr', { hasText: 'TOPIC_ALREADY_EXISTS' })).toHaveCount(1);
  await expect(page.locator('#traffic-table tbody tr:not(:has(.badge))')).toHaveCount(0);

  await page.locator('input[name=errors]').uncheck();
  await page.locator('input[name=client]').fill('nobody-by-this-name');
  await page.getByRole('button', { name: 'Filter', exact: true }).click();
  await expect(page.locator('.honest', { hasText: 'No request matches the filter' })).toBeVisible();
});

test('a row opens its request in the inspector, and Escape closes it', async ({ page }) => {
  await page.goto('traffic?errors=1&client=doze-seed&api=CreateTopics');
  const row = page.locator('#traffic-table tbody tr').first();
  await row.click();
  const inspector = page.locator('#inspector');
  await expect(inspector).toBeVisible();
  await expect(inspector.locator('#inspector-title')).toContainText('CreateTopics');
  await expect(inspector).toContainText('doze-seed');
  await expect(inspector).toContainText('seed-empty');
  await expect(inspector).toContainText('TOPIC_ALREADY_EXISTS');
  await expect(inspector).toContainText(process.env.E2E_SASL_USER ? `User:${process.env.E2E_SASL_USER}` : 'User:ANONYMOUS');
  await page.keyboard.press('Escape');
  await expect(inspector).toBeHidden();
});

test('the live toggle pauses the tail', async ({ page }) => {
  await page.goto('traffic');
  const toggle = page.locator('[data-live-toggle]');
  await expect(toggle).toHaveAttribute('aria-pressed', 'false');
  await toggle.click();
  await expect(toggle).toHaveAttribute('aria-pressed', 'true');
  await toggle.click();
  await expect(toggle).toHaveAttribute('aria-pressed', 'false');
});

test('a group page shows its timeline, newest first', async ({ page }) => {
  await page.goto('groups/g-timeline');
  const timeline = page.locator('#group-timeline');
  await expect(timeline).toBeVisible();
  const kinds = (await timeline.locator('.tl-k').allInnerTexts()).map((s) => s.trim());
  // Newest first: the member left, after the leader assigned, after the generation began, after it joined.
  expect(kinds.indexOf('left')).toBeGreaterThanOrEqual(0);
  expect(kinds.indexOf('left')).toBeLessThan(kinds.indexOf('assigned'));
  expect(kinds.indexOf('assigned')).toBeLessThan(kinds.indexOf('new generation'));
  expect(kinds.indexOf('new generation')).toBeLessThan(kinds.indexOf('joined'));
  await expect(timeline).toContainText('doze-seed-consumer'); // the join names the client.
});

test('Traffic is in the palette', async ({ page, openPalette }) => {
  await page.goto('topics');
  await openPalette();
  await page.keyboard.type('Traffic');
  await expect(page.locator('.pal-item', { hasText: 'Traffic' })).toBeVisible();
});

test('a client that reports its own metrics shows them, drawn, and says they are not traces', async ({ page }) => {
  await page.goto('traffic');
  const card = page.getByRole('region', { name: 'Metrics from doze-seed-telemetry' });
  await expect(card).toBeVisible();
  await expect(page.locator('.sec-title', { hasText: 'Client metrics' })).toContainText('metrics, not traces');
  await expect(card).toContainText('3 reports');
  await expect(card).toContainText('client_rack=seed-rack');
  const latency = card.locator('.mrow', { hasText: 'producer.node.request.latency.avg' });
  await expect(latency).toContainText('node_id=1');
  await expect(latency).toContainText('9.75 ms'); // the last of 4.5, 6.25, 9.75.
  await expect(latency.locator('svg.spark polyline')).toHaveCount(1); // a history to draw.
  // The reports are in the request tail too, folded into one counted row.
  await expect(page.locator('#traffic-table tbody tr', { hasText: 'PushTelemetry' }).filter({ hasText: 'doze-seed-telemetry' })).toHaveCount(1);
});
