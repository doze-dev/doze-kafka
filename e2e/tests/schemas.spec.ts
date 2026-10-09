import { test, expect } from '../fixtures/console';

// The seed registers seed-events-value (Avro, two versions) and seed-proto-value
// (Protobuf), and writes records encoded with them (see seed/main.go).

// htmx processes content it has just swapped in about 20 ms later; a click before that
// does nothing. A person cannot click that fast, a test can.
const settled = (page: any) => page.waitForFunction(() => !document.querySelector('.htmx-added, .htmx-swapping, .htmx-settling'));

const AVRO = (extra = '') =>
  `{"type":"record","name":"Order","fields":[{"name":"id","type":"long"}${extra}]}`;

test('the rail lists Schemas, and the list shows each subject with its language and version', async ({ page }) => {
  await page.goto('topics');
  await expect(page.locator('.rail a[href$="/schemas"]')).toBeVisible();
  await page.locator('.rail a[href$="/schemas"]').click();
  await expect(page).toHaveURL(/\/schemas$/);
  const events = page.locator('.listpane .li[data-name="seed-events-value"]');
  await expect(events).toContainText('Avro');
  await expect(events).toContainText('v2');
  await expect(page.locator('.listpane .li[data-name="seed-proto-value"]')).toContainText('Protobuf');
  await expect(page.locator('.empty-detail')).toBeVisible(); // "pick a subject".
  await page.locator('.listpane input[type=text]').fill('proto');
  await expect(events).toBeHidden();
});

test('a subject shows its versions and what changed from the one before', async ({ page }) => {
  await page.goto('schemas/seed-events-value');
  await expect(page.locator('.idband h1')).toHaveText('seed-events-value');
  await expect(page.locator('.ptab', { hasText: 'v2' })).toHaveAttribute('aria-current', 'true');
  // The newest version opens on its changes from the first: the added field is a + line.
  await expect(page.getByRole('button', { name: 'Changes from v1' })).toHaveAttribute('aria-pressed', 'true');
  const added = page.locator('.diff .dl.add');
  await expect(added.filter({ hasText: '"note"' }).first()).toBeVisible();
  await expect(page.locator('.diff .dl.del')).toHaveCount(0);
  await page.getByRole('button', { name: 'Schema', exact: true }).click();
  await expect(page.locator('.schema-view .valuebox').last()).toContainText('"kind"');
  await page.locator('.ptab', { hasText: 'v1' }).click();
  await expect(page.locator('.ptab', { hasText: 'v1' })).toHaveAttribute('aria-current', 'true');
  await expect(page.getByRole('button', { name: /Changes from/ })).toHaveCount(0); // nothing before version 1.
});

test('trying a schema says whether it may be registered, and why not', async ({ page, uniqueName }) => {
  const subject = uniqueName('check');
  await page.goto('new/schema');
  await page.locator('#f-subject').fill(subject);
  await page.locator('#f-schema').fill(AVRO());
  await page.getByRole('button', { name: 'Register', exact: true }).click();
  await expect(page.locator('.idband h1')).toHaveText(subject);

  const editor = page.locator('#newver #f-schema');
  await editor.fill(AVRO(`,{"name":"email","type":"string"}`)); // a field with no default: older data cannot be read.
  await settled(page);
  await page.getByRole('button', { name: 'Check compatibility' }).click();
  const result = page.locator('#check-result');
  await expect(result).toContainText('Not compatible');
  await expect(result).toContainText('email');

  await editor.fill(`{"type":"record"`);
  await page.getByRole('button', { name: 'Check compatibility' }).click();
  await expect(result).toContainText('Not a valid schema');

  await editor.fill(AVRO(`,{"name":"email","type":["null","string"],"default":null}`));
  await page.getByRole('button', { name: 'Check compatibility' }).click();
  await expect(result).toContainText('Compatible.');

  await page.locator('#newver').getByRole('button', { name: 'Register', exact: true }).click();
  await expect(page.locator('.ptab', { hasText: 'v2' })).toHaveAttribute('aria-current', 'true');
  await expect(page.locator('.flash')).toContainText('Schema registered');
  await expect(page.locator('.diff .dl.add').filter({ hasText: 'email' }).first()).toBeVisible();
});

test('a subject can be given its own compatibility level, then handed back', async ({ page, uniqueName }) => {
  const subject = uniqueName('level');
  await page.goto('new/schema');
  await page.locator('#f-subject').fill(subject);
  await page.locator('#f-schema').fill(AVRO());
  await page.getByRole('button', { name: 'Register', exact: true }).click();
  await expect(page.locator('.idchips')).toContainText('BACKWARD · default');
  await settled(page);
  await page.locator('.compat-form select').selectOption('NONE');
  await page.locator('.compat-form').getByRole('button', { name: 'Save' }).click();
  await expect(page.locator('.flash')).toContainText('Compatibility updated');
  await expect(page.locator('.idchips')).toContainText('NONE');
  await expect(page.locator('.idchips')).not.toContainText('default');
  // With NONE, a breaking change registers.
  await page.locator('#newver #f-schema').fill(AVRO(`,{"name":"email","type":"string"}`));
  await page.locator('#newver').getByRole('button', { name: 'Register', exact: true }).click();
  await expect(page.locator('.ptab', { hasText: 'v2' })).toHaveAttribute('aria-current', 'true');
  await settled(page);
  await page.locator('.compat-form select').selectOption('default');
  await page.locator('.compat-form').getByRole('button', { name: 'Save' }).click();
  await expect(page.locator('.idchips')).toContainText('BACKWARD · default');
});

test('deleting a subject asks first, and then it is gone', async ({ page, uniqueName, confirmDialog }) => {
  const subject = uniqueName('gone');
  await page.goto('new/schema');
  await page.locator('#f-subject').fill(subject);
  await page.locator('#f-schema').fill(AVRO());
  await page.getByRole('button', { name: 'Register', exact: true }).click();
  await expect(page.locator('.idband h1')).toHaveText(subject);
  await settled(page);
  await page.getByRole('button', { name: 'Delete', exact: true }).click();
  await confirmDialog('cancel');
  await expect(page.locator('.idband h1')).toHaveText(subject);
  await page.getByRole('button', { name: 'Delete', exact: true }).click();
  await confirmDialog('accept');
  await expect(page).toHaveURL(/\/schemas$/);
  await expect(page.locator(`.listpane .li[data-name="${subject}"]`)).toHaveCount(0);
  const gone = await page.goto(`schemas/${subject}`);
  expect(gone?.status()).toBe(404);
  await expect(page.locator('main')).toContainText('no subject named');
});

test('a topic links its schema, and its records are shown as the JSON they mean', async ({ page }) => {
  await page.goto('topics/seed-events');
  await expect(page.locator('.schema-chip')).toContainText('value schema · v2');
  // The table shows the decoded value, not a binary blob.
  const row = page.locator('tr[id^="msg-"]').filter({ hasText: 'shipped' });
  await expect(row.locator('.cell-val')).toContainText('"note"');
  await expect(row.locator('.cell-val')).not.toHaveClass(/binary/);
  await page.locator('#msg-0').click();
  const drawer = page.locator('#drawer');
  await expect(drawer).toContainText('"kind"');
  await expect(drawer).toContainText('"created"');
  await expect(drawer.locator('.schema-tag')).toContainText('Avro · schema');
  await expect(drawer.locator('.schema-tag a')).toContainText('seed-events-value v1');
  // The bytes are still there to read.
  await drawer.getByRole('button', { name: 'Hex', exact: true }).click();
  await expect(drawer.locator('.hexbox')).toContainText('00 00 00 00');
});

test('Protobuf records are decoded too', async ({ page }) => {
  await page.goto('topics/seed-proto');
  await page.locator('#msg-0').click();
  const drawer = page.locator('#drawer');
  await expect(drawer).toContainText('"boiler-1"');
  await expect(drawer).toContainText('71.5');
  await expect(drawer.locator('.schema-tag')).toContainText('Protobuf · schema');
});

test('the composer writes a record encoded with the topic\'s schema, and says when the value does not fit', async ({ page, uniqueName, send, setEditor }) => {
  const topic = uniqueName('enc');
  await page.goto('new/schema');
  await page.locator('#f-subject').fill(`${topic}-value`);
  await page.locator('#f-schema').fill(AVRO());
  await page.getByRole('button', { name: 'Register', exact: true }).click();
  await expect(page.locator('.idband h1')).toHaveText(`${topic}-value`);

  await page.goto('new/topic');
  await page.locator('#f-name').fill(topic);
  await page.getByRole('button', { name: 'Create topic' }).click();
  await expect(page.locator('.idband h1')).toHaveText(topic);
  await expect(page.locator('.schema-chip')).toContainText('value schema');

  await page.locator('#compose-head').click();
  const pick = page.locator('select[name="schema_subject"]');
  await expect(pick).toHaveValue(`${topic}-value`); // the topic's own subject is chosen for you.
  await send('{"id": "not a number"}');
  await expect(page.locator('#compose-body .err')).toBeVisible();
  await expect(page.locator('#compose-body .err')).toContainText('does not fit');
  await setEditor('{"id": 123}');
  await page.getByRole('button', { name: 'Send message' }).click();
  const row = page.locator('tr[id^="msg-"]').first();
  await expect(row.locator('.cell-val')).toContainText('123');
  await expect(row.locator('.cell-val')).not.toHaveClass(/binary/);
  // The stored record is Confluent's wire format: magic byte, schema id, Avro.
  await row.click();
  await page.locator('#drawer').getByRole('button', { name: 'Hex', exact: true }).click();
  await expect(page.locator('#drawer .hexbox')).toContainText('00 00 00 00');
  // Edit a copy brings the JSON back, with the subject chosen again.
  await page.locator('#drawer-edit').click();
  await expect(page.locator('select[name="schema_subject"]')).toHaveValue(`${topic}-value`);
  await expect(page.locator('.dock .CodeMirror')).toContainText('"id"');
});
