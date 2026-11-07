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
  const events = page.locator('.list .li[data-name="seed-events-value"]');
  await expect(events).toContainText('Avro');
  await expect(events).toContainText('2');
  await expect(page.locator('.list .li[data-name="seed-proto-value"]')).toContainText('Protobuf');
  await expect(page.locator('main h1')).toHaveText('Schemas'); // nothing selected: the home of schemas.
  await page.locator('.list input[data-filter-target]').fill('proto');
  await expect(events).toBeHidden();
});

test('a subject shows its versions and what changed from the one before', async ({ page }) => {
  await page.goto('schemas/seed-events-value');
  await expect(page.locator('.idband h1')).toHaveText('seed-events-value');
  await expect(page.locator('#versions-table a.r').first()).toContainText('v2');
  await expect(page.locator('#versions-table a.r').first()).toHaveAttribute('aria-current', 'true');
  // Each version says what it changed, in fields, and whether it passes the level against the one before.
  await expect(page.locator('#versions-table a.r').first()).toContainText('note');
  await expect(page.locator('#versions-table a.r').first()).toContainText('v1, BACKWARD ok');
  await expect(page.locator('#versions-table a.r').last()).toContainText('first version');
  await expect(page.locator('main pre.sc-text')).toContainText('"kind"');
  // The diff: the added field is a line of its own.
  await page.getByRole('link', { name: 'Diff v1 to v2' }).click();
  await expect(page).toHaveURL(/\/diff\?from=1&to=2/);
  await expect(page.locator('#diff-table .dl-add').filter({ hasText: '"note"' }).first()).toBeVisible();
  await expect(page.locator('#diff-facts')).toContainText('v1 to v2');
  await page.goBack();
  await page.locator('#versions-table a.r').last().click();
  await expect(page.locator('#versions-table a.r').last()).toHaveAttribute('aria-current', 'true');
  await expect(page.getByRole('link', { name: /^Diff v/ })).toHaveCount(0); // nothing before version 1.
  // The version opens in the inspector.
  await expect(page.locator('#inspector .versionb')).toContainText('Schema id');
});

/** Registers a first version through the register page and lands on the subject. */
async function registerFirst(page: import('@playwright/test').Page, subject: string, schema: string) {
  await page.goto('new/schema');
  await page.locator('#f-subject').fill(subject);
  await page.locator('#f-schema').fill(schema);
  // Register is enabled by a check that passes (Sc-02).
  await expect(page.locator('#reg-acts [data-register]')).toBeDisabled();
  await page.locator('#reg-acts').getByRole('button', { name: 'Check compatibility' }).click();
  await expect(page.locator('#reg-answer')).toContainText('Parses');
  await page.locator('#reg-acts [data-register]').click();
  await expect(page.locator('.idband h1')).toHaveText(subject);
}

test('trying a schema says whether it may be registered, and why not', async ({ page, uniqueName }) => {
  const subject = uniqueName('check');
  await registerFirst(page, subject, AVRO());

  await page.getByRole('link', { name: 'Register new version' }).click();
  await expect(page.locator('main h1')).toHaveText('Register a new version');
  const editor = page.locator('#f-schema');
  await expect(editor).toHaveValue(/"id"/); // it starts from the latest version.
  await editor.fill(AVRO(`,{"name":"email","type":"string"}`)); // a field with no default: older data cannot be read.
  await settled(page);
  await page.getByRole('button', { name: 'Check compatibility' }).click();
  const result = page.locator('#reg-answer');
  await expect(result).toContainText('Not compatible');
  await expect(result).toContainText('READER_FIELD_MISSING_DEFAULT_VALUE');
  // The fix gives the field a default, and the draft then passes.
  await page.locator('#reg-acts').getByRole('button', { name: /^Fix: add "default"/ }).click();
  await expect(editor).toHaveValue(/"default": ""/);
  await page.locator('#reg-acts').getByRole('button', { name: /Check/ }).click();
  await expect(result).toContainText('Compatible');

  await editor.fill(`{"type":"record"`);
  await page.locator('#reg-acts').getByRole('button', { name: /Check/ }).click();
  // The registry's refusal of a schema it cannot read, said as one (A2): its kind and code.
  await expect(result.locator('[data-refusal-kind="refused"][data-refusal-source="registry"][data-refusal-name="42201"]')).toBeVisible();

  // Registering a breaking change anyway: refused where the answer is, nothing stored (Sc-04).
  await editor.fill(AVRO(`,{"name":"email","type":"string"}`));
  await page.locator('#reg-acts [data-register]').click();
  await expect(result).toContainText('409 Conflict');
  await expect(page.locator('#reg-acts')).toContainText('Nothing was stored');

  await editor.fill(AVRO(`,{"name":"email","type":["null","string"],"default":null}`));
  await expect(page.locator('#reg-acts [data-register]')).toBeDisabled(); // off after a refusal, until a check passes.
  // The row is Fix and Register (Sc-04); ⌘↵ checks from the editor.
  await editor.press('ControlOrMeta+Enter');
  await expect(result).toContainText('Compatible');
  await page.locator('#reg-acts [data-register]').click();
  await expect(page.locator('#versions-table a.r', { hasText: 'v2' })).toHaveAttribute('aria-current', 'true');
  await expect(page.locator('.flash')).toContainText('Schema registered');
  await expect(page.locator('#versions-table a.r').first()).toContainText('added optional email');
});

test('a subject can be given its own compatibility level, then handed back', async ({ page, uniqueName }) => {
  const subject = uniqueName('level');
  await registerFirst(page, subject, AVRO());
  await expect(page.locator('#subject-facts')).toContainText('level BACKWARD (global)');
  await page.getByRole('link', { name: 'Compatibility', exact: true }).click();
  await settled(page);
  await page.locator('.compat-form select').selectOption('NONE');
  await page.locator('.compat-form').getByRole('button', { name: 'Save' }).click();
  await expect(page.locator('.flash')).toContainText('Compatibility updated');
  await expect(page.locator('#subject-facts')).toContainText('level NONE');
  await expect(page.locator('#subject-facts')).not.toContainText('global');
  // With NONE, a breaking change registers.
  await page.getByRole('link', { name: 'Register new version' }).click();
  await page.locator('#f-schema').fill(AVRO(`,{"name":"email","type":"string"}`));
  await page.locator('#reg-acts').getByRole('button', { name: 'Check compatibility' }).click();
  await expect(page.locator('#reg-answer')).toContainText('Compatible');
  await page.locator('#reg-acts [data-register]').click();
  await expect(page.locator('#versions-table a.r', { hasText: 'v2' })).toHaveAttribute('aria-current', 'true');
  await settled(page);
  await page.getByRole('link', { name: 'Compatibility', exact: true }).click();
  await settled(page);
  await page.locator('.compat-form select').selectOption('default');
  await page.locator('.compat-form').getByRole('button', { name: 'Save' }).click();
  await expect(page.locator('#subject-facts')).toContainText('level BACKWARD (global)');
});

test('deleting a subject asks first, and then it is gone', async ({ page, uniqueName, confirmDialog }) => {
  const subject = uniqueName('gone');
  await registerFirst(page, subject, AVRO());
  await settled(page);
  await page.getByRole('button', { name: 'More actions' }).click();
  await page.getByRole('button', { name: 'Delete subject' }).click();
  await confirmDialog('cancel');
  await expect(page.locator('.idband h1')).toHaveText(subject);
  await page.getByRole('button', { name: 'More actions' }).click();
  await page.getByRole('button', { name: 'Delete subject' }).click();
  await confirmDialog('accept');
  await expect(page).toHaveURL(/\/schemas$/);
  await expect(page.locator(`.list .li[data-name="${subject}"]`)).toHaveCount(0);
  const gone = await page.goto(`schemas/${subject}`);
  expect(gone?.status()).toBe(404);
  await expect(page.locator('main')).toContainText('no subject named');
});

test('a topic links its schema, and its records are shown as the JSON they mean', async ({ page }) => {
  await page.goto('topics/seed-events');
  await expect(page.locator('#topic-facts')).toContainText('an Avro value with schema seed-events-value v2'); // the newest record's
  // The table shows the decoded value, not a binary blob.
  const row = page.locator('tr[id^="msg-"]').filter({ hasText: 'shipped' });
  await expect(row.locator('.cell-val')).toContainText('"note"');
  await expect(row.locator('.cell-val')).not.toHaveClass(/binary/);
  await page.locator('#msg-0').click();
  const inspector = page.locator('#inspector');
  await expect(inspector).toContainText('"kind"');
  await expect(inspector).toContainText('"created"');
  await expect(inspector.locator('.kv')).toContainText('Avro · schema');
  await expect(inspector.locator('.kv a')).toContainText('seed-events-value v1');
  // The bytes are still there to read.
  await inspector.getByRole('button', { name: 'Raw', exact: true }).click();
  await expect(inspector.locator('.hexbox')).toContainText('00 00 00 00');
});

test('Protobuf records are decoded too', async ({ page }) => {
  await page.goto('topics/seed-proto');
  await page.locator('#msg-0').click();
  const inspector = page.locator('#inspector');
  await expect(inspector).toContainText('"boiler-1"');
  await expect(inspector).toContainText('71.5');
  await expect(inspector.locator('.kv')).toContainText('Protobuf · schema');
});

test('the composer writes a record encoded with the topic\'s schema, and says when the value does not fit', async ({ page, uniqueName, send, setEditor }) => {
  const topic = uniqueName('enc');
  await registerFirst(page, `${topic}-value`, AVRO());

  await page.goto('new/topic');
  await page.locator('#f-name').fill(topic);
  await page.locator('#create-topic').getByRole('button', { name: 'Create topic' }).click();
  await expect(page.locator('.idband h1')).toHaveText(topic);
  // Nothing is written yet: a subject named like the topic says nothing about its records.
  await expect(page.locator('#topic-facts')).not.toContainText('schema');

  await page.locator('#compose-head').click();
  const pick = page.locator('#c-sub');
  await expect(pick).toHaveValue(`${topic}-value`); // the topic's own subject is chosen for you.
  await send('{"id": "not a number"}');
  await expect(page.locator('#compose-body [data-err-slot]')).toBeVisible();
  await expect(page.locator('#compose-body [data-err-slot]')).toContainText('does not fit');
  await setEditor('{"id": 123}');
  await page.getByRole('button', { name: 'Send message' }).click();
  const row = page.locator('tr[id^="msg-"]').first();
  await expect(row.locator('.cell-val')).toContainText('123');
  await expect(row.locator('.cell-val')).not.toHaveClass(/binary/);
  // The stored record is Confluent's wire format: magic byte, schema id, Avro.
  await row.click();
  await page.locator('#inspector').getByRole('button', { name: 'Raw', exact: true }).click();
  await expect(page.locator('#inspector .hexbox')).toContainText('00 00 00 00');
  // Edit a copy brings the JSON back, with the subject chosen again.
  await page.locator('#inspector-edit').click();
  await expect(page.locator('#c-sub')).toHaveValue(`${topic}-value`);
  await expect(page.locator('#f-value')).toHaveValue(/"id"/);
});
