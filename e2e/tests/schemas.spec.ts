import { test, expect } from '../fixtures/console';
import { OwnBroker } from '../ownbroker';

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
  // Try it: the bytes a serializer would send for a value, sent nowhere.
  await page.locator('#try-it').getByRole('button', { name: 'Encode' }).click();
  await expect(page.locator('#try-result')).toContainText('magic byte 0');
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
  // Used by (Sc-11): the ids the topic's messages carry, and the newest on one.
  await page.getByRole('link', { name: 'Used by', exact: true }).click();
  await expect(page.locator('#used-table')).toContainText('seed-events');
  await page.locator('#open-id').getByRole('button', { name: /^Open on/ }).click();
  await expect(page).toHaveURL(/\/topics\/seed-events\/message\?partition=\d+&offset=\d+/);
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
  await expect(page.locator('.flash')).toContainText(/Registered v2, id \d+/);
  await expect(page.locator('#versions-table a.r').first()).toContainText('added optional email');
});

test('a subject can be given its own compatibility level, then handed back', async ({ page, uniqueName }) => {
  const subject = uniqueName('level');
  await registerFirst(page, subject, AVRO());
  await expect(page.locator('#subject-facts')).toContainText('level BACKWARD (global)');
  await page.getByRole('link', { name: 'Compatibility', exact: true }).click();
  await settled(page);
  // Sc-09: the levels are a table of choices; j and k choose too.
  await page.getByRole('radio', { name: 'NONE', exact: true }).check();
  await expect(page.locator('#level-body')).toHaveText('{"compatibility":"NONE"}');
  await page.getByRole('button', { name: 'Save level' }).click();
  await expect(page.locator('.flash')).toContainText('Compatibility updated');
  await expect(page.locator('#subject-facts')).toContainText('level NONE, set on this subject');
  // With NONE, a breaking change registers.
  await page.getByRole('link', { name: 'Versions', exact: true }).click();
  await page.getByRole('link', { name: 'Register new version' }).click();
  await page.locator('#f-schema').fill(AVRO(`,{"name":"email","type":"string"}`));
  await page.locator('#reg-acts').getByRole('button', { name: 'Check compatibility' }).click();
  await expect(page.locator('#reg-answer')).toContainText('Compatible');
  await page.locator('#reg-acts [data-register]').click();
  await expect(page.locator('#versions-table a.r', { hasText: 'v2' })).toHaveAttribute('aria-current', 'true');
  await settled(page);
  await page.getByRole('link', { name: 'Compatibility', exact: true }).click();
  await settled(page);
  // Checked at BACKWARD before it is saved, the breaking v2 would fail.
  await page.getByRole('radio', { name: 'BACKWARD', exact: true }).check();
  await page.getByRole('button', { name: 'Test v2 against this level' }).click();
  await expect(page.locator('#level-answer')).toContainText('Would fail');
  await page.getByRole('button', { name: 'Remove it' }).click();
  await expect(page.locator('#subject-facts')).toContainText('level BACKWARD, the global default');
});

test('a read-only subject refuses a new version, and can be opened again', async ({ page, uniqueName }) => {
  const subject = uniqueName('frozen');
  await registerFirst(page, subject, AVRO());
  await page.getByRole('link', { name: 'Compatibility', exact: true }).click();
  await settled(page);
  await page.locator('#compat-form').getByText('READONLY', { exact: true }).click();
  await page.getByRole('button', { name: 'Save level' }).click();
  await expect(page.locator('#subject-facts')).toContainText('mode READONLY, set on this subject');
  await page.goto(`new/schema?subject=${subject}`);
  await expect(page.locator('#reg-answer')).toContainText('This subject is read-only');
  await page.locator('#f-schema').fill(AVRO(`,{"name":"email","type":["null","string"],"default":null}`));
  await page.locator('#reg-acts').getByRole('button', { name: 'Check compatibility' }).click();
  await expect(page.locator('#reg-answer')).toContainText('Register will be refused with 42205');
  await page.locator('#reg-acts [data-register]').click();
  await expect(page.locator('#reg-answer')).toContainText('42205');
  await page.getByRole('button', { name: 'Set READWRITE for this subject' }).click();
  await expect(page.locator('.flash')).toContainText('Mode is READWRITE');
  await expect(page.locator('main h1 + .pill + .pill')).toHaveCount(0);
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
  // Sc-12: for good, two calls, and only once the name is typed.
  await expect(page.locator('#confirm-title')).toHaveText(`Permanently delete subject ${subject}`);
  await expect(page.locator('#confirm-yes')).toBeDisabled();
  await page.locator('#confirm-type').fill(subject);
  await confirmDialog('accept');
  await expect(page).toHaveURL(/\/schemas$/);
  await expect(page.locator(`.list .li[data-name="${subject}"]`)).toHaveCount(0);
  const gone = await page.goto(`schemas/${subject}`);
  expect(gone?.status()).toBe(404);
  await expect(page.locator('main')).toContainText(`No subject called ${subject}`);
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

/** Checks a draft and registers it, on the register page as it is. */
async function checkAndRegister(page: import('@playwright/test').Page, answer: string | RegExp) {
  await page.locator('#reg-acts').getByRole('button', { name: /Check/ }).click();
  await expect(page.locator('#reg-answer')).toContainText(answer);
  await page.locator('#reg-acts [data-register]').click();
  await expect(page.locator('.flash')).toContainText(/Registered v\d+/);
}

test('a version is soft-deleted, the last one takes the subject to Soft-deleted, and it is deleted for good from there', async ({ page, uniqueName, confirmDialog }) => {
  const subject = uniqueName('soft');
  await registerFirst(page, subject, AVRO());
  await page.getByRole('link', { name: 'Register new version' }).click();
  await page.locator('#f-schema').fill(AVRO(`,{"name":"note","type":["null","string"],"default":null}`));
  await checkAndRegister(page, 'Compatible');
  // Sc-12 dialog 1: v2 goes, its id still resolves.
  await page.getByRole('button', { name: 'More actions' }).click();
  await page.getByRole('button', { name: 'Delete v2' }).click();
  await expect(page.locator('#confirm-title')).toHaveText(`Soft delete version 2 of ${subject}`);
  await expect(page.locator('#confirm-parts')).toContainText('still resolves');
  await confirmDialog('accept');
  await expect(page.locator('.flash')).toContainText('Version 2 soft-deleted');
  await expect(page.locator('#subject-facts')).toContainText('1 version');
  // The last version too: the subject is listed under Soft-deleted.
  await page.getByRole('button', { name: 'More actions' }).click();
  await page.getByRole('button', { name: 'Delete v1' }).click();
  await expect(page.locator('#confirm-parts')).toContainText("the subject's last version");
  await confirmDialog('accept');
  await expect(page).toHaveURL(/\/schemas\?|\/schemas$/);
  const gone = page.locator(`#list-deleted .li[data-name="${subject}"]`);
  await expect(gone).toHaveCount(1);
  // Delete for good: the permanent call alone, behind the name.
  await gone.getByRole('button', { name: /Delete for good/ }).click();
  await expect(page.locator('#confirm-lead')).toContainText('soft-deleted it already');
  await page.locator('#confirm-type').fill(subject);
  await confirmDialog('accept');
  await expect(page.locator(`#list-deleted .li[data-name="${subject}"]`)).toHaveCount(0);
});

test('a reference is added on the register page, and the resolved view writes its type in', async ({ page, uniqueName }) => {
  const key = uniqueName('refkey');
  const keyType = `h.K${key.replace(/[^a-z0-9]/gi, '')}`;
  await registerFirst(page, key, `{"type":"record","name":"K${key.replace(/[^a-z0-9]/gi, '')}","namespace":"h","fields":[{"name":"orderId","type":"string"}]}`);
  const subject = uniqueName('refs');
  await page.goto('new/schema');
  await page.locator('#f-subject').fill(subject);
  await page.locator('#f-schema').fill(`{"type":"record","name":"O","namespace":"h","fields":[{"name":"key","type":["null","${keyType}"],"default":null}]}`);
  await page.getByRole('button', { name: 'Add reference' }).click();
  const row = page.locator('#reg-refs .sc-ref-new');
  await row.getByLabel('Name used in the schema').fill(keyType);
  await row.getByLabel('Subject').fill(key);
  await row.getByLabel('Version').fill('1');
  await checkAndRegister(page, 'Parses');
  // The next version starts with the reference, and shows the type written in.
  await page.getByRole('link', { name: 'Register new version' }).click();
  await expect(page.locator('#reg-refs')).toContainText(keyType);
  await expect(page.locator('#reg-resolved')).toContainText('orderId');
});

test('a Protobuf subject: a new field number may follow, a changed type may not', async ({ page, uniqueName }) => {
  const subject = uniqueName('proto');
  const v1 = 'syntax = "proto3";\npackage p;\n\nmessage M {\n  string a = 1;\n}\n';
  await page.goto('new/schema');
  await page.locator('#f-subject').fill(subject);
  await page.locator('.sc-seg label', { hasText: 'Protobuf' }).click();
  await page.locator('#f-schema').fill(v1);
  await checkAndRegister(page, 'Parses');
  await expect(page.locator('.idband .pill')).toContainText('PROTOBUF');
  await expect(page.locator('#subject-facts')).toContainText('1 message type: M');
  await page.getByRole('link', { name: 'Register new version' }).click();
  await page.locator('#f-schema').fill(v1.replace('}', '  string b = 2;\n}'));
  await page.locator('#reg-acts').getByRole('button', { name: /Check/ }).click();
  await expect(page.locator('#reg-answer')).toContainText('In Protobuf, adding a field with a new number is wire-compatible.');
  await page.locator('#f-schema').fill(v1.replace('string a = 1', 'int64 a = 1'));
  await page.locator('#reg-acts').getByRole('button', { name: /Check/ }).click();
  await expect(page.locator('#reg-answer')).toContainText('Not compatible');
});

test('a JSON Schema subject draws its properties and validates a document', async ({ page, uniqueName }) => {
  const subject = uniqueName('json');
  await page.goto('new/schema');
  await page.locator('#f-subject').fill(subject);
  await page.locator('.sc-seg label', { hasText: 'JSON Schema' }).click();
  await page.locator('#f-schema').fill('{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","properties":{"memberId":{"type":"string"},"points":{"type":"integer","minimum":0}},"required":["memberId"],"additionalProperties":false}');
  await checkAndRegister(page, 'Parses');
  await expect(page.locator('#subject-facts')).toContainText('1 required of 2 properties');
  await expect(page.locator('.sc-props')).toContainText('integer, min 0');
  await page.locator('#try-it [name=doc]').fill('{"memberId":"M-1","points":3}');
  await page.locator('#try-it').getByRole('button', { name: 'Validate' }).click();
  await expect(page.locator('#try-pill')).toContainText('valid');
  await page.locator('#try-it [name=doc]').fill('{"points":3}');
  await page.locator('#try-it').getByRole('button', { name: 'Validate' }).click();
  await expect(page.locator('#try-pill')).toContainText('not valid');
});

test('a message with an id the registry never held is listed with the reason', async ({ page }) => {
  // The harbour seed writes one record on harbour-manifests with id 9999.
  await page.goto('schemas/harbour-manifests-value?tab=used');
  await expect(page.locator('#subject-facts')).toContainText('id 9999, unknown here');
  await expect(page.locator('#undecoded-why')).toContainText('40403');
  const row = page.locator('#undecoded-table .r[data-row]').first();
  await expect(row).toContainText('9999');
  await row.click();
  await expect(page.locator('#inspector')).toBeVisible();
});

test('the global level is set from a subject, and set back', async ({ page }) => {
  await page.goto('schemas/seed-events-value?tab=compat');
  await page.getByRole('radio', { name: 'FULL', exact: true }).check();
  await page.locator('.sc-seg label', { hasText: 'Global default' }).click();
  await page.getByRole('button', { name: 'Save level' }).click();
  await expect(page.locator('#subject-facts')).toContainText('global default FULL');
  await page.getByRole('radio', { name: 'BACKWARD', exact: true }).check();
  await page.locator('.sc-seg label', { hasText: 'Global default' }).click();
  await page.getByRole('button', { name: 'Save level' }).click();
  await expect(page.locator('#subject-facts')).toContainText('global default BACKWARD');
});

test('the keys of a subject: j k to a version, ↵ opens it, d diffs, [ and ] step the pair, n registers', async ({ page }) => {
  await page.goto('schemas/seed-events-value');
  await page.keyboard.press('j');
  await page.keyboard.press('j');
  await page.keyboard.press('Enter');
  await expect(page).toHaveURL(/version=1/);
  await page.goto('schemas/seed-events-value');
  await page.keyboard.press('d');
  await expect(page).toHaveURL(/\/diff\?from=1&to=2/);
  await page.keyboard.press('u');
  await expect(page).toHaveURL(/layout=unified/);
  await page.keyboard.press('Escape');
  await expect(page).toHaveURL(/\/schemas\/seed-events-value$/);
  await page.keyboard.press('n');
  await expect(page).toHaveURL(/\/new\/schema\?subject=seed-events-value/);
});

for (const width of [960, 390]) {
  test(`a subject and the register page fit at ${width} px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 800 });
    for (const path of ['schemas/seed-events-value', 'new/schema?subject=seed-events-value', 'schemas/seed-events-value/diff']) {
      await page.goto(path);
      await expect(page.locator('main h1')).toBeVisible();
      const over = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
      expect(over, `${path} at ${width}`).toBeLessThanOrEqual(1);
    }
  });
}

test('Try it encodes a value, and Raw is the registry\'s answer', async ({ page }) => {
  await page.goto('schemas/seed-events-value');
  await page.locator('#try-it').getByRole('button', { name: 'Encode' }).click();
  await expect(page.locator('#try-result')).toContainText('bytes: magic byte 0');
  await page.getByRole('link', { name: 'Raw', exact: true }).click();
  await expect(page.locator('#raw')).toContainText('"subject": "seed-events-value"');
});

test.describe('on a broker started with the registry off', () => {
  const off = new OwnBroker('schemas-spec-off', 90, ['--schema-registry=false']);
  test.beforeAll(async () => { test.setTimeout(180_000); await off.start(); });
  test.afterAll(async () => { await off.kill(); });

  test('the rail keeps Schemas, marked off, and the page says how to turn it on', async ({ page }) => {
    await page.goto(off.console + 'topics');
    await expect(page.locator('nav.rail a.nav', { hasText: 'Schemas' })).toContainText('off');
    await page.locator('nav.rail a.nav', { hasText: 'Schemas' }).click();
    await expect(page.locator('main h1 + .note')).toContainText('switched off');
    await expect(page.locator('main [data-copy-command]')).toHaveAttribute('data-copy', /--schema-registry$/);
  });
});

