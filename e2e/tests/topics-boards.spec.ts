import { execFileSync } from 'node:child_process';
import { Buffer } from 'node:buffer';
import { test, expect } from '../fixtures/console';
import { kafkaAddr } from '../addr';

// Tp-07 to Tp-15, the functional boards: the message inspector for bytes, a registry schema and a
// tombstone; the composer with a schema, refused, and Send again; go to offset and go to time.
// Each test makes its own topic, so none waits on another's data.

const b64 = (s: string | Buffer) => Buffer.from(s).toString('base64');

async function topic(request: any, name: string, extra: Record<string, string> = {}, partitions = '1') {
  const r = await request.post('new/topic', { form: { name, partitions, ...extra }, maxRedirects: 0 });
  expect([200, 303]).toContain(r.status());
}
async function send(request: any, name: string, form: Record<string, string>) {
  const r = await request.post(`topics/${name}/produce`, { form: { partition: '0', partition_ctx: '0', ...form } });
  expect(r.status(), await r.text()).toBe(200);
}
async function open(page: any, name: string, offset: number) {
  await page.goto(`topics/${name}?partition=0&open=${offset}`);
  await expect(page.locator('#inspector')).toBeVisible();
  return page.locator('#inspector');
}

test('Tp-07: a value that is not text is shown as bytes, says why, and has its base64; null and empty keys are told apart', async ({ page, request, uniqueName }) => {
  const name = uniqueName('bin');
  await topic(request, name);
  const body = Buffer.from([0x0a, 0x08, ...Buffer.from('DP-00417'), 0x10, 0x02, 0x1a, 0x05, ...Buffer.from('BAY-3')]);
  await send(request, name, { key: 'DP-00417', value: b64(body), value_enc: 'base64' });
  await send(request, name, { key_empty: '1', value: b64(body), value_enc: 'base64' });
  await send(request, name, { key: 'x', value: 'plain' });

  // The table says it is binary, its size, and its first bytes; an empty key is drawn as one.
  await page.goto(`topics/${name}?partition=0`);
  await expect(page.locator('#msg-0 .cell-val')).toContainText('binary · 19 B · 0a 08 44 50 2d 30 30 34 31 37');
  await expect(page.locator('#msg-1 .cell-key')).toHaveText('"" (empty)');

  const insp = await open(page, name, 0);
  await expect(insp.locator('.cap').filter({ hasText: 'Value' }).first()).toContainText('19 B · binary');
  await expect(insp.getByRole('button', { name: 'Hex', exact: true })).toHaveAttribute('aria-pressed', 'true');
  await expect(insp.locator('.hexbox')).toContainText('0a 08 44 50 2d 30 30 34');
  await expect(insp.locator('.tp-b64')).toContainText(b64(body));
  await expect(insp.locator('.callout')).toContainText('Not readable as text, so shown as bytes');
  await expect(insp.locator('.callout')).toContainText("The first byte is 0x0a, not the registry's 0x00 marker");
  await expect(insp.locator('.tp-legend')).toContainText('Null is not empty');
  await expect(insp.locator('.tp-legend')).toContainText('"" (empty)');
  await expect(insp.locator('.tp-legend')).toContainText('offset 1'); // the empty-key record, the nearest.
  await expect(insp.locator('.tp-legend .r.on')).toContainText('this record: 8 bytes');
  await insp.getByRole('button', { name: 'Base64', exact: true }).click();
  await expect(insp.locator('pre.code').filter({ hasText: b64(body) })).toBeVisible();

  const empty = await open(page, name, 1);
  await expect(empty.locator('.tp-fields')).toContainText('"" (empty)');
  await expect(empty.locator('.tp-legend .r.on')).toContainText('this record');
});

test('Tp-08: a registry value is shown decoded with its schema id and subject, Raw explains the framing, and an unknown id says so', async ({ page, request, uniqueName }) => {
  await page.goto('topics/seed-events?partition=0&open=0');
  const insp = page.locator('#inspector');
  await expect(insp.locator('.tp-fields')).toContainText('Avro · schema');
  await expect(insp.locator('.tp-fields a')).toContainText('seed-events-value v1');
  await expect(insp.locator('.tp-val .cap').first()).toHaveText('Value, decoded');
  await expect(insp.locator('.tp-val')).toContainText('Decoded by the built-in registry at');
  await expect(insp.locator('.tp-val pre.code').first()).toContainText('"kind"');
  await insp.getByRole('button', { name: 'Raw', exact: true }).click();
  await expect(insp.locator('.tp-raw')).toContainText('the registry framing');
  await expect(insp.locator('.tp-raw')).toContainText('magic byte, always zero');
  await expect(insp.locator('.tp-raw')).toContainText('four bytes, big-endian');
  await expect(insp.locator('.tp-raw .hexbox')).toContainText('00 00 00 00');

  // A value framed with an id this registry has never seen is shown as bytes, with that said.
  const name = uniqueName('unk');
  await topic(request, name);
  await send(request, name, { key: 'k', value: b64(Buffer.from([0, 0, 0, 9, 9, 0x1c, 0x48, 0x52])), value_enc: 'base64' });
  const u = await open(page, name, 0);
  await expect(u.locator('.callout')).toContainText('Schema id 2313 is not registered'.replace('2313', String(0x00000909)));
  await expect(u.locator('.callout')).not.toContainText('not the registry');
});

test('Tp-09: a null value is a tombstone and an empty one is not; on a compacted topic the page says what the cleaner does with the key', async ({ page, request, uniqueName }) => {
  const name = uniqueName('tomb'), plain = uniqueName('tombplain');
  await topic(request, name, { 'cleanup.policy': 'compact' });
  await topic(request, plain);
  for (const t of [name, plain]) {
    await send(request, t, { key: 'SKU-BUOY-0007', value: '{"onHand":42}' });
    await send(request, t, { key: 'SKU-LAMP-0031', value: '' });
    await send(request, t, { key: 'SKU-BUOY-0007', tombstone: '1' });
  }
  await page.goto(`topics/${name}?partition=0`);
  await expect(page.locator('#msg-2 .cell-val')).toContainText('tombstone · null value');
  await expect(page.locator('#msg-1 .cell-val')).toContainText('empty value · 0 B, a record, not a delete');

  const t = await open(page, name, 2);
  await expect(t.locator('.pill.deny')).toHaveText('null, a tombstone');
  await expect(t.locator('.callout.deny')).toContainText('Not an empty string and not zero bytes: the value is absent. It says "this key is deleted".');
  await expect(t.locator('.tp-legend')).toContainText('Null is not empty');
  await expect(t.locator('.tp-legend')).toContainText('offset 1'); // the empty value.
  await expect(t.locator('.tp-history')).toContainText('What compaction will do with SKU-BUOY-0007');
  await expect(t.locator('.tp-history')).toContainText('goes once cleaned');
  await expect(t.locator('.tp-history')).toContainText('delete.retention.ms');
  await expect(t.locator('.tp-history')).toContainText('Records of this key that compaction has already removed');

  const e = await open(page, name, 1);
  await expect(e.locator('.valuebox')).toContainText('zero bytes, a record and not a delete');
  const p = await open(page, plain, 2);
  await expect(p.locator('.callout.deny')).toContainText('This topic is not compacted, so nothing is deleted by it');
  await expect(p.locator('.tp-history')).toHaveCount(0);
});

test('Tp-10: What will be sent is the broker\'s partitioner and the batch\'s own size, and follows the fields', async ({ page, request, uniqueName }) => {
  const name = uniqueName('wws');
  await topic(request, name, {}, '3');
  await page.goto(`topics/${name}`);
  await page.locator('#compose-head').click();
  const sent = page.locator('.tp-cp-sent');
  await page.locator('#f-key').fill('HRB-2026-04812');
  await page.locator('#f-value').fill('{"orderId":"HRB-2026-04812"}');
  await expect(sent).toContainText('14 B');
  await expect(sent).toContainText('28 B');
  await expect(sent).toContainText(/p[012], by key/);
  await expect(sent).toContainText(/1 record, [\d,]+ B/);
  await expect(page.locator('.tp-cp-part .note')).toContainText('murmur2 of the key bytes modulo 3');
  await expect(page.locator('.tp-cp-part .note')).toContainText('HRB-2026-04812 lands on p');
  await expect(page.locator('#compose-valid')).toContainText('Valid JSON, 28 B. Checked as you type; the broker would accept any bytes.');
  // The same key goes where the page said it would.
  const where = (await sent.innerText()).match(/p([012]), by key/)![1];
  await page.getByRole('button', { name: 'Send message' }).click();
  await expect(page.locator(`#tape-and-table tbody tr`).first()).toContainText('HRB-2026-04812');
  await expect(page.locator('#tape-and-table tbody tr').first().locator('.col-p')).toHaveText(where);
  // An empty key is a key; a null value is said to be one; headers are counted with their size.
  await page.locator('[aria-label="empty key, not null"]').click();
  await expect(sent).toContainText('empty');
  await page.getByRole('button', { name: 'Null (tombstone)' }).click();
  await expect(sent).toContainText('null, a tombstone');
  await page.getByRole('button', { name: '+ Add header' }).click();
  await page.getByLabel('Header key').fill('source');
  await page.getByLabel('Header value', { exact: true }).fill('console');
  await expect(sent).toContainText('1 · 13 B');
  await page.getByRole('button', { name: 'Fixed' }).click();
  await page.locator('#f-partition').selectOption('2');
  await expect(sent).toContainText('p2, fixed');
  await page.getByRole('button', { name: 'Clear' }).click();
  await expect(page.locator('#f-key')).toHaveValue('');
});

test('Tp-11: with a schema subject the value is written as JSON, checked as it is typed, shown as the bytes it becomes, and sent encoded', async ({ page, request, uniqueName }) => {
  const name = uniqueName('enc');
  await page.goto('new/schema');
  await page.locator('#f-subject').fill(`${name}-value`);
  await page.locator('#f-schema').fill('{"type":"record","name":"Order","fields":[{"name":"orderId","type":"string"},{"name":"lines","type":"int"}]}');
  await page.getByRole('button', { name: 'Register', exact: true }).click();
  await expect(page.locator('.idband h1')).toHaveText(`${name}-value`);
  await topic(request, name);
  await page.goto(`topics/${name}`);
  await page.locator('#compose-head').click();
  await expect(page.locator('#c-sub')).toHaveValue(`${name}-value`);
  await expect(page.locator('label[for="f-value"]')).toContainText('Value, as JSON');
  await page.locator('#f-value').fill('{"orderId":"HRB-1","lines":"two"}');
  await expect(page.locator('#compose-valid')).toHaveClass(/bad/);
  await expect(page.locator('#compose-valid')).toContainText('does not fit');
  await page.locator('#f-value').fill('{"orderId":"HRB-1","lines":2}');
  await expect(page.locator('#compose-valid')).toContainText(`Valid against ${name}-value v1: orderId string, lines int.`);
  await expect(page.locator('.tp-cp-body .cap').filter({ hasText: 'Encoded preview' })).toContainText(/Encoded preview · \d+ B/);
  await expect(page.locator('.tp-cp-body pre.code').first()).toContainText('00 00 00 00');
  await expect(page.locator('.tp-cp-sent')).toContainText('Schema id');
  await expect(page.locator('.tp-cp-foot')).toContainText(/Acks 1\./);
  await page.getByRole('button', { name: 'Send message' }).click();
  await expect(page.locator('#tape-and-table tbody tr').first().locator('.cell-val')).toContainText('"orderId":"HRB-1"');
  await expect(page.locator('#tape-and-table tbody tr').first().locator('.cell-val')).toContainText('Avro · schema');
});

test('Tp-12: a send the broker refuses is shown as the broker answered it, with the facts the console knows kept apart; the form catches a bad header before any request', async ({ page, request, uniqueName }) => {
  const small = uniqueName('limit'), comp = uniqueName('comp'), orders = uniqueName('hdr');
  await topic(request, small, { 'max.message.bytes': '1024' });
  await topic(request, comp, { 'cleanup.policy': 'compact' });
  await topic(request, orders);

  // 1. The form catches it: nothing is requested, and the page says the broker never saw it.
  await page.goto(`topics/${orders}`);
  await page.locator('#compose-head').click();
  const posted: string[] = [];
  page.on('request', (r) => { if (r.method() === 'POST' && r.url().endsWith('/produce')) posted.push(r.url()); });
  await page.locator('#f-value').fill('x');
  await page.getByRole('group', { name: 'Headers as' }).getByRole('button', { name: 'JSON' }).click();
  await page.getByLabel('Headers as JSON').fill('[{"k":"source","v":"console"');
  await page.getByRole('button', { name: 'Send message' }).click();
  const err = page.locator('.tp-compose [data-err-slot]');
  await expect(err).toContainText('Not sent');
  await expect(err).toContainText('Headers must be a JSON array of {"k": …, "v": …}');
  await expect(err).toContainText('No Kafka error exists for this, because the broker never saw it');
  expect(posted).toEqual([]);
  // Rows and JSON are the same headers: the JSON, mended, comes back as rows.
  await page.getByLabel('Headers as JSON').fill('[{"k":"source","v":"console"}]');
  await page.getByRole('group', { name: 'Headers as' }).getByRole('button', { name: 'Rows' }).click();
  await expect(page.getByLabel('Header key')).toHaveValue('source');

  // 2. The broker refuses the size: its code, Kafka's sentence for it, and the console's own facts.
  await page.goto(`topics/${small}`);
  await page.locator('#compose-head').click();
  await page.locator('#f-value').fill('x'.repeat(2400));
  await page.getByRole('button', { name: 'Send message' }).click();
  await expect(err).toContainText('Refused');
  await expect(err).toContainText('MESSAGE_TOO_LARGE');
  await expect(err).toContainText('larger than the max message size the server will accept');
  await expect(err.locator('.refusal-facts')).toContainText("this topic's max.message.bytes is 1,024");
  await expect(page.locator('#tape-and-table tbody tr[id^="msg-"]')).toHaveCount(0);

  // With acks 0 the broker answers nothing; the page says the record was not delivered.
  await page.locator('[aria-labelledby="l-acks"] button', { hasText: /^0$/ }).click();
  await page.getByRole('button', { name: 'Send message' }).click();
  await expect(err).toContainText('Not delivered');
  await expect(page.locator('#tape-and-table tbody tr[id^="msg-"]')).toHaveCount(0);

  // 3. A compacted topic and no key: the broker's own sentence for it.
  await page.goto(`topics/${comp}`);
  await page.locator('#compose-head').click();
  await page.locator('#f-value').fill('x');
  await page.getByRole('button', { name: 'Send message' }).click();
  await expect(err).toContainText('INVALID_RECORD');
  await expect(err).toContainText('Compacted topic cannot accept message without key');
  // An empty key is allowed.
  await page.locator('[aria-label="empty key, not null"]').click();
  await page.getByRole('button', { name: 'Send message' }).click();
  await expect(page.locator('#tape-and-table tbody tr[id^="msg-"]')).toHaveCount(1);
});

test('Tp-13: Send again writes the copy and sets it beside the original, read back from the broker', async ({ page, request, uniqueName }) => {
  const name = uniqueName('again');
  await topic(request, name);
  await send(request, name, { key: 'HRB-2026-04811', value: '{"orderId":"HRB-2026-04811","lines":3}', headers: JSON.stringify([{ k: 'traceparent', v: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' }, { k: 'source', v: 'checkout-api' }]) });
  const insp = await open(page, name, 0);
  await insp.getByRole('button', { name: 'Send again' }).click();
  await expect(page.locator('#resend-note')).toContainText('Sent again. An exact copy of p0 offset 0 was written to p0 at offset 1.');
  await expect(insp.locator('.cap').first()).toHaveText('Send again');
  await expect(insp.locator('#inspector-title')).toHaveText('Original and copy, side by side');
  const where = insp.locator('.tp-resend-where');
  await expect(where.locator('.r').nth(1)).toContainText('0');
  await expect(where.locator('.r').nth(1)).toContainText('1');
  const same = insp.locator('.tp-resend-same');
  await expect(same).toContainText('HRB-2026-04811 · 14 B');
  await expect(same.locator('.ok')).toHaveCount(3);
  await expect(same.locator('.bad')).toHaveCount(0);
  await expect(insp).toContainText('Headers carried over');
  await expect(insp).toContainText('so it carries the same trace id');
  await expect(insp).toContainText('no producer id or sequence');
  await expect(page.locator('#msg-1')).toHaveClass(/copied/); // the copy's row is marked.
  await insp.getByRole('button', { name: 'Open the copy' }).click();
  await expect(page.locator('#inspector .h2, #inspector-title')).toContainText('offset 1');
});

test('Tp-14: go to offset says when the offset is past the end, with the latest, and a way to the newest records', async ({ page, request, uniqueName }) => {
  const name = uniqueName('jo');
  await topic(request, name);
  for (let i = 0; i < 5; i++) await send(request, name, { key: `k${i}`, value: `v${i}` });
  await page.goto(`topics/${name}?partition=0`);
  await page.locator('#jump-to').fill('9000');
  await page.locator('form.jump', { has: page.locator('#jump-to') }).getByRole('button', { name: 'Go' }).click();
  const note = page.locator('#jump-msg');
  await expect(note.locator('.b')).toHaveText('Offset 9,000 is past the end (latest is 5)');
  await expect(note).toContainText('p0 holds offsets 0 to 4 now. Showing the newest records.');
  await expect(note.getByRole('link', { name: 'Jump to latest' })).toBeVisible();
  await expect(page.locator('#msg-4')).toBeVisible(); // the view is the newest records.
  await note.getByRole('button', { name: 'Dismiss' }).click();
  await expect(note).toBeHidden();
  // An offset on disk lands on its record, highlighted.
  await page.locator('#jump-to').fill('2');
  await page.locator('form.jump', { has: page.locator('#jump-to') }).getByRole('button', { name: 'Go' }).click();
  await expect(page.locator('tr.hl')).toHaveCount(1);
  await expect(page.locator('#jump-msg')).toHaveCount(0);
});

test('Tp-15: go to time lands on the first record at or after it (ListOffsets by time) and says what it does for a time before the first record or after the last', async ({ page, request, uniqueName }) => {
  const name = uniqueName('jt');
  await topic(request, name);
  for (const [t, v] of [['2026-03-14T23:59:07.000Z', 'a'], ['2026-03-15T00:00:07.000Z', 'b'], ['2026-03-15T00:01:07.000Z', 'c']]) {
    execFileSync('go', ['run', './e2e/put', kafkaAddr(), name, t, v], { cwd: '..', env: { ...process.env, GOWORK: 'off' } });
  }
  const go = async (at: string) => {
    await page.goto(`topics/${name}?partition=0`);
    await page.locator('#jump-at').fill(at);
    await page.locator('form.jump', { has: page.locator('#jump-at') }).getByRole('button', { name: 'Go' }).click();
    return page.locator('#jump-msg');
  };
  let note = await go('2026-03-15 00:00:00');
  await expect(note.locator('.b')).toHaveText('First record at or after 2026-03-15 00:00:00 UTC is offset 1');
  await expect(note).toContainText('Timestamp 2026-03-15 00:00:07.000 UTC on p0, found by the timestamp lookup (ListOffsets by time). The row is highlighted below.');
  await expect(page.locator('tr.hl')).toHaveCount(1);
  await note.getByRole('link', { name: 'Open it' }).click();
  await expect(page.locator('#inspector')).toContainText('offset 1');

  note = await go('2026-03-01');
  await expect(note.locator('.b')).toHaveText('Nothing that early: showing from the start');
  await expect(note).toContainText('p0 starts at offset 0, stamped 2026-03-14 23:59:07.000 UTC');

  note = await go('2026-04-01');
  await expect(note.locator('.b')).toHaveText('No record at or after 2026-04-01 00:00:00 UTC');
  await expect(note).toContainText("The broker answers -1 for a time later than the last record. The console's rule: it shows the newest records, from the end of the log (offset 3, nothing there yet); p0 holds offsets 0 to 2.");
  await expect(note.getByRole('link', { name: 'Follow live' })).toBeVisible();
});
