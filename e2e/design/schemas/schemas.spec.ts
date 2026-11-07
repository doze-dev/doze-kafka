import { test, expect } from '@playwright/test';
import { livePage, runBoard } from '../lib/gate';
import { openLive } from '../lib/render';
import { OwnBroker } from '../../ownbroker';
import { ALLOW } from './allow';
import { DIFF, NONE, SCHEMA, SUBJECT_HEAD, TRY, VERSION_INSP, VERSIONS, DOCK } from './boards';
import { AUDIT, DISPATCH, ORDERS, POINTS, SchemasWorld } from './world';

// The Schemas boards against the live pages, element by element, in light and dark.
// What may differ is in allow.ts, each with its reason.

test.describe.configure({ mode: 'serial' });

const dockOf = (html: string) => {
  const at = html.indexOf('id="wire-dock"');
  return html.slice(at, html.indexOf('</section>', at));
};

test.describe('on a broker whose registry is empty', () => {
  const empty = new OwnBroker('schemas-none', 70);
  const registry = () => `http://127.0.0.1:${empty.port + 2}`;
  test.beforeAll(async () => { test.setTimeout(180_000); await empty.start(); });
  test.afterAll(async () => { await empty.kill(); });

  test('Sc-01 says this broker\'s addresses, and its dock is what clients asked the registry', async ({ request }) => {
    const html = await (await request.get(empty.console + 'schemas')).text();
    const page = html.replace(/<[^>]+>/g, '');
    expect(page).toContain(`bootstrap.servers=${empty.kafka}`);
    expect(page).toContain(`schema.registry.url=${registry()}`);
    // The console has asked the registry for its subjects to draw this page; its own
    // reads are not kept, so nothing has asked.
    expect(dockOf(html)).toContain('Nothing has asked the registry yet.');
    // The board's state: a client has asked for the subjects, and was told [].
    const subjects = await request.get(`${registry()}/subjects`);
    expect(subjects.status()).toBe(200);
    expect(await subjects.json()).toEqual([]);
    const dock = dockOf(await (await request.get(empty.console + 'schemas')).text());
    expect(dock.match(/data-reg-row/g)?.length).toBe(1);
    expect(dock).toContain('>GET<');
    expect(dock).toContain('>/subjects<');
    expect(dock).toContain('>200<');
  });

  test('Sc-01 matches the Schemas page of an empty registry', async ({ browser }) => {
    test.setTimeout(120_000);
    const out = await runBoard(browser, { id: 'Sc-01', file: 'Sc-01-None', live: async (b, place) => openLive(b, empty.console + 'schemas', place.size, place.theme), probes: NONE, allow: ALLOW });
    expect(out.text, out.text).toBe('');
  });
});

test.describe('on the boards\' world: their subjects, under their names', () => {
  const w = new SchemasWorld(74);
  const seg = encodeURIComponent;
  const inspectorOpen = async (p: import('@playwright/test').Page) => { await p.locator('#inspector .versionb').waitFor(); };
  test.beforeAll(async () => { test.setTimeout(180_000); await w.start(); });
  test.afterAll(async () => { await w.stop(); });

  test('what the subject pages say is the registry\'s', async ({ request }) => {
    const text = async (path: string) => (await (await request.get(w.console + path)).text()).replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');
    const orders = await text(`schemas/${seg(ORDERS)}`);
    for (const want of ['3 versions', 'latest v3, id 7', 'level BACKWARD (global)', 'mode READWRITE', 'added optional coupon (null or string, default null)',
      'added currency with default EUR', 'first version: orderId, lines, total', 'v2, BACKWARD ok', 'v1, BACKWARD ok', 'id 7 · 5 fields', 'Diff v2 to v3']) expect(orders).toContain(want);
    expect(await text(`schemas/${seg(DISPATCH)}`)).toContain('added carrier = 4');
    expect(await text(`schemas/${seg(DISPATCH)}`)).toContain('1 message type: Dispatch');
    const points = await text(`schemas/${seg(POINTS)}`);
    for (const want of ['2 required of 4 properties', 'closed: additionalProperties false', 'id 5 · draft-07', 'integer, min 0']) expect(points).toContain(want);
    const diff = await text(`schemas/${seg(ORDERS)}/diff?from=2&to=3`);
    for (const want of ['v2 to v3', '1 line added', '0 removed', '1 line changed', 'the change is allowed at BACKWARD: the new field has a default', 'At FULL it would pass.']) expect(diff).toContain(want);
    // The inspector's messages on id 7 are the one record the composer wrote with v3.
    const insp = await text(`inspect/version?key=${seg(`${seg(ORDERS)}:3`)}`);
    expect(insp).toContain('1 of the newest 1 on harbour.checkout.orders carry id 7');
  });

  test('Sc-05 matches an Avro subject with its latest version open', async ({ browser }) => {
    test.setTimeout(120_000);
    const path = `schemas/${seg(ORDERS)}?version=3&open=${seg(`version:${seg(ORDERS)}:3`)}`;
    const out = await runBoard(browser, { id: 'Sc-05', file: 'Sc-05-Versions', live: livePage(w.console, path, inspectorOpen), probes: [...SUBJECT_HEAD, ...VERSIONS, ...SCHEMA, ...TRY, ...VERSION_INSP, ...DOCK], allow: ALLOW });
    expect(out.text, out.text).toBe('');
  });

  test('Sc-06 matches v2 to v3 of the Avro subject', async ({ browser }) => {
    test.setTimeout(120_000);
    const out = await runBoard(browser, { id: 'Sc-06', file: 'Sc-06-Diff', live: livePage(w.console, `schemas/${seg(ORDERS)}/diff?from=2&to=3`), probes: [...DIFF, ...DOCK], allow: ALLOW });
    expect(out.text, out.text).toBe('');
  });

  test('Sc-07 matches a Protobuf subject', async ({ browser }) => {
    test.setTimeout(120_000);
    const out = await runBoard(browser, { id: 'Sc-07', file: 'Sc-07-Protobuf', live: livePage(w.console, `schemas/${seg(DISPATCH)}`), probes: [...SUBJECT_HEAD, ...VERSIONS, ...SCHEMA, ...TRY, ...DOCK], allow: ALLOW });
    expect(out.text, out.text).toBe('');
  });

  test('Sc-08 matches a JSON Schema subject', async ({ browser }) => {
    test.setTimeout(120_000);
    // The board draws Try it after Validate: the document is valid.
    const validated = async (p: import('@playwright/test').Page) => {
      await p.locator('#try-it [name=doc]').fill('{"memberId":"M-20817","points":120,"orderId":"HRB-2026-04811"}');
      await p.locator('#try-it button[value=check]').click();
      await p.locator('#try-pill .pill.ok').waitFor();
    };
    const out = await runBoard(browser, { id: 'Sc-08', file: 'Sc-08-JsonSchema', live: livePage(w.console, `schemas/${seg(POINTS)}`, validated), probes: [...SUBJECT_HEAD, ...VERSIONS, ...TRY, ...DOCK], allow: ALLOW });
    expect(out.text, out.text).toBe('');
  });

  test('Sc-18 matches a subject a client registered', async ({ browser }) => {
    test.setTimeout(120_000);
    await w.registerByClient();
    const out = await runBoard(browser, { id: 'Sc-18', file: 'Sc-18-Registered-By-Client', live: livePage(w.console, `schemas/${seg(AUDIT)}`), probes: [...SUBJECT_HEAD, ...VERSIONS, ...DOCK], allow: ALLOW });
    expect(out.text, out.text).toBe('');
  });
});
