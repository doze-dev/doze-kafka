import { test, expect } from '@playwright/test';
import { runBoard } from '../lib/gate';
import { openLive } from '../lib/render';
import { OwnBroker } from '../../ownbroker';
import { ALLOW } from './allow';
import { NONE } from './boards';

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
