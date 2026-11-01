import { test, expect } from '@playwright/test';
import { BASE_URL } from '../../playwright.config';
import { livePage, runBoard } from '../lib/gate';
import { CREATE, SETTINGS, REFUSED, NONE, TOPIC_BODY, TOPIC_COMMON, TOPIC_TABLE } from './boards';
import { TOPIC } from '../lib/topic';
import { OwnBroker } from '../../ownbroker';
import { openLive } from '../lib/render';
import { SHELL } from '../lib/probes';
import { ALLOW } from './allow';

const NAME = 'harbour.checkout.invoices';

test('Tp-02 matches the create form', async ({ browser }) => {
  test.setTimeout(120_000);
  const out = await runBoard(browser, { id: 'Tp-02', file: 'Tp-02-Create', live: livePage(BASE_URL, `new/topic?name=${NAME}&partitions=3`), probes: CREATE, allow: ALLOW });
  expect(out.text, out.text).toBe('');
});

test('Tp-03 matches the create form with its settings open', async ({ browser }) => {
  test.setTimeout(120_000);
  const q = `new/topic?settings=1&name=${NAME}&partitions=3&segment.bytes=1048576&cleanup.policy=compact`;
  const out = await runBoard(browser, { id: 'Tp-03', file: 'Tp-03-Create-Config', live: livePage(BASE_URL, q), probes: SETTINGS, allow: ALLOW });
  expect(out.text, out.text).toBe('');
});

test('Tp-04 matches the refused create form', async ({ browser }) => {
  test.setTimeout(120_000);
  const out = await runBoard(browser, {
    id: 'Tp-04', file: 'Tp-04-Create-Refused', probes: REFUSED, allow: ALLOW,
    live: livePage(BASE_URL, 'new/topic?name=harbour/orders&partitions=3', async (p) => {
      await p.getByRole('button', { name: 'Create topic' }).last().click();
      await p.locator('[data-err-slot] .callout').waitFor();
    }),
  });
  expect(out.text, out.text).toBe('');
});

test.describe('on an empty broker', () => {
  const empty = new OwnBroker('topics-none', 50);
  test.beforeAll(async () => { test.setTimeout(180_000); await empty.start(); });
  test.afterAll(async () => { await empty.kill(); });
  test('Tp-01 matches the topics page of a broker with none', async ({ browser }) => {
    test.setTimeout(120_000);
    const out = await runBoard(browser, { id: 'Tp-01', file: 'Tp-01-None', live: async (b, place) => openLive(b, empty.console + 'topics', place.size, place.theme), probes: NONE, allow: ALLOW });
    expect(out.text, out.text).toBe('');
  });
});

test('Tp-05 matches the topic page', async ({ browser }) => {
  test.setTimeout(120_000);
  const out = await runBoard(browser, { id: 'Tp-05', file: 'Tp-05-Messages', live: livePage(BASE_URL, `topics/${TOPIC}`), probes: [...SHELL, ...TOPIC_COMMON, ...TOPIC_BODY, ...TOPIC_TABLE], allow: ALLOW });
  expect(out.text, out.text).toBe('');
});
