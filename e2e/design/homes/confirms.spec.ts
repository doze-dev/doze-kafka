import { test, expect, type Page } from '@playwright/test';
import { BASE_URL } from '../../playwright.config';
import type { Probe } from '../lib/compare';
import { grouped, livePage, runBoard, truth } from '../lib/gate';
import { SHELL } from '../lib/probes';
import { ALLOW } from './allow';

// Tp-24: the purge and the delete of a topic. The board draws both dialogs side by
// side over a topic page; the console opens one at a time. Each is compared with its
// own specimen, and what it says is held to the seeded broker.
//
// The seed has no topic with three partitions and a schema subject, as the board's
// has: the purge is opened on harbour-arrivals (three partitions, a group behind) and
// the delete on harbour-manifests (a subject, a group that has committed on it).
const PURGED = 'harbour-arrivals';
const DELETED = 'harbour-manifests';

async function openConfirm(page: Page, verb: 'Purge' | 'Delete') {
  await page.locator('main [data-more]').click();
  await page.locator('main .menu button', { hasText: verb }).click();
  await expect(page.locator('#confirm')).toBeVisible();
  await expect(page.locator('#confirm-rows')).toBeVisible();
  await page.waitForTimeout(250); // the pop-in animation.
}

// Inside the dialog, x and y are measured from the dialog's corner: the board draws its
// two specimens at the sides of the page, the console centres the one that is open.
const IN = (n: number) => ({ board: '.modal', live: '#confirm .modal', nth: n, liveNth: 0 });
const TEXT_H = 'its height is its text, and the text is the seed\'s topic, not the board\'s';
const BELOW_TEXT = 'it is below text whose height is the seed\'s, not the board\'s';
const dialogProbes = (n: number): Probe[] => [
  { el: 'scrim', board: '.scrim', live: '#confirm.overlay' },
  { el: 'dialog', board: '.modal', live: '#confirm .modal', nth: n, liveNth: 0, free: { h: TEXT_H } },
  { el: 'dialog pad', board: '.modal > .pad', live: '#confirm .modal > .pad', nth: n, liveNth: 0, origin: IN(n), free: { h: TEXT_H } },
  { el: 'title', board: '.modal h2', live: '#confirm-title', nth: n, liveNth: 0, origin: IN(n), free: { w: TEXT_H } },
  { el: 'paragraph', board: '.modal p', live: '#confirm-msg', nth: n, liveNth: 0, origin: IN(n), free: { h: TEXT_H } },
  { el: 'paragraph mono', board: '.modal p .m', live: '#confirm-msg .m', nth: n, liveNth: 0, origin: IN(n), free: { w: TEXT_H, x: TEXT_H, y: TEXT_H } },
  { el: 'table', board: '.modal .kv', live: '#confirm-rows', nth: n, liveNth: 0, origin: IN(n), free: { h: TEXT_H, y: BELOW_TEXT } },
  { el: 'table label', board: '.modal .kv > span:nth-child(odd)', live: '#confirm-rows > span:nth-child(odd)', nth: n * 4, liveNth: 0, text: true, origin: IN(n), free: { h: TEXT_H, y: BELOW_TEXT } },
  { el: 'table label 2', board: '.modal .kv > span:nth-child(odd)', live: '#confirm-rows > span:nth-child(odd)', nth: n * 4 + 1, liveNth: 1, text: true, origin: IN(n), free: { h: TEXT_H, y: BELOW_TEXT } },
  { el: 'table label 3', board: '.modal .kv > span:nth-child(odd)', live: '#confirm-rows > span:nth-child(odd)', nth: n * 4 + 2, liveNth: 2, text: true, origin: IN(n), free: { h: TEXT_H, y: BELOW_TEXT } },
  { el: 'table label 4', board: '.modal .kv > span:nth-child(odd)', live: '#confirm-rows > span:nth-child(odd)', nth: n * 4 + 3, liveNth: 3, text: true, origin: IN(n), free: { h: TEXT_H, y: BELOW_TEXT } },
  { el: 'table value', board: '.modal .kv > span:nth-child(even)', live: '#confirm-rows > span:nth-child(even)', nth: n * 4, liveNth: 0, origin: IN(n), free: { h: TEXT_H, y: BELOW_TEXT } },
  { el: 'buttons', board: '.modal .row.g8', live: '#confirm .confirm-f', nth: n, liveNth: 0, origin: IN(n), free: { y: BELOW_TEXT } },
  { el: 'cancel', board: '.modal .row.g8 .btn:first-child', live: '#confirm-no', nth: n, liveNth: 0, text: true, origin: IN(n), free: { x: 'right-aligned beside the verb, whose width is its number of records', y: BELOW_TEXT } },
  { el: 'verb', board: '.modal .row.g8 .btn.danger', live: '#confirm-yes', nth: n, liveNth: 0, origin: IN(n), free: { w: 'its words carry the seed\'s number of records', x: 'right-aligned: its x is its width\'s', y: BELOW_TEXT } },
  { el: 'keys strip', board: '.keys', live: '#keys', text: true },
  { el: 'key', board: '.keys .k', live: '#keys .keys-dialog .kbd', text: true },
];

for (const [id, verb, n, topic] of [['Tp-24 purge', 'Purge', 0, PURGED], ['Tp-24 delete', 'Delete', 1, DELETED]] as const) {
  test(`${id} matches the live confirm`, async ({ browser }) => {
    test.setTimeout(120_000);
    const out = await runBoard(browser, {
      id, file: 'Tp-24-Confirms',
      live: livePage(BASE_URL, `/topics/${topic}`, (p) => openConfirm(p, verb)),
      probes: [...SHELL.filter((p) => p.el !== 'keys strip' && p.el !== 'key'), ...dialogProbes(n)],
      allow: ALLOW,
    });
    expect(out.text, out.text).toBe('');
  });
}

test('Tp-24: what the confirms say is what the broker holds', async ({ page }) => {
  const t = truth().topics.find((x) => x.name === PURGED)!;
  const counts = t.ends.map((e, i) => e - t.starts[i]);
  const readers = (name: string) => truth().groups.filter((g) => (g.topics ?? []).includes(name));
  await page.goto(BASE_URL + `topics/${PURGED}`);
  await openConfirm(page, 'Purge');
  await expect(page.locator('#confirm-title')).toHaveText(`Purge topic ${PURGED}`);
  await expect(page.locator('#confirm-yes')).toHaveText(`Purge ${grouped(t.messages)} records`);
  await expect(page.locator('#confirm-msg')).toContainText(`Deletes every record in all ${t.partitions} partitions, up to the high watermark: ${counts.map(grouped).join(' + ')} = ${grouped(t.messages)} records`);
  await expect(page.locator('#confirm-msg')).toContainText('This is DeleteRecords on each partition.');
  const rows = await page.locator('#confirm-rows > span').allInnerTexts();
  expect(rows.filter((_, i) => i % 2 === 0)).toEqual(['Kept', 'Groups', 'New log start', 'Readers behind']);
  expect(rows[5]).toBe(t.ends.map(grouped).join(' / '));
  // Every group behind is named with its lag (each of the seed's groups reads one topic).
  const behind = readers(PURGED).filter((g) => g.lag > 0);
  expect(behind.length).toBeGreaterThan(0);
  for (const g of behind.slice(0, 3)) expect(rows[7]).toContain(`${g.name} (lag ${grouped(g.lag)})`);
  // Cancel has the focus, and Esc cancels.
  await expect(page.locator('#confirm-no')).toBeFocused();
  await page.keyboard.press('Escape');
  await expect(page.locator('#confirm')).toBeHidden();

  const d = truth().topics.find((x) => x.name === DELETED)!;
  await page.goto(BASE_URL + `topics/${DELETED}`);
  await openConfirm(page, 'Delete');
  await expect(page.locator('#confirm-title')).toHaveText(`Delete topic ${DELETED}`);
  await expect(page.locator('#confirm-msg')).toHaveText('The topic is removed. A new topic with the same name would get a new id and start at offset 0.');
  const drows = await page.locator('#confirm-rows > span').allInnerTexts();
  expect(drows[0]).toBe('Data');
  const parts = d.partitions === 1 ? '1 partition log' : `${d.partitions} partition logs`;
  expect(drows[1]).toMatch(new RegExp(`^all ${grouped(d.messages)} records and the ${parts} under .+ are deleted$`));
  expect(drows[2]).toBe('Offsets');
  for (const g of readers(DELETED)) expect(drows[3]).toContain(g.name);
  const subjects = truth().subjects.filter((s) => s.name === `${DELETED}-value` || s.name === `${DELETED}-key`);
  expect(drows[4]).toBe(subjects.length > 1 ? 'Schema subjects' : 'Schema subject');
  for (const s of subjects) expect(drows[5]).toContain(s.name);
  await page.keyboard.press('Escape');
});
