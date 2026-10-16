import { readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { test, expect, type Page } from '@playwright/test';
import { BASE_URL } from '../../playwright.config';
import { grouped, truth } from '../lib/gate';

// The numbers each home shows are the broker's: held to what e2e/design/truth read
// from the broker (Kafka's own requests, the registry's REST API) and, for sizes, from
// the data directory's files. A count the console gets wrong cannot agree with itself
// here.

const PORT = Number(process.env.E2E_PORT ?? 14092);
const DATA = resolve(import.meta.dirname, '..', '..', '.tmp', `data-${PORT}`);

/** What `du` would say a topic takes: every file of its partition directories. */
function topicBytes(topic: string, partitions: number): number {
  let n = 0;
  for (let p = 0; p < partitions; p++) {
    const dir = join(DATA, `${topic}-${p}`);
    for (const f of readdirSync(dir)) {
      const st = statSync(join(dir, f));
      if (st.isFile()) n += st.size;
    }
  }
  return n;
}

/** A size as the console writes it (console/disk.go diskSize). */
function diskSize(n: number): string {
  const units = ['B', 'KiB', 'MiB', 'GiB'];
  let f = n, i = 0;
  while (f >= 1024 && i < units.length - 1) { f /= 1024; i++; }
  if (i === 0) return `${n} B`;
  if (f < 10) return `${(Math.floor(f * 10) / 10).toFixed(1).replace(/\.0$/, '')} ${units[i]}`;
  return `${Math.floor(f)} ${units[i]}`;
}

async function cells(page: Page, row: string) {
  return page.locator(row).locator(':scope > span').allInnerTexts();
}

test('Tp-Home: topics, partitions, messages and sizes are the broker\'s', async ({ page }) => {
  const w = truth();
  await page.goto(BASE_URL + 'topics');
  const parts = w.topics.reduce((s, t) => s + t.partitions, 0);
  const msgs = w.topics.reduce((s, t) => s + t.messages, 0);
  // Sizes move while the broker writes its indexes; read them after the page, and allow
  // nothing but what the files say at that moment.
  const bytes = w.topics.reduce((s, t) => s + topicBytes(t.name, t.partitions), 0);
  const summary = page.locator('#topics-summary');
  await expect(summary).toContainText(`${w.topics.length} topics`);
  await expect(summary).toContainText(`${grouped(parts)} partitions`);
  await expect(summary).toContainText(`${grouped(msgs)} messages, ${diskSize(bytes)}`);
  for (const t of w.topics) {
    const c = await cells(page, `#topics-table > a.r:has(span[title="${t.name}"])`);
    expect(c.slice(0, 4), t.name).toEqual([t.name, String(t.partitions), grouped(t.messages), diskSize(topicBytes(t.name, t.partitions))]);
  }
  // The list on the left says the same numbers.
  for (const t of w.topics) await expect(page.locator(`.list .li[data-name="${t.name}"] [data-n]`)).toHaveText(grouped(t.messages));
});

test('Gr-Home: groups, members and lag are the broker\'s', async ({ page }) => {
  const w = truth();
  await page.goto(BASE_URL + 'groups');
  const withMembers = w.groups.filter((g) => g.members > 0).length;
  const members = w.groups.reduce((s, g) => s + g.members, 0);
  const lag = w.groups.reduce((s, g) => s + g.lag, 0);
  const summary = page.locator('#groups-summary');
  await expect(summary).toContainText(`${w.groups.length} groups`);
  await expect(summary).toContainText(`${withMembers} with members`);
  await expect(summary).toContainText(`${members} member`);
  await expect(summary).toContainText(`${grouped(lag)} records behind in total`);
  for (const g of w.groups) {
    const c = await cells(page, `#groups-table > a.r[data-name="${g.name}"]`);
    expect([c[0], c[2], c[3], c[5]], g.name).toEqual([g.name, g.state, String(g.members), grouped(g.lag)]);
    await expect(page.locator(`.list .li[data-name="${g.name}"] [data-n]`)).toHaveText(grouped(g.lag));
  }
});

test('Sc-Home: subjects, versions and ids are the registry\'s', async ({ page }) => {
  const w = truth();
  await page.goto(BASE_URL + 'schemas');
  const versions = w.subjects.reduce((s, x) => s + x.versions, 0);
  await expect(page.locator('#schemas-summary')).toContainText(`${w.subjects.length} subjects`);
  await expect(page.locator('#schemas-summary')).toContainText(`${versions} versions`);
  for (const s of w.subjects) {
    const c = await cells(page, `#schemas-table > a.r:has(span.b:text-is("${s.name}"))`);
    expect([c[0], c[1], c[2], c[3]], s.name).toEqual([s.name, s.type, String(s.versions), String(s.latestId)]);
  }
  await expect(page.locator('.list .list-foot')).toContainText(`${w.subjects.length} subjects, ${versions} schemas`);
});

test('Ac-Home: the ACLs are the broker\'s', async ({ page }) => {
  const w = truth();
  const acls = w.acls ?? [];
  await page.goto(BASE_URL + 'access');
  await expect(page.locator('#access-summary')).toContainText(`${acls.length} ACL`);
  await expect(page.locator('#acl-table > .r:not(.h)')).toHaveCount(acls.length);
  const shown = await page.locator('#acl-table > .r:not(.h)').evaluateAll((rows) => rows.map((r) => Array.from(r.children).map((c) => (c as HTMLElement).innerText.trim()).join('|')));
  const word = (s: string) => s.toLowerCase().replace(/_/g, ' ');
  const cap = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);
  const want = acls.map((a) => [a.Principal, cap(word(a.ResourceType)), a.Name, word(a.Pattern), cap(word(a.Operation)), a.Permission.toUpperCase(), a.Host].join('|'));
  expect(shown.sort()).toEqual(want.sort());
});

test('Cl-Home: the counts are the broker\'s', async ({ page }) => {
  const w = truth();
  await page.goto(BASE_URL + 'cluster');
  const parts = w.topics.reduce((s, t) => s + t.partitions, 0);
  // A row by its label exactly: "Partitions" is also inside "Compacted partitions".
  const count = (label: string) => page.locator('#cluster-counts a.r')
    .filter({ has: page.locator(':scope > span', { hasText: new RegExp(`^${label}$`) }) })
    .locator(':scope > span').last();
  await expect(count('Topics')).toHaveText(String(w.topics.length));
  await expect(count('Partitions')).toHaveText(String(parts));
  await expect(count('Consumer groups')).toHaveText(String(w.groups.length));
  const compacted = w.topics.filter((t) => t.compact).reduce((s, t) => s + t.partitions, 0);
  await expect(count('Compacted partitions')).toHaveText(String(compacted));
  await expect(page.locator('#cluster-sentence')).toContainText(`with ${w.topics.length} topics in ${parts} partitions, ${w.groups.length} groups`);
});
