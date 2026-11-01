import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { test, expect } from '../fixtures/console';
import { kafkaAddr } from '../addr';
import { BASE_URL } from '../playwright.config';

// The functional group boards: Gr-04 (timeline), Gr-09 (no commits), Gr-10 (ahead), Gr-13 (a
// classic group), Gr-18 (retention). Each test makes the topic and the group it reads, so none
// waits on another spec's data. A member is a real consumer (e2e/hold); an Empty group with an
// offset past the log end is one OffsetCommit (e2e/gfix).

const build = (cmd: string, out: string) =>
  execFileSync('go', ['build', '-o', `e2e/.tmp/bin/${out}-${process.pid}`, `./e2e/${cmd}`], { cwd: '..', env: { ...process.env, GOWORK: 'off' } });

async function makeTopic(name: string, partitions = 1) {
  await fetch(new URL('new/topic', BASE_URL), { method: 'POST', body: new URLSearchParams({ name, partitions: String(partitions) }) });
}

async function produce(topic: string, n: number) {
  for (let i = 0; i < n; i++) {
    await fetch(new URL(`topics/${topic}/produce`, BASE_URL), { method: 'POST', body: new URLSearchParams({ partition: '0', partition_ctx: '0', value: `v${i}` }) });
  }
}

function hold(client: string, group: string, topic: string): Promise<ChildProcess> {
  const child = spawn(`e2e/.tmp/bin/hold-gb-${process.pid}`, [kafkaAddr(), client, group, topic], { cwd: '..', stdio: ['ignore', 'pipe', 'inherit'] });
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('the held consumer never joined its group')), 20_000);
    child.stdout!.on('data', (b) => { if (String(b).includes('ready')) { clearTimeout(t); resolve(child); } });
    child.on('exit', () => reject(new Error('the held consumer exited')));
  });
}

test.describe('a group with a member', () => {
  test.describe.configure({ mode: 'serial' });
  const TOPIC = 'grb-live-topic', GROUP = 'grb-live-group';
  let member: ChildProcess | undefined;
  test.beforeAll(async () => {
    build('hold', 'hold-gb');
    await makeTopic(TOPIC, 2);
    member = await hold('grb-client', GROUP, TOPIC);
  });
  test.afterAll(() => { member?.kill('SIGTERM'); });

  test('Gr-04: the timeline says what happened, how long it waited, and filters by kind', async ({ page }) => {
    await page.goto(`groups/${GROUP}?tab=timeline`);
    const rows = page.locator('#group-timeline .tl-row');
    const kind = (k: string) => rows.filter({ has: page.locator('.tl-k', { hasText: new RegExp(`^${k}$`) }) }).first();
    await expect(kind('joined')).toContainText('client grb-client');
    await expect(kind('new generation')).toContainText('1 member, leader');
    await expect(kind('rebalance')).toContainText('a member joined');
    await expect(page.locator('#timeline-count')).toContainText('The broker keeps the last');
    await expect(page.locator('#timeline-count')).toContainText('group event');
    await expect(page.locator('#timeline-count')).toContainText('in memory');
    // The chips: members keeps joins and leaves, problems keeps nothing here, all keeps all.
    await page.locator('#timeline-chips').getByRole('link', { name: 'members' }).click();
    await expect(page).toHaveURL(/show=members/);
    await expect(rows.first()).toContainText('joined');
    await expect(rows.filter({ hasText: 'new generation' })).toHaveCount(0);
    await expect(rows.filter({ has: page.locator('.tl-k', { hasText: /^rebalance$/ }) })).toHaveCount(0);
    await page.locator('#timeline-chips').getByRole('link', { name: 'problems' }).click();
    await expect(page.locator('#timeline-none')).toContainText('No event of this kind');
    await page.locator('#timeline-chips').getByRole('link', { name: 'all' }).click();
    await expect(rows.filter({ hasText: 'new generation' })).not.toHaveCount(0);
    await expect(page.locator('#timeline-reading')).toContainText('Each rebalance row says why');
    await expect(page.locator('#flap-pill')).toHaveCount(0); // one join is not a loop.
  });

  test('Gr-13: a classic group names its protocol, generation, leader and timeouts', async ({ page }) => {
    await page.goto(`groups/${GROUP}?tab=members`);
    const card = page.locator('#classic-facts');
    await expect(card).toContainText('JoinGroup, SyncGroup, Heartbeat');
    await expect(card.locator('.kv')).toContainText('Protocol type');
    await expect(card.locator('.kv')).toContainText('consumer');
    await expect(card.locator('.kv')).toContainText('Leader');
    await expect(card.locator('.kv')).toContainText('Session timeout');
    await expect(card).toContainText('A group on KIP-848 has an epoch per member and no leader');
    await expect(page.locator('#rebalance-wait')).toHaveCount(0); // it is not rebalancing.
  });

  test('Gr-09: members and no commits is not lag 0; the owned partitions are listed with their log end', async ({ page }) => {
    await page.goto(`groups/${GROUP}`);
    await expect(page.locator('#no-commits')).toContainText('No commits yet is not lag 0');
    const rows = page.locator('#nocommit-table .r:not(.h)');
    await expect(rows).toHaveCount(2);
    await expect(rows.first()).toContainText(TOPIC);
    await expect(rows.first()).toContainText('grb-client');
    await expect(rows.first()).toContainText('no commit yet');
    await expect(page.locator('#nocommit-table')).not.toContainText('lag');
  });
});

test.describe('groups made by a commit', () => {
  test.beforeAll(() => { build('gfix', 'gfix-gb'); });
  const commit = (group: string, topic: string, offset: number) =>
    execFileSync(`e2e/.tmp/bin/gfix-gb-${process.pid}`, ['commit', kafkaAddr(), group, topic, '0', String(offset)], { cwd: '..' });

  test('Gr-10: a committed offset past the log end is said so, with the arithmetic and without a story', async ({ page }) => {
    await makeTopic('grb-ahead-topic');
    await produce('grb-ahead-topic', 4);
    commit('grb-ahead-group', 'grb-ahead-topic', 1200);
    await page.goto('groups/grb-ahead-group');
    const note = page.locator('#ahead-note');
    await expect(note).toContainText('Ahead of log end on 1 of 1 partition');
    await expect(note).toContainText('OFFSET_OUT_OF_RANGE');
    await expect(note).toContainText('re-read the 4 that exist');
    await expect(page.locator('#lag-table')).toContainText('ahead of log end by 1,196');
    await expect(page.locator('#group-live')).not.toContainText('recreated');
    await note.getByRole('link', { name: 'Reset offsets…' }).click();
    await expect(page).toHaveURL(/tab=offsets/);
  });

  test('Gr-18: the countdown is the brokers own: when the offsets expire, and from what', async ({ page }) => {
    await makeTopic('grb-ret-topic');
    await produce('grb-ret-topic', 2);
    commit('grb-ret-group', 'grb-ret-topic', 1);
    await page.goto('groups/grb-ret-group');
    // The e2e broker keeps the default 7 days. A group that never had a member counts each offset
    // from its own commit, so the countdown has no "empty since" bar.
    const note = page.locator('#expiry-note');
    await expect(note).toContainText('Offsets are kept for now');
    await expect(note).toContainText(/expire .* · in \d days/);
    await expect(note).toContainText('7.0 d');
    await expect(note).toContainText('offsets.retention.minutes');
    await expect(note).toContainText('1 offset of this group will expire');
    await expect(note.locator('progress')).toHaveCount(0);
  });
});
