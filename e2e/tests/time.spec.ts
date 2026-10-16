import { execFileSync } from 'node:child_process';
import { test, expect } from '../fixtures/console';
import { kafkaAddr } from '../addr';

// One formatter for time: the server writes the instant, the browser shows it in
// its own zone. A record written at 23:59 UTC on the 14th is the 15th in Auckland
// and still the 14th in Los Angeles; the table, the inspector and the hover agree.

const AT = '2026-03-14T23:59:07Z';

function put(topic: string, value: string) {
  execFileSync('go', ['run', './e2e/put', kafkaAddr(), topic, AT, value], { cwd: '..', env: { ...process.env, GOWORK: 'off' } });
}

test.describe('a browser far ahead of UTC', () => {
  test.use({ timezoneId: 'Pacific/Auckland' });

  test('a message at 23:59 UTC is shown on the next date, and says which zone', async ({ page, createTopic, uniqueName }) => {
    const name = uniqueName('clock');
    await createTopic(name);
    put(name, 'late');
    await page.goto(`topics/${name}`);
    const row = page.locator('tr[id^="msg-"]').first();
    await expect(row).toContainText('late');
    // The table's Age (Sy-01) counts from the instant; the moment itself, in the browser's
    // zone (UTC+13 in March: 23:59 + 13h) and in UTC, is its hover.
    const t = row.locator('.cell-age time.when');
    await expect(t).toHaveAttribute('datetime', '2026-03-14T23:59:07.000Z');
    await expect(t).toHaveAttribute('title', /2026-03-15 12:59:07\.000 Pacific\/Auckland \(UTC\+13:00\) · 2026-03-14 23:59:07\.000 UTC/);
    // The inspector shows the date, which is the 15th here.
    await row.click();
    await expect(page.locator('#inspector time.when')).toHaveText('2026-03-15 12:59:07');
    // Relative times are computed in the browser, not left as the server wrote them.
    await expect(t).toHaveAttribute('data-style', 'since');
    await expect(t).toHaveText(/^\d+d \d\dh$/);
  });
});

test.describe('a browser behind UTC', () => {
  test.use({ timezoneId: 'America/Los_Angeles' });

  test('the same message is still the 14th, at 16:59', async ({ page, createTopic, uniqueName }) => {
    const name = uniqueName('clock');
    await createTopic(name);
    put(name, 'late');
    await page.goto(`topics/${name}`);
    await page.locator('tr[id^="msg-"]').first().click();
    await expect(page.locator('#inspector time.when')).toHaveText('2026-03-14 16:59:07');
    await expect(page.locator('#inspector time.when')).toHaveAttribute('title', /America\/Los_Angeles \(UTC-07:00\)/);
  });
});

test('the request table and the group timeline use the same zone', async ({ page }) => {
  await page.goto('traffic');
  const traffic = page.locator('#traffic-table .tf-rows > .r[data-row]').first().locator('time.when').first();
  await expect(traffic).toHaveAttribute('title', /\(UTC[+-]\d\d:\d\d\) · \d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3} UTC$/);
  const zone = (await traffic.getAttribute('title'))!.match(/\(UTC[+-]\d\d:\d\d\)/)![0];
  await page.goto('groups/g-timeline?tab=timeline');
  const tl = page.locator('#group-timeline li').first().locator('time.when');
  await expect(tl).toHaveAttribute('title', /\(UTC[+-]\d\d:\d\d\)/);
  expect((await tl.getAttribute('title'))!).toContain(zone);
});
