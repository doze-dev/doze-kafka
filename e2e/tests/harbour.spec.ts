import { test, expect } from '../fixtures/console';

// The harbour world (seed/harbour.go) is the fixture the design's boards are drawn
// from. One test per section: the first screen shows the counts the board shows.
// Everything in it is named harbour-*, so the counts are not disturbed by what other
// specs create beside it.

const TRACE = '5bd66ef5095369c7b0d1f8f4bd33716a';

test('Topics: seven harbour topics, one of them with three partitions', async ({ page }) => {
  await page.goto('topics');
  await expect(page.locator('.listpane .li[data-name^="harbour-"]')).toHaveCount(7);
  await page.goto('topics/harbour-arrivals');
  await expect(page.locator('.idband h1')).toHaveText('harbour-arrivals');
});

test('Groups: four harbour groups', async ({ page }) => {
  await page.goto('groups');
  await expect(page.locator('.listpane .li[data-name^="harbour-"]')).toHaveCount(4);
});

test('Schemas: five subjects, and the soft-deleted one is not listed', async ({ page }) => {
  await page.goto('schemas');
  const rows = page.locator('.listpane .li[data-name^="harbour-"]');
  await expect(rows).toHaveCount(5);
  await expect(page.locator('.listpane .li[data-name="harbour-retired-value"]')).toHaveCount(0);
});

test('Access: six ACLs', async ({ page }) => {
  await page.goto('access');
  await expect(page.locator('.cpanel .crow2')).toHaveCount(6);
});

test('Traffic: five harbour clients are in the requests', async ({ page }) => {
  await page.goto('traffic');
  const clients = new Set<string>();
  for (const cell of await page.locator('#traffic-table tbody tr').allInnerTexts()) {
    for (const m of cell.matchAll(/harbour-[a-z-]+/g)) {
      if (/^harbour-(gate|crane|pilot-boat|customs-scanner|billing-sync)$/.test(m[0])) clients.add(m[0]);
    }
  }
  expect([...clients].sort()).toEqual(['harbour-billing-sync', 'harbour-crane', 'harbour-customs-scanner', 'harbour-gate', 'harbour-pilot-boat']);
});

test('Trace: one trace crosses three topics', async ({ page }) => {
  // The records carry the fixed times of the world (06:01 onwards on 14 March), so the
  // scan is pointed at them and given three hours either side.
  const at = Date.UTC(2026, 2, 14, 7, 0, 0);
  await page.goto(`trace?id=${TRACE}&at=${at}&window=3h`);
  await expect(page.locator('#trace-live')).not.toHaveAttribute('data-live', /.+/, { timeout: 15_000 });
  await expect(page.locator('#trace-list li')).toHaveCount(3);
});
