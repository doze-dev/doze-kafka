import { test, expect, type Page } from '@playwright/test';
import { BASE_URL } from '../../playwright.config';
import { livePage, runBoard, truth } from '../lib/gate';
import { HOME, SHELL } from '../lib/probes';
import { ALLOW } from './allow';

// The gate bites: a one-pixel change the board does not have, and a count the broker
// does not hold, are both reported. Each is planted in the live page after it loads
// (the console is not touched), and the test passes only if the gate fails it. Runs
// with every `task test:design`, so a change to lib/ that blunts the comparison fails.

async function plant1px(page: Page) {
  await page.addStyleTag({ content: '.list .li { height: 31px !important; }' });
}

test('a one-pixel change to a home is a difference nothing explains', async ({ browser }) => {
  test.setTimeout(120_000);
  const out = await runBoard(browser, {
    id: 'Tp-Home', file: 'Tp-Home', places: [{ size: 1440, theme: 'light' }],
    live: livePage(BASE_URL, '/topics', plant1px), probes: [...SHELL, ...HOME], allow: ALLOW,
  });
  expect(out.unexplained.length, 'the planted pixel is reported').toBe(1);
  expect(out.text).toContain(`el: 'list item', prop: 'h', board_value: "30", live_value: "31"`);
});

test('the same home with nothing planted has nothing unexplained', async ({ browser }) => {
  test.setTimeout(120_000);
  const out = await runBoard(browser, {
    id: 'Tp-Home', file: 'Tp-Home', places: [{ size: 1440, theme: 'light' }],
    live: livePage(BASE_URL, '/topics'), probes: [...SHELL, ...HOME], allow: ALLOW,
  });
  expect(out.unexplained, out.text).toEqual([]);
});

test('a wrong count in a home\'s summary fails the data check', async ({ page }) => {
  const w = truth();
  await page.goto(BASE_URL + 'topics');
  const summary = page.locator('#topics-summary');
  // As data.spec holds it: the count of topics is the broker's.
  await expect(summary).toContainText(`${w.topics.length} topics`);
  // Planted: one topic more than the broker holds.
  await summary.evaluate((e, n) => { e.innerHTML = e.innerHTML.replace(`${n} topics`, `${n + 1} topics`); }, w.topics.length);
  let failed = false;
  try {
    await expect(summary).toContainText(`${w.topics.length} topics`, { timeout: 1000 });
  } catch {
    failed = true;
  }
  expect(failed, 'the planted count is caught').toBe(true);
});
