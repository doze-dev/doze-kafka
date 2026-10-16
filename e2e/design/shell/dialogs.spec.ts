import { test, expect, type Page } from '@playwright/test';
import { BASE_URL } from '../../playwright.config';
import type { Probe } from '../lib/compare';
import { livePage, runBoard } from '../lib/gate';
import { SHELL, TEXT_W } from '../lib/probes';
import { atTop, TOPIC } from '../lib/topic';
import { ALLOW } from './allow';

// Sy-05 (the command palette, "ord" typed on a topic) and Sy-06 (the keyboard
// shortcuts over Traffic) against the live console with the same dialog open, at 1440
// in light and dark. The page behind is compared only for the top bar and the rail;
// the dialogs are compared part by part, x and y from the dialog's corner.

const SEED_RESULTS = 'the results are the seed\'s things that match "ord", not the board\'s sample: how many, and in which groups';
// What precedes the match in a name ("g-" before "ord" in g-orders-audit) is the seed's.
const AFTER_PREFIX = 'it is where "ord" is in the seed\'s name';
const IN_PAL = { board: '.modal', live: '#palette .palette' };
const IN_HELP = { board: '.modal', live: '#help .dialog' };

const SHELL_BEHIND: Probe[] = SHELL.filter((p) => !['keys strip', 'key'].includes(p.el));

const PALETTE: Probe[] = [
  ...SHELL_BEHIND,
  { el: 'scrim', board: '.scrim', live: '#palette.overlay' },
  { el: 'palette', board: '.modal', live: '#palette .palette', free: { h: SEED_RESULTS } },
  { el: 'search row', board: '.modal > div:first-child', live: '.pal-input', origin: IN_PAL },
  { el: 'search glyph', board: '.modal > div:first-child > .mut', live: '.pal-input > .mut', origin: IN_PAL, text: true },
  { el: 'search field', board: '.modal > div:first-child > input', live: '#pal-q', origin: IN_PAL },
  { el: 'count', board: '.modal > div:first-child > .note', live: '#pal-count', origin: IN_PAL, free: { w: TEXT_W, x: 'right-aligned: its x is its width\'s, and its words count the seed\'s groups' } },
  { el: 'esc', board: '.modal > div:first-child > .k', live: '.pal-input > .kbd', origin: IN_PAL, text: true },
  { el: 'results', board: '.modal > div:nth-child(2)', live: '#pal-list', origin: IN_PAL, free: { h: SEED_RESULTS } },
  { el: 'group', board: '.modal .cap', live: '#pal-list .pal-cap', origin: IN_PAL, text: true },
  { el: 'result chosen', board: '.modal > div:nth-child(2) > a', live: '#pal-list .pal-item.sel', origin: IN_PAL },
  { el: 'chip', board: '.modal > div:nth-child(2) > a .sv', live: '#pal-list .pal-item.sel .sv', origin: IN_PAL, text: true },
  { el: 'name', board: '.modal > div:nth-child(2) > a > span.m', live: '#pal-list .pal-item.sel .pal-name', origin: IN_PAL, free: { w: TEXT_W } },
  { el: 'match', board: '.modal > div:nth-child(2) > a > span.m b', live: '#pal-list .pal-item.sel .pal-name b', origin: IN_PAL, free: { w: TEXT_W, x: AFTER_PREFIX } },
  { el: 'detail', board: '.modal > div:nth-child(2) > a > .note', live: '#pal-list .pal-item.sel .pal-detail', origin: IN_PAL, free: { w: TEXT_W, x: 'it follows the name, the seed\'s' } },
  { el: 'enter', board: '.modal > div:nth-child(2) > a .k', live: '#pal-list .pal-item.sel .kbd', origin: IN_PAL, text: true },
  { el: 'result', board: '.modal > div:nth-child(2) > a', live: '#pal-list .pal-item:not(.sel)', nth: 1, liveNth: 0, origin: IN_PAL, free: { y: SEED_RESULTS } },
  { el: 'foot', board: '.modal > div:last-child', live: '.pal-foot', origin: IN_PAL, free: { y: SEED_RESULTS } },
  { el: 'foot link', board: '.modal > div:last-child > a', live: '.pal-foot > a', origin: IN_PAL, text: true, free: { y: SEED_RESULTS } },
  { el: 'foot keys', board: '.modal > div:last-child > .note', live: '.pal-foot > .note', origin: IN_PAL, text: true, free: { y: SEED_RESULTS } },
  { el: 'keys strip', board: '.keys', live: '#keys', text: true },
];

const HELP: Probe[] = [
  ...SHELL_BEHIND,
  { el: 'scrim', board: '.scrim', live: '#help.overlay' },
  { el: 'sheet', board: '.modal', live: '#help .dialog' },
  { el: 'head', board: '.modal > div:first-child', live: '#help .help-h', origin: IN_HELP },
  { el: 'title', board: '.modal h2', live: '#help-title', origin: IN_HELP, text: true },
  { el: 'lede', board: '.modal > div:first-child .note', live: '#help .help-h .note', origin: IN_HELP, text: true },
  { el: 'esc', board: '.modal > div:first-child .btn', live: '#help-close', origin: IN_HELP, text: true },
  { el: 'body', board: '.modal > div:nth-child(2)', live: '#help .help-b', origin: IN_HELP },
  { el: 'column', board: '.modal > div:nth-child(2) > .col', live: '#help .help-g', origin: IN_HELP },
  { el: 'column 2', board: '.modal > div:nth-child(2) > .col', live: '#help .help-g', nth: 1, origin: IN_HELP },
  { el: 'column 3', board: '.modal > div:nth-child(2) > .col', live: '#help .help-g', nth: 2, origin: IN_HELP },
  { el: 'cap', board: '.modal .cap', live: '#help .help-g .cap', origin: IN_HELP, text: true },
  { el: 'cap 2', board: '.modal .cap', live: '#help .help-g .cap', nth: 1, origin: IN_HELP, text: true },
  { el: 'cap 3', board: '.modal .cap', live: '#help .help-g .cap', nth: 2, origin: IN_HELP, text: true },
  { el: 'row', board: '.modal .col > .row', live: '#help .help-r', origin: IN_HELP, text: true },
  { el: 'row label', board: '.modal .col > .row > span:first-child', live: '#help .help-r .help-l', origin: IN_HELP },
  { el: 'row key', board: '.modal .col > .row .k', live: '#help .help-r .kbd', origin: IN_HELP },
  { el: 'foot', board: '.modal > div:last-child', live: '#help .help-f', origin: IN_HELP },
  { el: 'foot note', board: '.modal > div:last-child > .note', live: '#help .help-f .note', origin: IN_HELP },
  { el: 'keys strip', board: '.keys', live: '#keys', text: true },
];

async function openPalette(page: Page) {
  await atTop(page);
  await page.keyboard.press('ControlOrMeta+k');
  await expect(page.locator('#palette')).toBeVisible();
  await page.locator('#pal-q').fill('ord');
  await expect(page.locator('#pal-list .pal-item.sel')).toBeVisible();
  await page.waitForTimeout(250); // the pop-in animation.
  await page.mouse.move(0, 0);
}

/** Sy-06's "new" tag in its foot marks what is new to the design (Grounding): an annotation. */
async function dropNewTag(page: Page) {
  await page.evaluate(() => document.querySelectorAll('.modal .tag.new').forEach((e) => e.remove()));
}

async function openHelp(page: Page) {
  await atTop(page);
  await page.keyboard.press('?');
  await expect(page.locator('#help')).toBeVisible();
  await page.waitForTimeout(250);
  await page.evaluate(() => (document.activeElement as HTMLElement | null)?.blur());
}

test('Sy-05 matches the live palette, "ord" typed on a topic', async ({ browser }) => {
  test.setTimeout(120_000);
  const out = await runBoard(browser, {
    id: 'Sy-05', file: 'Sy-05-Palette', live: livePage(BASE_URL, `/topics/${TOPIC}`, openPalette), probes: PALETTE, allow: ALLOW,
  });
  expect(out.text, out.text).toBe('');
});

test('Sy-06 matches the live keyboard shortcuts over Traffic', async ({ browser }) => {
  test.setTimeout(120_000);
  const out = await runBoard(browser, {
    id: 'Sy-06', file: 'Sy-06-Keys', boardState: dropNewTag, live: livePage(BASE_URL, '/traffic', openHelp), probes: HELP, allow: ALLOW,
  });
  expect(out.text, out.text).toBe('');
});
