import { test, expect } from '@playwright/test';
import { BASE_URL } from '../../playwright.config';
import type { Probe } from '../lib/compare';
import { livePage, runBoard, type Place } from '../lib/gate';
import { AFTER_TEXT, BRAND_X, TEXT_W } from '../lib/probes';
import { ALLOW } from './allow';

// The homes at 960 and 390. No board draws a home at those widths; Sy-03 (960) and
// Sy-04 (390) draw the shell every page has there: the top bar, the rail of chips or
// the menu, the page's padding and the keys strip (or none). Each home is held to
// those, in light and dark. The homes' own cards at 390 (a row's name, then each
// value after its column's name) are held by tests/narrow.spec.ts.

const OPEN = 'the section open is the page\'s own, so its chip is another row of the rail';

const AT_960: Probe[] = [
  { el: 'top bar', board: '.top', live: 'header.topbar' },
  { el: 'brand', board: '.brand', live: 'a.brand', free: { w: TEXT_W } },
  { el: 'brand label', board: '.brand .mut', live: '.brand .sub', free: { w: TEXT_W, x: AFTER_TEXT } },
  { el: 'jump', board: '.jump', live: '.tb-palette', free: { x: BRAND_X } },
  { el: 'jump text', board: '.jump .grow', live: '.tb-palette .grow:not([hidden])', text: true, free: { x: BRAND_X } },
  { el: 'broker pill', board: '.top .pill', live: '#live-pill', free: { w: TEXT_W, x: 'it is right-aligned after the address, whose width is the port the test broker got' } },
  { el: 'theme switch', board: '.top .seg', live: 'header .theme-seg' },
  { el: 'rail', board: 'nav.rail', live: 'nav.rail' },
  { el: 'rail item', board: 'nav.rail a.nav:not(.on)', live: 'nav.rail a.nav:not(.on)' },
  { el: 'rail item on', board: 'nav.rail a.nav.on', live: 'nav.rail a.nav.on', free: { y: OPEN } },
  { el: 'rail chip', board: 'nav.rail a.nav .sv', live: 'nav.rail a.nav .sv', text: true },
  { el: 'list', board: '.list', live: 'aside.list, nav.list' },
  { el: 'main', board: 'main.main', live: 'main.main', free: { h: 'it ends where the page\'s own parts end (the wire, when the page has one)' } },
  { el: 'keys strip', board: '.keys', live: '#keys' },
];

const AT_390: Probe[] = [
  { el: 'top bar', board: '.top', live: 'header.topbar' },
  { el: 'menu', board: '.top .btn', live: '#menu-btn' },
  { el: 'logo', board: '.top .logo', live: '.topbar .logo' },
  { el: 'title', board: '.top .b.grow', live: '#pg-title', free: { w: 'it takes the rest of the bar' } },
  { el: 'jump', board: '.top .btn:nth-of-type(2)', live: '.tb-palette' },
  { el: 'broker dot', board: '.top .dot', live: '#live-pill .dot' },
  { el: 'rail', board: 'nav.rail', live: 'nav.rail' },
  { el: 'keys strip', board: '.keys', live: '#keys' },
];

const HOMES: [string, string][] = [
  ['Tp-Home', '/topics'], ['Gr-Home', '/groups'], ['Sc-Home', '/schemas'],
  ['Tc-Home', '/trace'], ['Ac-Home', '/access'], ['Cl-Home', '/cluster'],
];
const both = (size: 960 | 390): Place[] => [{ size, theme: 'light' }, { size, theme: 'dark' }];

for (const [home, path] of HOMES) {
  test(`${home} at 960 has Sy-03's shell`, async ({ browser }) => {
    test.setTimeout(120_000);
    const out = await runBoard(browser, {
      id: `${home} at 960`, file: 'Sy-03-Narrow', boardSize: 1440, places: both(960),
      live: livePage(BASE_URL, path), probes: AT_960, allow: ALLOW,
    });
    expect(out.text, out.text).toBe('');
  });

  test(`${home} at 390 has Sy-04's shell`, async ({ browser }) => {
    test.setTimeout(120_000);
    const out = await runBoard(browser, {
      id: `${home} at 390`, file: 'Sy-04-Phone', boardSize: 1440, places: both(390),
      live: livePage(BASE_URL, path), probes: AT_390, allow: ALLOW,
    });
    expect(out.text, out.text).toBe('');
  });
}
