import { test, expect, type Page } from '@playwright/test';
import { BASE_URL } from '../../playwright.config';
import { diffProbes, explain, report, stale, type Probe } from '../lib/compare';
import { livePage, runBoard } from '../lib/gate';
import { SHELL } from '../lib/probes';
import { openBoard, openLive, settle, type Theme } from '../lib/render';
import { ALLOW } from './allow';

// Sy-07, themes and contrast. It is not a page of the console: it draws the theme
// switch beside its title, and the three token sets as specimens (a rail, pills,
// buttons, rows, callouts) with each pair's contrast ratio. So it is held three ways:
//  1. the switch, part by part, against the top bar's (where Sy-07 says it is);
//  2. each set's tokens against the console's in that theme, and every ratio the board
//     prints, recomputed from the console's own tokens (A11: contrast is measured);
//  3. each set's specimens drawn by the console's stylesheet in that theme, against
//     the same specimens drawn by the board's, element by element.

const SWITCH_SPOT = 'Sy-07 draws the switch beside its title as a specimen; the console\'s is in the top bar, where Sy-07 says it is ("a switch in the top bar overrides it")';

const SWITCH: Probe[] = [
  ...SHELL.filter((p) => !['theme switch', 'rail item on', 'keys strip', 'key'].includes(p.el)),
  { el: 'switch', board: 'main .seg', live: 'header .theme-seg', free: { x: SWITCH_SPOT, y: SWITCH_SPOT } },
  { el: 'switch item', board: 'main .seg > span:not(.on)', live: 'header .theme-seg > button:not(.on)', text: true, free: { x: SWITCH_SPOT, y: SWITCH_SPOT } },
  { el: 'switch item on', board: 'main .seg > .on', live: 'header .theme-seg > button.on', text: true, free: { x: SWITCH_SPOT, y: SWITCH_SPOT } },
  { el: 'switch item 4', board: 'main .seg > span', live: 'header .theme-seg > button', nth: 3, text: true, free: { x: SWITCH_SPOT, y: SWITCH_SPOT } },
];

/** Sy-07's "new" tags mark what is new to the design (Grounding): annotations. */
async function dropNewTags(page: Page) {
  await page.evaluate(() => document.querySelectorAll('.tag.new').forEach((e) => e.remove()));
}

test('Sy-07: the theme switch is the board\'s', async ({ browser }) => {
  test.setTimeout(120_000);
  const out = await runBoard(browser, {
    id: 'Sy-07', file: 'Sy-07-Themes', places: [{ size: 1440, theme: 'light' }], boardState: dropNewTags,
    live: livePage(BASE_URL, '/cluster'), probes: SWITCH, allow: ALLOW,
  });
  expect(out.text, out.text).toBe('');
});

const SETS: [string, Theme, string][] = [['Light', 'light', 'section.sp:not(.dk):not(.hc)'], ['Dark', 'dark', 'section.sp.dk'], ['High contrast', 'contrast', 'section.sp.hc']];
const TOKENS = ['bg', 'card', 'ink', 'mut', 'line', 'line2', 'sel', 'well', 'ok', 'warn', 'bad', 'deny', 'okbg', 'warnbg', 'badbg', 'denybg'];

/** WCAG 2.x contrast of two #rrggbb colours, to two places. */
function ratio(a: string, b: string): string {
  const lum = (hex: string) => {
    const c = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16) / 255).map((v) => (v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4));
    return 0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2];
  };
  const [x, y] = [lum(a), lum(b)].sort((p, q) => q - p);
  return ((x + 0.05) / (y + 0.05)).toFixed(2);
}

/** The tokens as rgb(), read where they apply (an element's computed custom properties, resolved by the browser). */
async function tokens(page: Page, sel: string): Promise<Record<string, string>> {
  return page.evaluate(({ sel, names }) => {
    const host = document.querySelector(sel) as HTMLElement;
    const probe = document.createElement('i');
    host.appendChild(probe);
    const out: Record<string, string> = {};
    for (const n of names) { probe.style.color = `var(--${n})`; out[n] = getComputedStyle(probe).color; }
    probe.remove();
    return out;
  }, { sel, names: TOKENS });
}

for (const [name, theme, sel] of SETS) {
  test(`Sy-07 ${name}: the tokens are the board's, and every ratio it prints is the console's`, async ({ browser }) => {
    const board = await openBoard(browser, 'Sy-07-Themes', 1440, 'light');
    const live = await openLive(browser, `${BASE_URL}cluster`, 1440, theme);
    try {
      expect(await tokens(live, 'body'), `${name} tokens`).toEqual(await tokens(board, sel));
      // "text on card, #17191c on #ffffff 17.61": each pair is the console's tokens, and the ratio is theirs.
      const rows = await board.locator(`${sel} .kv.m > span`).allInnerTexts();
      const live_ = await live.evaluate((names) => {
        const cs = getComputedStyle(document.documentElement);
        return Object.fromEntries(names.map((n) => [n, cs.getPropertyValue(`--${n}`).trim().toLowerCase()]));
      }, TOKENS);
      const pairs: Record<string, [string, string]> = { 'text on card': ['ink', 'card'], 'muted on card': ['mut', 'card'], ok: ['ok', 'okbg'], warn: ['warn', 'warnbg'], bad: ['bad', 'badbg'], deny: ['deny', 'denybg'], 'border on card': ['line', 'card'] };
      expect(rows.length).toBe(14);
      for (let i = 0; i < rows.length; i += 2) {
        const m = rows[i].match(/^(.+), (#[0-9a-f]{6}) on (#[0-9a-f]{6})$/);
        expect(m, rows[i]).not.toBeNull();
        const [fg, bg] = pairs[m![1]];
        expect([live_[fg], live_[bg]], `${name}: ${m![1]}`).toEqual([m![2], m![3]]);
        expect(ratio(live_[fg], live_[bg]), `${name}: ${m![1]}'s ratio`).toBe(rows[i + 1]);
      }
    } finally {
      await board.context().close();
      await live.context().close();
    }
  });
}

// The specimens, drawn by each stylesheet. The board's panel is copied into the live
// page (its inline styles too, which are the specimen's layout, but not the inline
// outlines Sy-07 draws on high contrast's selection: the console's own CSS must draw
// those), placed where the board places it, and measured from its corner on both sides.
const SPECIMENS: Probe[] = [
  { el: 'pill ok', board: '.pill.ok', live: '.pill.ok' },
  { el: 'pill warn', board: '.pill.warn', live: '.pill.warn' },
  { el: 'pill bad', board: '.pill.bad', live: '.pill.bad' },
  { el: 'pill deny', board: '.pill.deny', live: '.pill.deny' },
  { el: 'dot warn', board: '.pill .dot.warn', live: '.pill .dot.warn' },
  { el: 'primary', board: '.btn.pri', live: '.btn.pri' },
  { el: 'default', board: '.btn:not(.pri):not(.danger):not(.ghost)', live: '.btn:not(.pri):not(.danger):not(.ghost)' },
  { el: 'delete', board: '.btn.danger', live: '.btn.danger' },
  { el: 'ghost', board: '.btn.ghost', live: '.btn.ghost' },
  { el: 'row', board: '.card .r:not(.on)', live: '.card .r:not(.on)' },
  { el: 'row on', board: '.card .r.on', live: '.card .r.on' },
  { el: 'row refused', board: '.card .r', live: '.card .r', nth: 2 },
  { el: 'refused text', board: '.card .r .bad', live: '.card .r .bad' },
  { el: 'callout deny', board: '.callout.deny', live: '.callout.deny' },
  { el: 'callout warn', board: '.callout.warn', live: '.callout.warn' },
  { el: 'callout title', board: '.callout .b', live: '.callout .b' },
  { el: 'ratios', board: '.kv.m', live: '.kv.m' },
  { el: 'ratio', board: '.kv.m > span', live: '.kv.m > span' },
];

for (const [name, theme, sel] of SETS) {
  test(`Sy-07 ${name}: the console's stylesheet draws the board's specimens`, async ({ browser }) => {
    const board = await openBoard(browser, 'Sy-07-Themes', 1440, 'light');
    const live = await openLive(browser, `${BASE_URL}cluster`, 1440, theme);
    try {
      const html = await board.locator(sel).evaluate((e) => {
        const c = e.cloneNode(true) as HTMLElement;
        c.querySelectorAll('[style*="outline"]').forEach((x) => { (x as HTMLElement).style.outline = ''; (x as HTMLElement).style.outlineOffset = ''; });
        c.querySelectorAll('.nav, .tag.new').forEach((x) => x.remove()); // the rail's items are the rail's (the shell's probes hold them)
        return c.innerHTML;
      });
      await board.evaluate((s) => document.querySelectorAll(`${s} .nav, ${s} .tag.new`).forEach((x) => x.remove()), sel);
      const box = await board.locator(sel).boundingBox();
      await live.evaluate(({ html, box }) => {
        const s = document.createElement('section');
        s.id = 'specimen';
        s.setAttribute('style', `position:fixed;z-index:99;left:${box!.x}px;top:${box!.y}px;width:${box!.width}px;height:${box!.height}px;box-sizing:border-box;display:flex;flex-direction:column;gap:10px;padding:12px;border:1px solid var(--line);border-radius:6px;background:var(--bg);color:var(--ink);overflow:hidden`);
        s.innerHTML = html;
        document.body.appendChild(s);
      }, { html, box });
      await settle(live);
      const scoped = SPECIMENS.map((p) => ({ ...p, board: `${sel} ${p.board}`, live: `#specimen ${p.live}`, origin: { board: sel, live: '#specimen' } }));
      const id = `Sy-07 ${name}`;
      const at = `1440-${theme}`;
      const used = new Set<(typeof ALLOW)[number]>();
      const rest = explain(id, at, await diffProbes(board, live, scoped), ALLOW, used);
      const old = stale(id, ALLOW, used);
      const text = (rest.length ? `${id}: ${rest.length} difference(s) nothing explains:\n${report(id, at, rest)}` : '') + old.map((a) => `\n${id}: allow-list entry explains nothing: ${a.el} ${a.prop}`).join('');
      expect(text, text).toBe('');
      if (theme === 'contrast') {
        // Selection in high contrast gets a 2 px outline in ink (Sy-07's note), from the console's CSS.
        const o = await live.locator('#specimen .card .r.on').evaluate((e) => { const s = getComputedStyle(e); return `${s.outlineWidth} ${s.outlineStyle} ${s.outlineColor} ${s.outlineOffset}`; });
        expect(o).toBe('2px solid rgb(255, 255, 255) -2px');
      }
    } finally {
      await board.context().close();
      await live.context().close();
    }
  });
}

// "Selection in high contrast also gets a 2 px outline": every kind of selection the
// console draws, not only the specimen's row.
test('Sy-07: every selection is outlined in high contrast', async ({ browser }) => {
  const live = await openLive(browser, `${BASE_URL}topics/seed-orders?partition=0&open=39`, 1440, 'contrast');
  try {
    await expect(live.locator('#inspector-title')).toBeVisible();
    for (const sel of ['nav.rail a.nav.on', '.list .li.on', '#messages-table tr.on']) {
      const o = await live.locator(sel).first().evaluate((e) => { const s = getComputedStyle(e); return `${s.outlineWidth} ${s.outlineStyle} ${s.outlineColor}`; });
      expect(o, sel).toBe('2px solid rgb(255, 255, 255)');
    }
  } finally {
    await live.context().close();
  }
});
