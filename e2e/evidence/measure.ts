import type { Page } from '@playwright/test';

// What is measured on a board and on the live page, by the same function. The boards
// and the console share class names for the shell (`.rail`, `.insp`, `.keys`, `.r`,
// `.pill`), not for every element, so each probe names a selector for each side.

export type Probe = { name: string; ref: string; live: string };

export const PROBES: Probe[] = [
  { name: 'top bar', ref: '.top', live: 'header.topbar' },
  { name: 'rail', ref: 'nav.rail', live: 'nav.rail' },
  { name: 'list', ref: '.list', live: 'aside.list, .list' },
  { name: 'main', ref: 'main.main', live: 'main.main' },
  { name: 'inspector', ref: '.insp', live: 'aside.insp:not([hidden])' },
  { name: 'keys strip', ref: '.keys', live: '#keys' },
  { name: 'h1', ref: 'main h1', live: 'main h1' },
  { name: 'table header row', ref: '.card .r.h, .r.h', live: 'table thead tr:first-child, .r.h' },
  { name: 'first table row', ref: '.card .r:not(.h)', live: 'table tbody tr, .r:not(.h)' },
  { name: 'tab', ref: '.tab.on', live: '.tabs .tab.on' },
  { name: 'primary button', ref: '.btn.pri', live: '.btn.pri' },
  { name: 'pill', ref: 'main .pill, .pill', live: 'main .pill, .pill' },
  { name: 'callout', ref: '.callout', live: '.callout:not([hidden])' },
  { name: 'segment', ref: '.seg', live: '.seg' },
  { name: 'dialog', ref: '.modal', live: '.overlay:not([hidden]) > *' },
];

export const TOKENS = ['--bg', '--card', '--ink', '--mut', '--line', '--line2', '--sel', '--well',
  '--ok', '--warn', '--bad', '--deny', '--okbg', '--warnbg', '--badbg', '--denybg'];

export type Measured = {
  tokens: Record<string, string>;
  probes: Record<string, Record<string, unknown> | null>;
};

/** Measures the page in the browser. `side` picks which selector of each probe to use. */
export async function measure(page: Page, side: 'ref' | 'live'): Promise<Measured> {
  return page.evaluate(({ probes, tokens, side }) => {
    const root = (side === 'ref' ? document.querySelector('.app') : document.documentElement) as HTMLElement;
    const rcs = getComputedStyle(root);
    const tk: Record<string, string> = {};
    for (const t of tokens) tk[t] = rcs.getPropertyValue(t).trim().toLowerCase();
    const r1 = (n: number) => Math.round(n * 10) / 10;
    const out: Record<string, Record<string, unknown> | null> = {};
    for (const p of probes) {
      const sel = side === 'ref' ? p.ref : p.live;
      let el: HTMLElement | null = null;
      for (const cand of Array.from(document.querySelectorAll<HTMLElement>(sel))) {
        const b = cand.getBoundingClientRect();
        if (b.width > 0 && b.height > 0) { el = cand; break; }
      }
      if (!el) { out[p.name] = null; continue; }
      const b = el.getBoundingClientRect();
      const isRow = p.name.includes('row');
      // A row's type and fill are its first cell's (a table row has neither of its own).
      const sty = getComputedStyle(isRow && el.firstElementChild ? el.firstElementChild : el);
      const cs = getComputedStyle(el);
      const entry: Record<string, unknown> = {
        w: r1(b.width), h: r1(b.height),
        font: `${sty.fontSize}/${sty.lineHeight} ${sty.fontWeight}`,
        color: sty.color, radius: cs.borderRadius, padding: cs.padding,
        background: isRow && cs.backgroundColor === 'rgba(0, 0, 0, 0)' ? sty.backgroundColor : cs.backgroundColor,
      };
      if (p.name === 'main') entry.gap = cs.rowGap;
      if (p.name === 'h1') entry.family = cs.fontFamily.split(',')[0].replace(/["']/g, '').replace(/ Variable$/, '').trim();
      if (isRow) {
        entry.columns = Array.from(el.children).map((c) => r1(c.getBoundingClientRect().width));
      }
      out[p.name] = entry;
    }
    return { tokens: tk, probes: out };
  }, { probes: PROBES, tokens: TOKENS, side });
}
