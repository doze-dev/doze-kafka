// What the gate measures of an element, on a board's render and on the live page
// alike: where it is, how big, and how it is drawn.

import type { Page } from '@playwright/test';

/** One measured element. Numbers are CSS pixels to a tenth. */
export type El = {
  /** The element's text, white space collapsed (an input's value or placeholder). */
  t: string;
  x: number; y: number; w: number; h: number;
  /** "600 12px/20px IBM Plex Mono": weight, size, line height and the first family. */
  font: string;
  color: string; bg: string;
  pad: string;
  /** The four border widths and the bottom border's colour. */
  bd: string;
  rad: string;
  gap: string;
  ls: string;
  tt: string;
  op: string;
  /** grid-template-columns as computed (the column tracks, in pixels), or "none". */
  cols: string;
  /** white-space and text-overflow: whether text wraps or is cut, and how. */
  ws: string;
  ta: string;
  /** The element's own attributes the truth checks read (datetime of a <time>). */
  attrs?: Record<string, string>;
};

export const PROPS = ['x', 'y', 'w', 'h', 'font', 'color', 'bg', 'pad', 'bd', 'rad', 'gap', 'ls', 'tt', 'op', 'cols', 'ws', 'ta', 't'] as const;
export type Prop = (typeof PROPS)[number];

// Runs in the page, for every selector at once: a live page that polls cannot change
// between two of them. The family is the first of the list, "Variable" taken off (the
// console names its variable Plex Sans "IBM Plex Sans Variable"; it is the same face).
// An element that is not drawn (display: none, or inside one) is not measured: the
// gate compares what is on the screen.
const MEASURE_ALL = (sels: string[]): Record<string, El[]> => {
  const r1 = (n: number) => Math.round(n * 10) / 10;
  const all: Record<string, El[]> = {};
  for (const sel of sels) {
    const out: El[] = [];
    for (const e of Array.from(document.querySelectorAll(sel)) as HTMLElement[]) {
      if (e.getClientRects().length === 0) continue;
      const r = e.getBoundingClientRect();
      const c = getComputedStyle(e);
      const inp = e as HTMLInputElement;
      const text = (e.tagName === 'INPUT' ? inp.value || inp.placeholder : e.innerText || '').replace(/\s+/g, ' ').trim();
      const family = c.fontFamily.split(',')[0].replace(/"/g, '').replace(/ Variable$/, '').trim();
      const attrs: Record<string, string> = {};
      const times = e.tagName === 'TIME' ? [e] : Array.from(e.querySelectorAll('time'));
      times.forEach((tm, i) => { const d = tm.getAttribute('datetime'); if (d) attrs[i ? `datetime${i}` : 'datetime'] = d; });
      for (const a of Array.from(e.attributes)) if (a.name.startsWith('data-') || a.name === 'id' || a.name === 'class') attrs[a.name] = a.value;
      out.push({
        t: text, x: r1(r.x), y: r1(r.y), w: r1(r.width), h: r1(r.height),
        font: `${c.fontWeight} ${c.fontSize}/${c.lineHeight} ${family}`,
        color: c.color, bg: c.backgroundColor, pad: c.padding,
        bd: `${c.borderTopWidth} ${c.borderRightWidth} ${c.borderBottomWidth} ${c.borderLeftWidth} ${c.borderBottomColor}`,
        rad: c.borderRadius, gap: c.gap, ls: c.letterSpacing, tt: c.textTransform, op: c.opacity,
        cols: c.display === 'grid' ? c.gridTemplateColumns : 'none',
        ws: `${c.whiteSpace} ${c.textOverflow}`, ta: c.textAlign, attrs,
      });
    }
    all[sel] = out;
  }
  return all;
};

export async function snapshot(page: Page, sels: string[]): Promise<Record<string, El[]>> {
  return page.evaluate(MEASURE_ALL, sels);
}

export async function measure(page: Page, sel: string): Promise<El[]> {
  return (await snapshot(page, [sel]))[sel];
}
