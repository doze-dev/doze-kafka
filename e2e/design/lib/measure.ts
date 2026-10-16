// What the gate measures of an element, on a board's render and on the live page
// alike. There is one measuring function (RAW_ALL, run in the page); the two gates
// read it through the view they compare by:
//
//   - `snapshot` (the Traffic gate): every drawn match of each selector, as `El`, with
//     the property names of traffic/allow.json;
//   - `measure` in compare.ts (the shell and homes gates): the nth visible match, as
//     `Measured`, with the property names of shell/allow.ts and homes/allow.ts.
//
// The views differ in which properties they compare and how they write them (that is
// what the allow-lists' entries are in), never in what was measured.

import type { Page } from '@playwright/test';

/** What the page says of one element, unformatted. Lengths are CSS pixels. */
export type Raw = {
  /** Where it is, as the browser has it. */
  rx: number; ry: number;
  /** Position and size, rounded to a tenth. */
  x: number; y: number; w: number; h: number;
  /** Takes up room and is not visibility:hidden (the shell views' "visible"). */
  box: boolean;
  /** Is drawn at all: has a layout box (the Traffic view's "drawn"). */
  drawn: boolean;
  tag: string;
  /** innerText, white space collapsed; and an input's value or placeholder. */
  inner: string;
  field: string;
  family: string; weight: string; size: string; lineHeight: string;
  color: string; bg: string; padding: string; margin: string;
  gap: string; rowGap: string; columnGap: string;
  /** The four border widths and colours, top right bottom left. */
  bw: string[]; bc: string[];
  radius: string; ls: string; tt: string; opacity: string; deco: string;
  display: string; gridCols: string;
  ws: string; textOverflow: string; ta: string;
  attrs: Record<string, string>;
};

// Runs in the page, for every selector at once: a live page that polls cannot change
// between two of them. The family is the first of the list, quotes and "Variable" taken
// off (the console names its variable Plex Sans "IBM Plex Sans Variable"; it is the same
// face).
const RAW_ALL = (sels: string[]): Record<string, Raw[]> => {
  const r1 = (n: number) => Math.round(n * 10) / 10;
  const all: Record<string, Raw[]> = {};
  for (const sel of sels) {
    const out: Raw[] = [];
    for (const e of Array.from(document.querySelectorAll(sel)) as HTMLElement[]) {
      const b = e.getBoundingClientRect();
      const c = getComputedStyle(e);
      const inp = e as HTMLInputElement;
      const clean = (s: string) => s.replace(/\s+/g, ' ').trim();
      const attrs: Record<string, string> = {};
      const times = e.tagName === 'TIME' ? [e] : Array.from(e.querySelectorAll('time'));
      times.forEach((tm, i) => { const d = tm.getAttribute('datetime'); if (d) attrs[i ? `datetime${i}` : 'datetime'] = d; });
      for (const a of Array.from(e.attributes)) if (a.name.startsWith('data-') || a.name === 'id' || a.name === 'class') attrs[a.name] = a.value;
      out.push({
        rx: b.x, ry: b.y, x: r1(b.x), y: r1(b.y), w: r1(b.width), h: r1(b.height),
        box: b.width > 0 && b.height > 0 && c.visibility !== 'hidden',
        drawn: e.getClientRects().length > 0,
        tag: e.tagName,
        inner: clean(e.innerText || ''),
        field: e.tagName === 'INPUT' ? clean(inp.value || inp.placeholder) : '',
        family: c.fontFamily.split(',')[0].replace(/["']/g, '').replace(/ Variable$/, '').trim(),
        weight: c.fontWeight, size: c.fontSize, lineHeight: c.lineHeight,
        color: c.color, bg: c.backgroundColor, padding: c.padding, margin: c.margin,
        gap: c.gap, rowGap: c.rowGap, columnGap: c.columnGap,
        bw: [c.borderTopWidth, c.borderRightWidth, c.borderBottomWidth, c.borderLeftWidth],
        bc: [c.borderTopColor, c.borderRightColor, c.borderBottomColor, c.borderLeftColor],
        radius: c.borderRadius, ls: c.letterSpacing, tt: c.textTransform, opacity: c.opacity, deco: c.textDecorationLine,
        display: c.display, gridCols: c.gridTemplateColumns,
        ws: c.whiteSpace, textOverflow: c.textOverflow, ta: c.textAlign,
        attrs,
      });
    }
    all[sel] = out;
  }
  return all;
};

/** Every match of each selector, unformatted. */
export async function raw(page: Page, sels: string[]): Promise<Record<string, Raw[]>> {
  return page.evaluate(RAW_ALL, sels);
}

// ---------- the Traffic gate's view ----------

/** One measured element, as traffic/allow.json writes its values. */
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

const asEl = (r: Raw): El => ({
  t: r.tag === 'INPUT' ? r.field : r.inner,
  x: r.x, y: r.y, w: r.w, h: r.h,
  font: `${r.weight} ${r.size}/${r.lineHeight} ${r.family}`,
  color: r.color, bg: r.bg, pad: r.padding,
  bd: `${r.bw.join(' ')} ${r.bc[2]}`,
  rad: r.radius, gap: r.gap, ls: r.ls, tt: r.tt, op: r.opacity,
  cols: r.display === 'grid' ? r.gridCols : 'none',
  ws: `${r.ws} ${r.textOverflow}`, ta: r.ta, attrs: r.attrs,
});

/** The drawn matches of each selector (an element that is not drawn is not on the screen). */
export async function snapshot(page: Page, sels: string[]): Promise<Record<string, El[]>> {
  const all = await raw(page, sels);
  const out: Record<string, El[]> = {};
  for (const sel of sels) out[sel] = all[sel].filter((r) => r.drawn).map(asEl);
  return out;
}
