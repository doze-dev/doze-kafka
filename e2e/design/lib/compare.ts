import type { Page } from '@playwright/test';

// The comparison itself. A probe names one element on each side (a selector for the
// board, one for the live page, and which visible match); both are measured by the
// same function and every property is compared exactly. What differs must be on the
// board's allow-list, as that exact pair of values, with the reason it is allowed.
// An allow-list entry that no longer matches a difference is an error too: the list
// says only what is true today.

/** Every property measured. Lengths are CSS pixels rounded to a tenth. */
export const PROPS = ['x', 'y', 'w', 'h', 'font', 'ls', 'tt', 'color', 'bg', 'padding', 'margin', 'gap',
  'border', 'radius', 'display', 'tracks', 'opacity', 'deco', 'text'] as const;
export type Prop = (typeof PROPS)[number];
export type Measured = Partial<Record<Prop, string>> | null;

export type Probe = {
  /** The name the allow-list and the report use. */
  el: string;
  board: string;
  live: string;
  /** Which visible match on the board side (default 0); `liveNth` on the live side (default nth). */
  nth?: number;
  liveNth?: number;
  /**
   * Properties this probe does not compare, each with the reason. Used only where a
   * value cannot be equal by construction (a width that is its text, and the text is
   * the seed's, not the board's sample). Every one is printed in the report.
   */
  free?: Partial<Record<Prop, string>>;
  /** Compare the element's text (default: no; data on the two sides differs). */
  text?: boolean;
  /**
   * Measure x and y from the top left of another element (its first visible match),
   * not the page: for a part of a box the board draws somewhere else (a dialog drawn
   * as a specimen at the side, opened centred by the console).
   */
  origin?: { board: string; live: string; nth?: number; liveNth?: number };
};

export type Allowed = {
  board: string;
  el: string;
  prop: Prop | 'presence';
  /** The value on the board, as measured, and on the live page. */
  board_value: string;
  live_value: string;
  /** Why the difference is allowed; it must be true. */
  reason: string;
  /** Where it holds: "1440-light", "1440-dark", "960-light"…; absent: everywhere it is measured. */
  at?: string[];
  /**
   * The boards disagree, and the console draws another board's value: that board's
   * file (without .dc.html), and the selector there when it is not the probe's. The
   * gate measures it and the entry fails unless that board gives the live value, so
   * "as Tp-05 draws it" is checked, not taken on trust.
   */
  per?: { board: string; sel?: string; nth?: number };
};

export type Diff = { el: string; prop: Prop | 'presence'; board: string; live: string };

/** Measures the nth visible element matching sel. */
export async function measure(page: Page, sel: string, nth = 0, origin?: { sel: string; nth: number }): Promise<Measured> {
  return page.evaluate(({ sel, nth, origin }) => {
    const vis = Array.from(document.querySelectorAll<HTMLElement>(sel)).filter((e) => {
      const b = e.getBoundingClientRect();
      return b.width > 0 && b.height > 0 && getComputedStyle(e).visibility !== 'hidden';
    });
    const el = vis[nth];
    if (!el) return null;
    const cs = getComputedStyle(el);
    const b = el.getBoundingClientRect();
    let ox = 0, oy = 0;
    if (origin) {
      const o = Array.from(document.querySelectorAll<HTMLElement>(origin.sel)).filter((e) => e.getBoundingClientRect().width > 0)[origin.nth];
      if (o) { const ob = o.getBoundingClientRect(); ox = ob.x; oy = ob.y; }
    }
    const r = (n: number) => String(Math.round(n * 10) / 10);
    const fam = cs.fontFamily.split(',')[0].replace(/["']/g, '').replace(/ Variable$/, '').trim();
    const bw = [cs.borderTopWidth, cs.borderRightWidth, cs.borderBottomWidth, cs.borderLeftWidth].map(parseFloat);
    const bc = [cs.borderTopColor, cs.borderRightColor, cs.borderBottomColor, cs.borderLeftColor];
    return {
      x: r(b.x - ox), y: r(b.y - oy), w: r(b.width), h: r(b.height),
      font: `${fam} ${cs.fontWeight} ${cs.fontSize}/${cs.lineHeight}`,
      ls: cs.letterSpacing, tt: cs.textTransform, color: cs.color, bg: cs.backgroundColor,
      padding: cs.padding, margin: cs.margin, gap: `${cs.rowGap} ${cs.columnGap}`,
      border: bw.map((w, i) => (w ? `${w} ${bc[i]}` : '0')).join(' | '),
      radius: cs.borderRadius, display: cs.display,
      tracks: cs.display.includes('grid') ? cs.gridTemplateColumns : '',
      opacity: cs.opacity, deco: cs.textDecorationLine,
      text: (el.innerText || '').replace(/\s+/g, ' ').trim(),
    };
  }, { sel, nth, origin });
}

/** The differences between the two pages over a list of probes. */
export async function diffProbes(board: Page, live: Page, probes: Probe[]): Promise<Diff[]> {
  const out: Diff[] = [];
  for (const p of probes) {
    const o = p.origin;
    const a = await measure(board, p.board, p.nth ?? 0, o && { sel: o.board, nth: o.nth ?? 0 });
    const b = await measure(live, p.live, p.liveNth ?? p.nth ?? 0, o && { sel: o.live, nth: o.liveNth ?? o.nth ?? 0 });
    if (!a && !b) continue;
    if (!a || !b) {
      out.push({ el: p.el, prop: 'presence', board: a ? 'present' : 'absent', live: b ? 'present' : 'absent' });
      continue;
    }
    for (const k of PROPS) {
      if (k === 'text' && !p.text) continue;
      if (p.free && k in p.free) continue;
      if (a[k] !== b[k]) out.push({ el: p.el, prop: k, board: a[k] ?? '', live: b[k] ?? '' });
    }
  }
  return out;
}

/**
 * Sorts differences into the explained (an allow-list entry for this board, element,
 * property and both values, at this place) and the rest. Returns the unexplained and
 * marks which entries were used.
 */
export function explain(boardId: string, at: string, diffs: Diff[], allow: Allowed[], used: Set<Allowed>): Diff[] {
  const rest: Diff[] = [];
  for (const d of diffs) {
    const hit = allow.find((a) => a.board === boardId && a.el === d.el && a.prop === d.prop &&
      a.board_value === d.board && a.live_value === d.live && (!a.at || a.at.includes(at)));
    if (hit) used.add(hit);
    else rest.push(d);
  }
  return rest;
}

/** The entries for a board that explained nothing in the places they were checked. */
export function stale(boardId: string, allow: Allowed[], used: Set<Allowed>): Allowed[] {
  return allow.filter((a) => a.board === boardId && !used.has(a));
}

/** One line per difference, ready to paste into an allow-list if it is true. */
export function report(boardId: string, at: string, diffs: Diff[]): string {
  return diffs.map((d) => `{ board: '${boardId}', el: '${d.el}', prop: '${d.prop}', board_value: ${JSON.stringify(d.board)}, live_value: ${JSON.stringify(d.live)}, at: ['${at}'], reason: '' },`).join('\n');
}
