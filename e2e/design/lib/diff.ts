// The differ: a board's render and the live page in the same state, element by
// element. A difference is one property of one element; it fails the gate unless the
// allow-list names it exactly (the board, the element, the property and both values)
// and points at words of plan/00-foundation.md that record why.
//
// What the gate compares, and how:
//
//   - geometry (x, y, w, h), to a tenth of a pixel: exact, no tolerance;
//   - font (weight, size, line height, family), colour, background, padding, border,
//     radius, gap, letter spacing, text transform, opacity, grid tracks, wrapping and
//     alignment: exact;
//   - text: the board's copy, where the copy is the board's (a heading, a sentence, a
//     key); where the text is data (a count, a time, a client's name), the pair names
//     the truth check that compares it with the generator's ledger instead, and the
//     spec must run that check: the differ refuses a pair whose check is not run.
//
// An allow-list entry that no difference used is stale, and fails the gate too: the
// list says exactly what differs, no more.

import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { ROOT } from './board';
import { PROPS, type El, type Prop } from './measure';

/** One pair: an element (or a list of them) on the board and its live counterpart. */
export type Pair = {
  /** The element's name in the gate's report and in the allow-list. */
  id: string;
  board: string;
  live: string;
  /** Properties that are not this pair's to compare (another pair has them). */
  skip?: readonly Prop[];
  /**
   * The text is data, compared with the ledger by the named truth check, not with the
   * board's sample; and the listed geometry follows from the text (a span's width).
   */
  data?: { truth: string; geometry?: readonly Prop[] };
  /**
   * The text is the board's copy, and the listed geometry follows from data elsewhere
   * (a count beside it that is longer than the board's sample moves it): compared with
   * the ledger by the named truth check instead.
   */
  moved?: { truth: string; props: readonly Prop[] };
  /** Compare the first n of each list (the board draws n; the page lists more). */
  n?: number;
};

export type Diff = { board: string; theme: string; width: number; element: string; index: number; prop: string; boardValue: string; liveValue: string };

export type AllowEntry = {
  board: string;
  /** "light", "dark" or "*". */
  theme: string;
  /** The viewport width, or 0 for any. */
  width: number;
  element: string;
  /** The element's index in its list, or -1 for every one of them. */
  index: number;
  prop: string;
  boardValue: string;
  liveValue: string;
  /** The record in plan/00-foundation.md this rests on ("AD6", "Deviations: Main"). */
  record: string;
  /** Words of that record, verbatim: the gate fails when the record no longer says them. */
  quote: string;
};

const FOUNDATION = join(ROOT, 'plan', '00-foundation.md');

/** Reads an allow-list and fails on an entry whose record does not say what it quotes. */
export function loadAllow(file: string): AllowEntry[] {
  const list: AllowEntry[] = JSON.parse(readFileSync(file, 'utf8'));
  const plan = readFileSync(FOUNDATION, 'utf8').replace(/\s+/g, ' ');
  const bad = list.filter((e) => !e.quote || !plan.includes(e.quote.replace(/\s+/g, ' ')) || !e.record);
  if (bad.length) {
    throw new Error(`allow-list entries whose record is not in plan/00-foundation.md:\n${bad.map((e) => `  ${e.board} ${e.element}[${e.index}].${e.prop}: «${e.quote}»`).join('\n')}`);
  }
  return list;
}

/** The selectors a list of pairs measures, on the board and on the page. */
export function selectors(pairs: Pair[]): { board: string[]; live: string[] } {
  return { board: [...new Set(pairs.map((p) => p.board))], live: [...new Set(pairs.map((p) => p.live))] };
}

/** Compares the pairs; returns every difference (allowed or not). */
export function diffPairs(b: Record<string, El[]>, l: Record<string, El[]>, pairs: Pair[], where: { board: string; theme: string; width: number }, truths: Set<string>): Diff[] {
  const out: Diff[] = [];
  const push = (element: string, index: number, prop: string, bv: unknown, lv: unknown) =>
    out.push({ ...where, element, index, prop, boardValue: String(bv), liveValue: String(lv) });
  for (const p of pairs) {
    if (p.data && !truths.has(p.data.truth)) throw new Error(`${where.board} ${p.id}: its text is said to be checked by "${p.data.truth}", which the spec does not run`);
    if (p.moved && !truths.has(p.moved.truth)) throw new Error(`${where.board} ${p.id}: its place is said to follow "${p.moved.truth}", which the spec does not run`);
    const be = b[p.board] ?? [], le = l[p.live] ?? [];
    if (be.length === 0) throw new Error(`${where.board} ${p.id}: the board has no ${p.board}`);
    const want = p.n ? Math.min(p.n, be.length) : be.length;
    if (le.length < want || (!p.n && le.length !== be.length)) push(p.id, -1, 'count', be.length, le.length);
    const n = Math.min(want, le.length);
    for (let i = 0; i < n; i++) {
      for (const prop of PROPS) {
        if (p.skip?.includes(prop)) continue;
        if (p.data && (prop === 't' || p.data.geometry?.includes(prop))) continue;
        if (p.moved?.props.includes(prop)) continue;
        const bv = be[i][prop], lv = le[i][prop];
        // A border of no width has no colour worth comparing.
        if (prop === 'bd' && /^0px 0px 0px 0px/.test(String(bv)) && /^0px 0px 0px 0px/.test(String(lv))) continue;
        if (bv !== lv) push(p.id, i, prop, bv, lv);
      }
    }
  }
  return out;
}

/** Splits the differences into the allowed and the failing, and finds stale entries. */
export function judge(diffs: Diff[], allow: AllowEntry[], scope: { board: string; theme: string; width: number }) {
  const inScope = allow.filter((a) => a.board === scope.board && (a.theme === '*' || a.theme === scope.theme) && (a.width === 0 || a.width === scope.width));
  const used = new Set<AllowEntry>();
  const failing: Diff[] = [];
  for (const d of diffs) {
    const a = inScope.find((e) => e.element === d.element && (e.index === -1 || e.index === d.index) && e.prop === d.prop && e.boardValue === d.boardValue && e.liveValue === d.liveValue);
    if (a) used.add(a);
    else failing.push(d);
  }
  const stale = inScope.filter((a) => !used.has(a));
  return { failing, stale };
}

export function report(ds: Diff[]): string {
  return ds.map((d) => `  ${d.board} ${d.theme} ${d.width}: ${d.element}[${d.index}].${d.prop}: board «${d.boardValue}» live «${d.liveValue}»`).join('\n');
}

/** An allow-list entry for a difference, to paste into the list once its record is written. */
export function entryFor(d: Diff): AllowEntry {
  return { board: d.board, theme: d.theme, width: d.width, element: d.element, index: d.index, prop: d.prop, boardValue: d.boardValue, liveValue: d.liveValue, record: '', quote: '' };
}
