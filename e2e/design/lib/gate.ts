import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import type { Browser, Page } from '@playwright/test';
import { diffProbes, explain, measure, report, stale, type Allowed, type Diff, type Probe } from './compare';
import { openBoard, openLive, settle, type Size, type Theme } from './render';

// One board against the live console, in every place it is compared. The spec names
// the board, how to bring the live page to the board's state, the probes and the
// allow-list; this runs them and returns what nothing explains.

export type Place = { size: Size; theme: Theme };
export const placeName = (p: Place) => `${p.size}-${p.theme}`;

export type BoardRun = {
  id: string;
  /** The board's file under boards/, without .dc.html. */
  file: string;
  /** The size the board is drawn at, when it is not the place's (a 1440 board compared at 960 is not a thing: each place names its board). */
  boardSize?: Size;
  /** Places to compare (default 1440 light and 1440 dark). */
  places?: Place[];
  /** Opens the live page and brings it to the board's state. */
  live: (browser: Browser, place: Place) => Promise<Page>;
  /** Brings the board to the state compared (an open dialog that the board draws hidden, …). */
  boardState?: (page: Page) => Promise<void>;
  probes: Probe[];
  allow: Allowed[];
};

export type Outcome = { unexplained: { at: string; diffs: Diff[] }[]; stale: Allowed[]; text: string };

export const BOTH_THEMES: Place[] = [{ size: 1440, theme: 'light' }, { size: 1440, theme: 'dark' }];

export async function runBoard(browser: Browser, run: BoardRun): Promise<Outcome> {
  const used = new Set<Allowed>();
  const unexplained: Outcome['unexplained'] = [];
  const falsePer: string[] = [];
  for (const place of run.places ?? BOTH_THEMES) {
    const at = placeName(place);
    const board = await openBoard(browser, run.file, run.boardSize ?? place.size, place.theme);
    if (run.boardState) await run.boardState(board);
    const live = await run.live(browser, place);
    await settle(live);
    try {
      // DESIGN_SHOTS=<dir> keeps a picture of both sides of every comparison.
      if (process.env.DESIGN_SHOTS) {
        const base = join(process.env.DESIGN_SHOTS, `${run.id.replace(/\s+/g, '-')}-${at}`);
        await board.screenshot({ path: `${base}-board.png` });
        await live.screenshot({ path: `${base}-live.png` });
      }
      const diffs = await diffProbes(board, live, run.probes);
      const hits = new Set<Allowed>();
      const rest = explain(run.id, at, diffs, run.allow, hits);
      hits.forEach((a) => used.add(a));
      if (rest.length) unexplained.push({ at, diffs: rest });
      falsePer.push(...await checkPer(browser, run, place, [...hits]));
    } finally {
      await board.context().close();
      await live.context().close();
    }
  }
  const old = stale(run.id, run.allow, used);
  let text = '';
  for (const u of unexplained) text += `\n${run.id} at ${u.at}: ${u.diffs.length} difference(s) nothing explains:\n${report(run.id, u.at, u.diffs)}\n`;
  for (const a of old) text += `\n${run.id}: allow-list entry explains nothing: ${a.el} ${a.prop} ${JSON.stringify(a.board_value)} -> ${JSON.stringify(a.live_value)}`;
  for (const f of falsePer) text += `\n${run.id}: ${f}`;
  return { unexplained, stale: old, text };
}

/**
 * The entries that say the console draws another board's value (`per`), checked: the
 * other board is opened in the same place and the element measured there. Returns a
 * line for each entry whose board does not give the live value.
 */
async function checkPer(browser: Browser, run: BoardRun, place: Place, hits: Allowed[]): Promise<string[]> {
  const out: string[] = [];
  const pages = new Map<string, Page>();
  try {
    for (const a of hits) {
      if (!a.per) continue;
      const probe = run.probes.find((p) => p.el === a.el);
      if (!probe) { out.push(`allow-list entry for ${a.el} names no probe`); continue; }
      let page = pages.get(a.per.board);
      if (!page) {
        page = await openBoard(browser, a.per.board, 1440, place.theme);
        pages.set(a.per.board, page);
      }
      const m = await measure(page, a.per.sel ?? probe.board, a.per.nth ?? probe.nth ?? 0);
      const got = a.prop === 'presence' ? (m ? 'present' : 'absent') : m?.[a.prop] ?? '(absent)';
      if (got !== a.live_value) out.push(`allow-list entry says ${a.per.board} draws ${a.el} ${a.prop} ${JSON.stringify(a.live_value)}, and it draws ${JSON.stringify(got)} at ${placeName(place)}`);
    }
  } finally {
    for (const p of pages.values()) await p.context().close();
  }
  return out;
}

/** The live page of a place: a URL of the console, in that size and theme. */
export function livePage(base: string, path: string, after?: (p: Page) => Promise<void>) {
  return async (browser: Browser, place: Place) => {
    const page = await openLive(browser, base + path.replace(/^\//, ''), place.size, place.theme);
    if (after) await after(page);
    return page;
  };
}

// ---------- the seeded truth ----------

export type Truth = {
  topics: { name: string; partitions: number; messages: number; compact: boolean; retentionMs: number; ends: number[]; starts: number[] }[];
  groups: { name: string; state: string; protocol: string; members: number; topics: string[] | null; lag: number }[];
  subjects: { name: string; versions: number; latestId: number; type: string }[];
  acls: { Principal: string; ResourceType: string; Name: string; Pattern: string; Operation: string; Permission: string; Host: string }[] | null;
};

/** What the broker holds, as e2e/design/truth read it after the seed (global-setup.ts). */
export function truth(): Truth {
  return JSON.parse(readFileSync(join(process.env.DESIGN_TMP ?? '', 'truth.json'), 'utf8'));
}

/** A number as the console writes it: 14,429. */
export const grouped = (n: number) => n.toLocaleString('en-US');
