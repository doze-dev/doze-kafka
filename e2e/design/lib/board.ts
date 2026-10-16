// The board renderer: a board of the design (a `.dc.html` file under
// e2e/design/boards/) as a plain page a browser can open and measure.
//
// A board file is the design tool's: its own <style> block (the tokens and parts the
// board draws with), then its `<div class="app">…</div>` inside an <x-dc> wrapper that
// needs the tool's script. The renderer keeps the style and the app and drops the
// rest: the wrapper, the tool's script and the Google Fonts link. The fonts are the
// console's own files (console/static/fonts), so a difference of a pixel is the page's
// and not a font's. The dark theme is the board's own `.app.dark` tokens.
//
// One thing the renderer cannot make the same: the boards ask for IBM Plex Mono 700
// (the logo, the section chips, the step numbers) and the console ships 400, 500 and
// 600, so those few glyphs are drawn bold by the browser from 600 on both sides.

import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

// The suite runs from e2e/ (playwright.config.ts).
export const ROOT = resolve(process.cwd(), '..');
export const BOARDS = join(ROOT, 'e2e', 'design', 'boards');
const OUT = join(ROOT, 'e2e', '.tmp', 'design-boards');
const FONTS = join(ROOT, 'console', 'static', 'fonts');

export type Theme = 'light' | 'dark';

function fontFaces(): string {
  const f = (file: string) => `url(file://${join(FONTS, file)}) format("woff2")`;
  return [
    `@font-face{font-family:"IBM Plex Sans";font-weight:100 700;src:url(file://${join(FONTS, 'ibm-plex-sans-latin-wght-normal.woff2')}) format("woff2-variations")}`,
    `@font-face{font-family:"IBM Plex Mono";font-weight:400;src:${f('ibm-plex-mono-latin-400-normal.woff2')}}`,
    `@font-face{font-family:"IBM Plex Mono";font-weight:500;src:${f('ibm-plex-mono-latin-500-normal.woff2')}}`,
    `@font-face{font-family:"IBM Plex Mono";font-weight:600;src:${f('ibm-plex-mono-latin-600-normal.woff2')}}`,
  ].join('');
}

/** The board's HTML as a standalone page: its style and its app. */
export function boardHTML(name: string, theme: Theme): string {
  const s = readFileSync(join(BOARDS, `${name}.dc.html`), 'utf8');
  const css = s.match(/<style>[\s\S]*?<\/style>/);
  if (!css) throw new Error(`${name}: the board has no <style>`);
  const from = s.indexOf('<div class="app'), to = s.indexOf('</x-dc>');
  if (from < 0 || to < 0) throw new Error(`${name}: the board has no <div class="app"> inside <x-dc>`);
  let body = s.slice(from, to);
  if (theme === 'dark') body = body.replace('<div class="app', '<div class="app dark');
  return `<!doctype html><html><head><meta charset="utf-8"><style>${fontFaces()}</style>${css[0]}</head><body>${body}</body></html>`;
}

/** Writes the board's page and returns its file:// address. */
export function boardURL(name: string, theme: Theme): string {
  mkdirSync(OUT, { recursive: true });
  const file = join(OUT, `${name}-${theme}.html`);
  writeFileSync(file, boardHTML(name, theme));
  return 'file://' + file;
}
