import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Browser, Page } from '@playwright/test';

// Rendering both sides of a comparison: a board from its own HTML and CSS (the files
// under e2e/design/boards, see its README for where they came from), and a page of
// the live console, at one size and in one theme.

const HERE = dirname(fileURLToPath(import.meta.url));
export const BOARDS_DIR = resolve(HERE, '..', 'boards');
/** The repository's root. */
export const ROOT = resolve(HERE, '..', '..', '..');
const FONTS = resolve(ROOT, 'console', 'static', 'fonts');

export type Theme = 'light' | 'dark' | 'contrast';
export type Size = 1440 | 960 | 390;
export const VIEWPORT: Record<Size, { width: number; height: number }> = {
  1440: { width: 1440, height: 900 },
  960: { width: 960, height: 700 },
  390: { width: 390, height: 844 },
};

// The boards ask Google Fonts for Plex. They are given the console's own vendored
// files instead, so a difference in type is never a difference in font files.
const FONT_CSS = `
@font-face{font-family:"IBM Plex Sans";font-weight:100 700;src:url(file://${FONTS}/ibm-plex-sans-latin-wght-normal.woff2) format("woff2-variations")}
@font-face{font-family:"IBM Plex Mono";font-weight:400;src:url(file://${FONTS}/ibm-plex-mono-latin-400-normal.woff2) format("woff2")}
@font-face{font-family:"IBM Plex Mono";font-weight:500;src:url(file://${FONTS}/ibm-plex-mono-latin-500-normal.woff2) format("woff2")}
@font-face{font-family:"IBM Plex Mono";font-weight:600 700;src:url(file://${FONTS}/ibm-plex-mono-latin-600-normal.woff2) format("woff2")}`;

/**
 * Opens a board (`file` is its name without `.dc.html`) at the size it is drawn.
 * The design runtime (support.js) is not loaded: these boards have no holes for it
 * to fill. `theme: 'dark'` puts the board's own dark tokens on (`.app.dark`, which
 * con.css defines); a board drawn dark is opened as it is.
 */
export async function openBoard(browser: Browser, file: string, size: Size, theme: Theme = 'light'): Promise<Page> {
  const ctx = await browser.newContext({ viewport: VIEWPORT[size] });
  const page = await ctx.newPage();
  await page.route('**/support.js', (r) => r.fulfill({ contentType: 'text/javascript', body: '' }));
  await page.route('https://fonts.googleapis.com/**', (r) => r.fulfill({ contentType: 'text/css', body: FONT_CSS }));
  await page.goto('file://' + join(BOARDS_DIR, `${file}.dc.html`));
  if (theme === 'dark') await page.evaluate(() => document.querySelector('.app')?.classList.add('dark'));
  await page.evaluate(() => document.fonts.ready);
  return page;
}

/** Opens a page of the live console in a theme, before its first paint. */
export async function openLive(browser: Browser, url: string, size: Size, theme: Theme = 'light'): Promise<Page> {
  const ctx = await browser.newContext({ viewport: VIEWPORT[size], colorScheme: theme === 'light' ? 'light' : 'dark' });
  await ctx.addInitScript((t) => { try { localStorage.setItem('theme', t); } catch { /* private mode */ } }, theme);
  const page = await ctx.newPage();
  await page.goto(url);
  await settle(page);
  return page;
}

/** Fonts loaded, the pointer out of the way, hover and transitions over. */
export async function settle(page: Page) {
  await page.evaluate(() => document.fonts.ready);
  await page.mouse.move(0, 0);
  await page.waitForTimeout(300);
}
