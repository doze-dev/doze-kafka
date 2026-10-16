import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { test, expect, type Browser, type Page } from '@playwright/test';
import { BASE_URL, KAFKA_ADDR } from '../playwright.config';
import { BOARDS, type BoardDef, type Size } from './boards';
import { measure, PROBES, type Measured } from './measure';

// The evidence run: drives the live console to each board's state and saves
// screenshots (light and dark, at 1440 and where the board has one at 960 or 390)
// into plan/evidence/<stage>/<board>-<theme>.png, the board's own render beside them
// (<board>-ref.png, when DESIGN_DIR holds the boards), the measurements of both in
// plan/evidence/measurements.json, and the index plan/evidence/README.md.
//
//   task evidence                     (from the repository root)
//   DESIGN_DIR=/path/to/project task evidence    (to render the boards as well)
//
// DESIGN_DIR is a folder with the design artifact's project/ files (con.css and the
// *.dc.html boards), saved with the Artifact tool's `read` action. Without it the
// reference renders and measurements already in plan/evidence are kept.

const ROOT = resolve(process.cwd(), '..');
const OUT = join(ROOT, 'plan', 'evidence');
const MEAS = join(OUT, 'measurements.json');
const DESIGN_DIR = process.env.DESIGN_DIR ? resolve(process.env.DESIGN_DIR) : '';
const FONTS = join(ROOT, 'console', 'static', 'fonts');
const SIZES: Record<Size, [number, number]> = { '1440': [1440, 900], '960': [960, 700], '390': [390, 844] };
const PORT = Number(process.env.E2E_PORT ?? 14092);

type Theme = 'light' | 'dark' | 'contrast';
type MeasFile = Record<string, { ref?: Measured; live?: Record<string, Measured> }>;

function loadMeas(): MeasFile {
  try { return JSON.parse(readFileSync(MEAS, 'utf8')); } catch { return {}; }
}
function saveMeas(id: string, patch: { ref?: Measured; live?: Record<string, Measured> }) {
  const all = loadMeas();
  const cur = all[id] ?? {};
  if (patch.ref) cur.ref = patch.ref;
  if (patch.live) cur.live = { ...(cur.live ?? {}), ...patch.live };
  all[id] = cur;
  const sorted = Object.fromEntries(Object.entries(all).sort(([a], [b]) => a.localeCompare(b)));
  mkdirSync(OUT, { recursive: true });
  writeFileSync(MEAS, JSON.stringify(sorted, null, 1) + '\n');
}

const board = (id: string): BoardDef => BOARDS.find((b) => b.id === id)!;
const shotPath = (b: BoardDef, size: Size, theme: Theme) => {
  const dir = join(OUT, b.stage);
  mkdirSync(dir, { recursive: true });
  const sz = size === '1440' ? '' : `-${size}`;
  return join(dir, `${b.id}${sz}-${theme}.png`);
};

async function settle(page: Page) {
  await page.evaluate(() => document.fonts.ready);
  await page.mouse.move(0, 0);
  await page.waitForTimeout(400);
}

/** Opens a page of `base` in a context of this size and theme, drives it, and takes it. */
async function shoot(browser: Browser, b: BoardDef, size: Size, theme: Theme, base: string,
  drive: (page: Page) => Promise<void>): Promise<void> {
  const [width, height] = SIZES[size];
  const ctx = await browser.newContext({ viewport: { width, height }, colorScheme: theme === 'light' ? 'light' : 'dark', baseURL: base });
  try {
    await ctx.addInitScript((t) => { try { localStorage.setItem('theme', t); } catch { /* none */ } }, theme);
    const page = await ctx.newPage();
    await drive(page);
    await record(page, b, size, theme);
  } finally {
    await ctx.close();
  }
}

async function record(page: Page, b: BoardDef, size: Size, theme: Theme) {
  await settle(page);
  await page.screenshot({ path: shotPath(b, size, theme) });
  saveMeas(b.id, { live: { [`${size}-${theme}`]: await measure(page, 'live') } });
}

// ---- the states --------------------------------------------------------------------

const TOPIC_MSG = 'topics/seed-orders?partition=0&open=5';
const CLIENT = 'ev-hold';
const QUIET = 'ev-quiet';
const GROUP = 'ev-hold-group';

const open = async (page: Page, path: string) => { await page.goto(path); await expect(page.locator('main')).toBeVisible(); };

const DRIVERS: Record<string, (page: Page, size: Size) => Promise<void>> = {
  'Sy-01': async (p) => { await open(p, TOPIC_MSG); await expect(p.locator('#inspector')).toBeVisible(); },
  'Sy-02': async (p) => { await open(p, TOPIC_MSG); await expect(p.locator('#inspector')).toBeVisible(); },
  'Sy-03': async (p) => { await open(p, TOPIC_MSG); await expect(p.locator('#inspector')).toBeVisible(); },
  'Sy-04': async (p) => { await open(p, TOPIC_MSG); await expect(p.locator('#inspector')).toBeVisible(); },
  'Sy-05': async (p) => {
    await open(p, 'topics/seed-orders');
    await p.locator('#palette-open').click();
    await expect(p.locator('#pal-q')).toBeFocused();
    await p.keyboard.type('ord');
    await expect(p.locator('.pal-item').first()).toBeVisible();
  },
  'Sy-06': async (p) => {
    await open(p, 'traffic');
    await p.locator('main h1').click();
    await p.keyboard.press('?');
    await expect(p.locator('#help')).toBeVisible();
  },
  'Sy-07': async (p) => { await open(p, 'topics'); },
  'Tp-Home': async (p) => { await open(p, 'topics'); },
  'Gr-Home': async (p) => { await open(p, 'groups'); },
  'Sc-Home': async (p) => { await open(p, 'schemas'); },
  'Tc-Home': async (p) => { await open(p, 'trace'); },
  'Ac-Home': async (p) => { await open(p, 'access'); },
  'Cl-Home': async (p) => { await open(p, 'cluster'); },
  'Tp-24': async (p) => {
    await open(p, 'topics/seed-orders');
    await p.getByRole('button', { name: 'More actions' }).click();
    await p.getByRole('button', { name: 'Purge' }).first().click();
    await expect(p.locator('#confirm')).toBeVisible();
  },
  Main: async (p) => { await open(p, 'traffic'); await expect(p.locator('#traffic-table .tf-rows > .r[data-row]').first()).toBeVisible(); },
  'Traf-02': async (p) => { await open(p, 'traffic?topic=seed-orders'); await expect(p.locator('#touched')).toBeVisible(); },
  'Traf-03': async (p) => { await open(p, 'traffic?kind=errors'); await expect(p.locator('#error-summary')).toBeVisible(); },
  'Traf-04': async (p) => {
    await open(p, 'traffic?kind=produce&topic=seed-orders');
    await p.locator('#traffic-table .tf-rows > .r[data-row]').first().click();
    await expect(p.locator('#request-inspector')).toBeVisible();
  },
  'Traf-05': async (p) => {
    await open(p, `traffic?q=${GROUP}&kind=groups`);
    const folded = p.locator('#traffic-table .tf-rows > .r[data-row]', { hasText: 'Heartbeat' }).filter({ hasText: /×\d+/ }).first();
    await expect(folded).toBeVisible({ timeout: 30_000 });
    await folded.click();
    await expect(p.locator('#fold-ticks')).toBeVisible();
  },
  'Traf-06': async (p) => { await open(p, 'traffic?view=clients'); await expect(p.locator('#clients-table .tf-rows > .r[data-row]').first()).toBeVisible(); },
  'Traf-07': async (p) => {
    await open(p, 'traffic?view=clients');
    const row = p.locator('#clients-table .tf-rows > .r[data-row]', { hasText: CLIENT }).first();
    await expect(row).toBeVisible();
    await expect(async () => {
      await p.keyboard.press('Escape');
      await row.click();
      await expect(p.locator('#client-inspector .well.tf-metrics')).toBeVisible({ timeout: 2_000 });
    }).toPass({ timeout: 40_000, intervals: [1_000] });
  },
  'Traf-08': async (p) => {
    await open(p, 'traffic');
    await p.locator('#traffic-controls [data-live-toggle]').click();
    await expect(p.locator('#paused-banner')).toBeVisible();
    await expect(async () => {
      expect(Number((await p.locator('#paused-banner [data-new-n]').textContent())?.replace(/\D/g, ''))).toBeGreaterThan(0);
    }).toPass({ timeout: 20_000 });
  },
  'Traf-11': async (p) => {
    await open(p, 'traffic?view=clients');
    const row = p.locator('#clients-table .tf-rows > .r[data-row]', { hasText: QUIET }).first();
    await expect(row).toBeVisible();
    await row.click();
    await expect(p.locator('#no-metrics')).toBeVisible();
  },
  'Traf-12': async (p) => {
    await open(p, `traffic?group=${GROUP}&kind=groups`);
    await expect(p.locator('#traffic-story .tf-rebalance').first()).toBeVisible();
  },
};

// ---- processes ---------------------------------------------------------------------

const procs: ChildProcess[] = [];
const bin = (n: string) => join(ROOT, 'e2e', '.tmp', 'bin', n);

async function hold(addr: string, client: string, group: string, topic: string, env: Record<string, string>, exe: string) {
  const p = spawn(exe, [addr, client, group, topic], { cwd: ROOT, stdio: ['ignore', 'pipe', 'inherit'], env: { ...process.env, ...env } });
  procs.push(p);
  await new Promise<void>((resolveP, reject) => {
    const t = setTimeout(() => reject(new Error(`${client} never joined its group`)), 30_000);
    p.stdout!.on('data', (c) => { if (String(c).includes('ready')) { clearTimeout(t); resolveP(); } });
    p.on('exit', () => reject(new Error(`${client} exited`)));
  });
  return p;
}

test.describe.configure({ mode: 'serial' });

let holdExe = '';
test.beforeAll(async () => {
  mkdirSync(join(ROOT, 'e2e', '.tmp', 'bin'), { recursive: true });
  holdExe = bin(`hold-evidence-${process.pid}`);
  execFileSync('go', ['build', '-o', holdExe, './e2e/hold'], { cwd: ROOT, env: { ...process.env, GOWORK: 'off' } });
  await hold(KAFKA_ADDR, CLIENT, GROUP, 'seed-orders', { HOLD_TELEMETRY: '1' }, holdExe);
  await hold(KAFKA_ADDR, QUIET, 'ev-quiet-group', 'seed-orders', { HOLD_NO_METRICS: '1' }, holdExe);
});
test.afterAll(() => { procs.forEach((p) => p.kill('SIGTERM')); });

// ---- the boards on the seeded broker -----------------------------------------------

const ON_SEEDED = BOARDS.filter((b) => !['Sy-08', 'Traf-09', 'Traf-10'].includes(b.id));
for (const b of ON_SEEDED) {
  test(`${b.id}: ${b.state}`, async ({ browser }) => {
    test.setTimeout(180_000);
    for (const size of b.sizes) {
      const themes: Theme[] = ['light', 'dark'];
      if (b.id === 'Sy-07') themes.push('contrast');
      for (const theme of themes) {
        await shoot(browser, b, size, theme, BASE_URL, (p) => DRIVERS[b.id](p, size));
      }
    }
  });
}

// ---- the boards that need a broker of their own ------------------------------------
// A fresh, empty broker (Sy-08, Traf-09), then the same one stopped under open pages
// (Traf-10), in that order: the last test kills it.

const FRESH_PORT = PORT + 20;
const FRESH_CONSOLE = `http://127.0.0.1:${FRESH_PORT + 1}/_console/`;
const FRESH_DATA = join(ROOT, 'e2e', '.tmp', `data evidence ${FRESH_PORT}`);
let fresh: ChildProcess | undefined;

test('fresh broker starts', async () => {
  test.setTimeout(120_000);
  const exe = bin(`doze-kafka-evidence-${FRESH_PORT}`);
  execFileSync('go', ['build', '-o', exe, './cmd/doze-kafka'], { cwd: ROOT, env: { ...process.env, GOWORK: 'off' } });
  rmSync(FRESH_DATA, { recursive: true, force: true });
  mkdirSync(FRESH_DATA, { recursive: true });
  fresh = spawn(exe, ['--listen', `127.0.0.1:${FRESH_PORT}`, '--console-addr', `127.0.0.1:${FRESH_PORT + 1}`,
    '--schema-registry-addr', `127.0.0.1:${FRESH_PORT + 2}`, '--data-dir', FRESH_DATA],
    { cwd: ROOT, env: { ...process.env, DOZE_ZONE: 'off' }, stdio: 'ignore' });
  procs.push(fresh);
  for (let i = 0; i < 150; i++) {
    try { if ((await fetch(`http://127.0.0.1:${FRESH_PORT + 1}/healthz`)).ok) return; } catch { /* not yet */ }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error('the fresh broker did not come up');
});

test('Sy-08: a fresh, empty broker: "Your broker is up"', async ({ browser }) => {
  const b = board('Sy-08');
  for (const size of b.sizes) {
    for (const theme of ['light', 'dark'] as Theme[]) {
      await shoot(browser, b, size, theme, FRESH_CONSOLE, async (p) => {
        await open(p, '');
        await expect(p.locator('h1')).toHaveText('Your broker is up');
      });
    }
  }
});

test('Traf-09: a broker nothing has asked anything of', async ({ browser }) => {
  const b = board('Traf-09');
  for (const size of b.sizes) {
    for (const theme of ['light', 'dark'] as Theme[]) {
      await shoot(browser, b, size, theme, FRESH_CONSOLE, async (p) => {
        await open(p, 'traffic');
        await expect(p.locator('main h1')).toHaveText('Traffic');
      });
    }
  }
});

test('Traf-10: the broker killed under open pages: rows frozen', async ({ browser, request }) => {
  test.setTimeout(150_000);
  const b = board('Traf-10');
  // Something for the page to freeze: a topic, and a consumer that stays in its group.
  const made = await request.post(`${FRESH_CONSOLE}new/topic`, { form: { name: 'ev-gone', partitions: '1' } });
  expect(made.ok()).toBe(true);
  await hold(`127.0.0.1:${FRESH_PORT}`, 'ev-app', 'ev-gone-group', 'ev-gone', {}, holdExe);

  // One page per theme, all opened before the broker goes: a page opened after has no rows.
  const pages: { page: Page; size: Size; theme: Theme; close: () => Promise<void> }[] = [];
  for (const size of b.sizes) {
    for (const theme of ['light', 'dark'] as Theme[]) {
      const [width, height] = SIZES[size];
      const ctx = await browser.newContext({ viewport: { width, height }, colorScheme: theme === 'light' ? 'light' : 'dark', baseURL: FRESH_CONSOLE });
      await ctx.addInitScript((t) => { try { localStorage.setItem('theme', t); } catch { /* none */ } }, theme);
      const page = await ctx.newPage();
      await open(page, 'traffic');
      await expect(page.locator('#traffic-table .tf-rows > .r[data-row]').first()).toBeVisible({ timeout: 20_000 });
      pages.push({ page, size, theme, close: () => ctx.close() });
    }
  }
  await new Promise<void>((done) => { fresh!.once('exit', () => done()); fresh!.kill('SIGKILL'); });
  for (const { page, size, theme, close } of pages) {
    await expect(page.locator('#live-pill')).toHaveClass(/disconnected/, { timeout: 20_000 });
    await expect(page.locator('#lost-callout')).toBeVisible();
    await record(page, b, size, theme);
    await close();
  }
});

// ---- the boards' own renders -------------------------------------------------------

test('reference: the boards rendered as the design draws them', async ({ browser }) => {
  test.skip(!DESIGN_DIR, 'DESIGN_DIR is not set: the reference renders already in plan/evidence are kept');
  test.setTimeout(180_000);
  const css = readFileSync(join(DESIGN_DIR, 'con.css'), 'utf8');
  const fontCss = `
@font-face{font-family:'IBM Plex Sans';font-weight:100 700;font-style:normal;src:url(http://ref.local/f/sans.woff2)}
@font-face{font-family:'IBM Plex Mono';font-weight:400;src:url(http://ref.local/f/mono400.woff2)}
@font-face{font-family:'IBM Plex Mono';font-weight:500;src:url(http://ref.local/f/mono500.woff2)}
@font-face{font-family:'IBM Plex Mono';font-weight:600 700;src:url(http://ref.local/f/mono600.woff2)}`;
  const fontFiles: Record<string, string> = {
    sans: 'ibm-plex-sans-latin-wght-normal.woff2', mono400: 'ibm-plex-mono-latin-400-normal.woff2',
    mono500: 'ibm-plex-mono-latin-500-normal.woff2', mono600: 'ibm-plex-mono-latin-600-normal.woff2',
  };
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 960 } });
  const page = await ctx.newPage();
  await page.route('http://ref.local/**', async (route) => {
    const u = new URL(route.request().url());
    if (u.pathname.startsWith('/f/')) {
      const f = fontFiles[u.pathname.slice(3).replace('.woff2', '')];
      return route.fulfill({ body: readFileSync(join(FONTS, f)), contentType: 'font/woff2', headers: { 'access-control-allow-origin': '*' } });
    }
    return route.fulfill({ body: '<!doctype html><title>x</title>', contentType: 'text/html' });
  });
  await page.route(/^https?:\/\/(?!ref\.local)/, (r) => r.abort());
  for (const b of BOARDS) {
    const src = join(DESIGN_DIR, `${b.file}.dc.html`);
    if (!existsSync(src)) { console.log(`no board file for ${b.id}: ${b.file}`); continue; }
    let html = readFileSync(src, 'utf8');
    html = html.replace(/<script[^>]*src="\.\/support\.js"[^>]*><\/script>/, '')
      .replace(/<link[^>]*href="\.\/con\.css"[^>]*>/, `<style>${css}</style>`)
      .replace(/<script type="text\/x-dc"[\s\S]*?<\/script>/, '')
      .replace('</head>', `<style>${fontCss}</style></head>`);
    await page.goto('http://ref.local/');
    await page.setContent(html, { waitUntil: 'load' });
    await page.evaluate(() => document.fonts.ready);
    await page.waitForTimeout(200);
    const app = page.locator('.app').first();
    const dir = join(OUT, b.stage);
    mkdirSync(dir, { recursive: true });
    await app.screenshot({ path: join(dir, `${b.id}-ref.png`) });
    saveMeas(b.id, { ref: await measure(page, 'ref') });
  }
  await ctx.close();
});

// ---- the index ---------------------------------------------------------------------

const FIELDS = ['w', 'h', 'padding', 'gap', 'font', 'family', 'radius', 'color', 'background'] as const; // family: h1 only, gap: main only
// Widths that follow the window are not facts of the board.
const FLUID_W = new Set(['top bar', 'main', 'h1', 'table header row', 'first table row', 'open row', 'keys strip', 'callout', 'segment', 'dialog', 'tab', 'primary button', 'pill']);

function diffs(id: string, m: MeasFile[string]): string[] {
  const b = board(id);
  const refSize = b.boardSize ?? '1440';
  const liveKey = `${refSize}-${b.boardTheme ?? 'light'}`;
  const live = m.live?.[liveKey];
  if (!m.ref || !live) return [];
  const out: string[] = [];
  for (const [k, v] of Object.entries(m.ref.tokens)) {
    if (live.tokens[k] !== v) out.push(`token ${k}: board ${v}, console ${live.tokens[k]}`);
  }
  for (const p of PROBES) {
    const r = m.ref.probes[p.name];
    const l = live.probes[p.name];
    if (!r || !l) continue;
    for (const f of FIELDS) {
      if (f === 'w' && FLUID_W.has(p.name)) continue;
      if (f === 'h' && ['main', 'inspector', 'list', 'rail', 'callout', 'dialog'].includes(p.name)) continue;
      if (r[f] !== l[f]) out.push(`${p.name} ${f}: board ${r[f]}, console ${l[f]}`);
    }
    const rc = r.columns as number[] | undefined;
    const lc = l.columns as number[] | undefined;
    if (rc && lc && rc.length !== lc.length) out.push(`${p.name} columns: board ${rc.length}, console ${lc.length}`);
  }
  return out;
}

test('index: plan/evidence/README.md', async () => {
  const all = loadMeas();
  const rows: string[] = [];
  const diffSections: string[] = [];
  for (const b of BOARDS) {
    const files: string[] = [];
    const m = all[b.id] ?? {};
    for (const key of Object.keys(m.live ?? {}).sort()) {
      const [size, theme] = key.split('-');
      files.push(`[${size === '1440' ? '' : size + ' '}${theme}](${b.stage}/${b.id}${size === '1440' ? '' : '-' + size}-${theme}.png)`);
    }
    if (existsSync(join(OUT, b.stage, `${b.id}-ref.png`))) files.push(`[board](${b.stage}/${b.id}-ref.png)`);
    const refSize = b.boardSize ?? '1440';
    const live = m.live?.[`${refSize}-${b.boardTheme ?? 'light'}`];
    const key: string[] = [];
    if (live) {
      const g = (n: string, f: string) => (live.probes[n] as Record<string, unknown> | null)?.[f];
      if (g('rail', 'w')) key.push(`rail ${g('rail', 'w')}`);
      if (g('inspector', 'w')) key.push(`inspector ${g('inspector', 'w')}`);
      if (g('top bar', 'h')) key.push(`top ${g('top bar', 'h')}`);
      if (g('h1', 'font')) key.push(`h1 ${g('h1', 'font')}`);
      if (g('first table row', 'h')) key.push(`row ${g('first table row', 'h')}`);
      const cols = (live.probes['first table row'] as { columns?: number[] } | null)?.columns;
      if (cols) key.push(`cols ${cols.join(' ')}`);
      if (g('open row', 'background')) key.push(`open row ${g('open row', 'background')}`);
    }
    rows.push(`| ${b.id} | ${b.state} | ${files.join(', ')} | ${key.join('; ') || 'n/a'} | ${b.verdict} |`);
    const d = diffs(b.id, m);
    if (d.length) diffSections.push(`**${b.id}**\n\n${d.map((x) => `- ${x}`).join('\n')}`);
  }
  const tok = (theme: string) => {
    const any = Object.values(all).map((m) => m.live?.[`1440-${theme}`]).find(Boolean);
    return any ? Object.entries(any.tokens).map(([k, v]) => `${k} ${v}`).join(', ') : 'not measured';
  };
  const text = `# Design evidence

Screenshots of the live console beside the design's boards, for the review that
\`plan/01-protocol.md\` ("Design conformance") asks for. Generated by
\`task evidence\` (\`e2e/evidence/evidence.spec.ts\`); do not edit by hand.

- Light and dark at 1440x900 for every board; 960x700 for Sy-03 and 390x844 for
  Sy-04, Sy-08, Main and Traf-06 (the boards, and the phone states AD7 describes,
  that have a small state). Sy-07 also has a high-contrast file.
- \`<board>-ref.png\` is the design's own render of the board (its .app at its
  own size, from the design artifact's source with the console's self-hosted
  Plex), when the run had DESIGN_DIR; \`measurements.json\` holds the numbers read
  off the board and off the live page by the same function (e2e/evidence/measure.ts).
- The verdict is the one in \`plan/00-foundation.md\`, "Deviations from the boards";
  this run does not change it. For Stage 0 (Sy-01 to Sy-08, the section homes and
  Tp-24) the verdict is the design gate's (\`e2e/design\`, \`go tool task test:design\`):
  it compares every probed element of the board and the console, in light and dark,
  and holds each difference to its reason in an allow-list. The differences listed
  below are this run's coarser measure (a few fixed sizes and colours per page),
  kept as a record beside the pictures; the gate is what holds the console.

To reproduce: \`go tool task evidence\` from the repository root (it boots its own
seeded broker on E2E_PORT and a second one on E2E_PORT+20 for the first-run, empty
and disconnected states, and shrinks the PNGs). To render the boards as well, save
the design artifact's \`project/\` files (the Artifact tool's \`read\` with \`paths\`)
into a folder and set \`DESIGN_DIR\` to it.

Computed tokens (the live page, light): ${tok('light')}.

Dark: ${tok('dark')}.

| Board | State | Files | Key values (live) | Verdict |
|---|---|---|---|---|
${rows.join('\n')}

## Measured differences, board against console

Fixed-size facts only (heights of bars, rows and controls, the widths of the rail,
list and inspector, type and radius, token values). The board's light render is
compared with the console's light page at the board's size, the dark board with
the dark page. A line here is not a verdict: some are in the deviations table
already (Result track, 28 px rows, inspector 400).

${diffSections.join('\n\n') || 'None, or the boards were not rendered in this run.'}
`;
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, 'README.md'), text);
});
