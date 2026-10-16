import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { test, expect } from '../fixtures/console';
import type { Page } from '@playwright/test';
import { kafkaAddr } from '../addr';

// Names are a client's to choose, and it may choose long ones. The boards draw a name
// in a track of a fixed width and cut it with an ellipsis (con.css: `.r > *` and
// `.el`), and the whole of it is a hover away or in the inspector. So nothing a client
// calls itself may widen a table past its card, push a control under the inspector or
// make the page scroll sideways.
//
// The names here are the longest each can be where Traffic shows it: a client id, a
// software name and version, a group and a transactional id are kept to 128 bytes
// (the recorder's MaxName, and protocol.MaxClientSoftware), a topic's name may be 249
// characters. Each is sent longer than that where a client can, so what is drawn is
// the clipped maximum.

const KEPT = 128;
const CLIENT = 'long-client-' + 'c'.repeat(188); // 200 sent.
const SOFTWARE = 'long-software-' + 's'.repeat(150); // with its version, 170 sent.
const VERSION = '9.9.9';
const GROUP = 'long-group-' + 'g'.repeat(189); // 200 sent.
const TXN = 'long-txn-' + 'x'.repeat(191); // 200 sent.
const TOPIC = 'long-topic-' + 't'.repeat(238); // 249: the longest a topic may be named.

/** What Traffic keeps of a name: all of it, or its first 125 bytes and an ellipsis (128). */
const kept = (s: string) => (s.length <= KEPT ? s : s.slice(0, KEPT - 3) + '…');

let hold: ChildProcess;

test.describe.configure({ mode: 'default' });

test.beforeAll(async () => {
  const bin = `e2e/.tmp/bin/hold-long-${process.pid}`;
  execFileSync('go', ['build', '-o', bin, './e2e/hold'], { cwd: '..', env: { ...process.env, GOWORK: 'off' } });
  hold = spawn(bin, [kafkaAddr(), CLIENT, GROUP, TOPIC], {
    cwd: '..', stdio: ['ignore', 'pipe', 'inherit'],
    env: { ...process.env, HOLD_SOFTWARE: `${SOFTWARE} ${VERSION}`, HOLD_TXN: TXN, HOLD_TELEMETRY: '1' },
  });
  await new Promise<void>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('the long-named client never joined its group')), 30_000);
    hold.stdout!.on('data', (b) => { if (String(b).includes('ready')) { clearTimeout(t); resolve(); } });
    hold.on('exit', () => reject(new Error('the long-named client exited')));
  });
});

test.afterAll(() => { hold?.kill('SIGTERM'); });

/**
 * Everything on the page that sticks out of what holds it. Three things are looked for:
 * the page scrolling sideways; a box that scrolls sideways (a table wider than its
 * card); and an element whose right or left edge is past the box that clips it, unless
 * that box is one that cuts its text with an ellipsis, which is the cut the boards draw.
 */
async function stickingOut(page: Page): Promise<string[]> {
  return page.evaluate(() => {
    const out: string[] = [];
    const name = (el: Element) => {
      const id = el.id ? `#${el.id}` : '';
      const cls = typeof el.className === 'string' && el.className ? '.' + el.className.trim().split(/\s+/).slice(0, 3).join('.') : '';
      return `${el.tagName.toLowerCase()}${id}${cls}`;
    };
    const root = document.documentElement;
    if (root.scrollWidth > root.clientWidth + 1) out.push(`the page scrolls sideways: ${root.scrollWidth} px of content in ${root.clientWidth}`);
    for (const el of Array.from(document.querySelectorAll('body *'))) {
      const box = el.getBoundingClientRect();
      if (box.width === 0 || box.height === 0) continue;
      const style = getComputedStyle(el);
      if (style.visibility === 'hidden' || style.display === 'none') continue;
      // A box that scrolls sideways. (A code block may: it is text kept on its lines.)
      if ((style.overflowX === 'auto' || style.overflowX === 'scroll') && el.scrollWidth > el.clientWidth + 1 && !el.matches('pre, pre *, .code, .cm-scroller')) {
        // Named with what is wider than it, the deepest such element, so a failure says what to fix.
        const eb = el.getBoundingClientRect();
        const wide = Array.from(el.querySelectorAll('*')).filter((k) => {
          const kb = k.getBoundingClientRect();
          return kb.width > 0 && kb.right > eb.right + 1 && !Array.from(k.children).some((c) => c.getBoundingClientRect().right > eb.right + 1);
        }).slice(0, 2).map(name);
        out.push(`${name(el)} scrolls sideways: ${el.scrollWidth} px of content in ${el.clientWidth} (${wide.join(', ')})`);
      }
      // The nearest box that clips this element.
      let clip: Element | null = el.parentElement;
      while (clip && getComputedStyle(clip).overflowX === 'visible') clip = clip.parentElement;
      if (!clip || clip === document.body || clip === root) continue;
      const cs = getComputedStyle(clip);
      if (cs.textOverflow === 'ellipsis') continue; // cut with an ellipsis, as drawn.
      if (clip.matches('pre, .code, .cm-scroller') || el.closest('pre, .code, .cm-editor')) continue;
      const cb = clip.getBoundingClientRect();
      // Something inside a box that scrolls sideways was reported with the box.
      if ((cs.overflowX === 'auto' || cs.overflowX === 'scroll') && clip.scrollWidth > clip.clientWidth + 1) continue;
      if (box.right > cb.right + 1 || box.left < cb.left - 1) {
        out.push(`${name(el)} sticks out of ${name(clip)}: ${Math.round(box.left)} to ${Math.round(box.right)} in ${Math.round(cb.left)} to ${Math.round(cb.right)}`);
      }
    }
    return out.slice(0, 12);
  });
}

async function settle(page: Page) {
  await page.addInitScript(() => { try { localStorage.setItem('theme', 'light'); } catch { /* none */ } });
}

for (const width of [1440, 960, 390]) {
  test.describe(`at ${width} px`, () => {
    test.use({ viewport: { width, height: 900 } });

    test('the clients view holds a client with the longest names in its tracks', async ({ page }) => {
      await settle(page);
      await page.goto('traffic?view=clients');
      const row = page.locator('#clients-table tbody tr', { hasText: 'long-client-' });
      await expect(row).toHaveCount(1);
      expect(await stickingOut(page)).toEqual([]);
      // The whole of what is kept is a hover away.
      await expect(row.locator('.cell-client a')).toHaveAttribute('title', kept(CLIENT));
      if (width >= 960) {
        await expect(row.locator('.c-sw')).toHaveAttribute('title', kept(`${SOFTWARE} ${VERSION}`));
        await expect(row.locator('.c-gr')).toHaveAttribute('title', new RegExp('^' + kept(GROUP).slice(0, 60)));
      }
      // And the table is as wide as its card, not as wide as its names.
      const table = await page.locator('#clients-table').boundingBox();
      const wrap = await page.locator('.tbl-wrap').first().boundingBox();
      expect(table!.width).toBeLessThanOrEqual(wrap!.width + 1);
    });

    test('the client inspector holds them, and shows the whole client id', async ({ page }) => {
      await settle(page);
      await page.goto('traffic?view=clients');
      await page.locator('#clients-table tbody tr', { hasText: 'long-client-' }).click();
      const insp = page.locator('#client-inspector');
      await expect(insp).toBeVisible();
      expect(await stickingOut(page)).toEqual([]);
      // The inspector is where the whole value is: the client id wraps, it is not cut.
      const id = insp.locator('.kv [data-full="client-id"]');
      await expect(id).toHaveText(kept(CLIENT));
      const cut = await id.evaluate((el) => el.scrollWidth > el.clientWidth + 1);
      expect(cut).toBe(false);
      await expect(insp.locator('.kv [data-full="txn-id"]')).toHaveText(kept(TXN));
    });

    test('the requests view holds its rows, and a topic of 249 characters', async ({ page }) => {
      await settle(page);
      await page.goto('traffic');
      await expect(page.locator('#traffic-table tbody tr', { hasText: 'long-client-' }).first()).toBeVisible();
      expect(await stickingOut(page)).toEqual([]);
      await page.goto('traffic?topic=' + TOPIC);
      await expect(page.locator('#traffic-table tbody tr').first()).toBeVisible();
      expect(await stickingOut(page)).toEqual([]);
      // A row's inspector, with the long topic among its partitions.
      await page.locator('#traffic-table tbody tr', { hasText: 'Produce' }).first().click();
      await expect(page.locator('#inspector')).toContainText('Result per partition');
      expect(await stickingOut(page)).toEqual([]);
    });

    test("a group's story holds its pill and its controls on the page", async ({ page }) => {
      await settle(page);
      await page.goto('traffic?group=' + GROUP);
      await expect(page.locator('#traffic-story')).toBeVisible();
      expect(await stickingOut(page)).toEqual([]);
      const pill = page.locator('.pill', { hasText: 'long-group-' }).first();
      // The pill is the filter as asked: the whole name, cut on screen and whole in its title.
      await expect(pill.locator('.m')).toHaveAttribute('title', GROUP);
      // The pill's own remove control and "clear" are inside the page's column.
      const main = (await page.locator('main').first().boundingBox())!;
      for (const control of [pill.locator('.pill-x'), page.locator('#detail a', { hasText: /^clear$/ }).first()]) {
        const box = (await control.boundingBox())!;
        expect(box.x + box.width).toBeLessThanOrEqual(main.x + main.width + 1);
      }
    });
  });
}

