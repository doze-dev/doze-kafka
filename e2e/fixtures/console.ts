import { test as base, expect, type Page, type Locator } from '@playwright/test';
import { randomBytes } from 'node:crypto';

type ConfirmAction = 'accept' | 'cancel';

type Fixtures = {
  /** `${prefix}-${hex}`: use for every resource a spec creates, so parallel tests never collide. */
  uniqueName: (prefix: string) => string;
  /** Opens the command palette from its button and waits for its first items. */
  openPalette: () => Promise<void>;
  /** Waits for a toast and returns its text. They remove themselves after 3-6s, so assert on the return value. */
  waitForToast: (opts?: { kind?: 'ok' | 'err' }) => Promise<string>;
  /** Answers the console's styled confirm dialog (it replaces every native confirm). */
  confirmDialog: (action: ConfirmAction) => Promise<void>;
  /** Writes into the composer's CodeMirror editor through the app's own API, keeping the textarea in sync. */
  setEditor: (value: string) => Promise<void>;
  /** Waits for the live tape to say something that satisfies `predicate`, however many polls that takes. */
  waitForLive: (predicate: (text: string) => boolean, opts?: { timeout?: number }) => Promise<void>;
  /** Creates a topic through the form and lands on its page. */
  createTopic: (name: string, partitions?: number) => Promise<void>;
  /** Sends a record through the open composer. */
  send: (value: string, opts?: { key?: string }) => Promise<void>;
};

export const test = base.extend<Fixtures>({
  uniqueName: async ({}, use) => {
    await use((prefix) => `${prefix}-${randomBytes(4).toString('hex')}`);
  },

  openPalette: async ({ page }, use) => {
    await use(async () => {
      const catalogue = page.waitForResponse((r) => r.url().includes('/api/palette'));
      await page.locator('#palette-open').click();
      await expect(page.locator('#palette')).toBeVisible();
      await expect(page.locator('#pal-q')).toBeFocused();
      await catalogue; // it re-renders the list when it lands; wait so callers see the settled one.
      await expect(page.locator('.pal-item').first()).toBeVisible();
    });
  },

  waitForToast: async ({ page }, use) => {
    await use(async (opts = {}) => {
      const toast = page.locator(opts.kind === 'err' ? '.toast.err' : '.toast:not(.err)').last();
      await expect(toast).toBeVisible({ timeout: 8000 });
      return (await toast.innerText()).replace(/\s+/g, ' ').trim();
    });
  },

  confirmDialog: async ({ page }, use) => {
    await use(async (action) => {
      await expect(page.locator('#confirm')).toBeVisible();
      await page.locator(action === 'accept' ? '#confirm-yes' : '#confirm-no').click();
      await expect(page.locator('#confirm')).toBeHidden();
    });
  },

  setEditor: async ({ page }, use) => {
    await use(async (value) => {
      await page.evaluate((v) => {
        const ta = document.querySelector('#f-value') as HTMLTextAreaElement;
        (window as any).dozeEditor.set(ta, v);
      }, value);
    });
  },

  waitForLive: async ({ page }, use) => {
    await use(async (predicate, opts = {}) => {
      await expect
        .poll(async () => predicate(await page.locator('#tape-and-table').innerText()), { timeout: opts.timeout ?? 10_000 })
        .toBe(true);
    });
  },

  createTopic: async ({ page }, use) => {
    await use(async (name, partitions = 1) => {
      await page.goto('new/topic');
      await page.locator('#f-name').fill(name);
      await page.locator('#f-partitions').fill(String(partitions));
      await page.getByRole('button', { name: 'Create topic' }).click();
      await expect(page.locator('.idband h1')).toHaveText(name);
    });
  },

  send: async ({ page, setEditor }, use) => {
    await use(async (value, opts = {}) => {
      if ((await page.locator('#compose-head').getAttribute('aria-expanded')) !== 'true') {
        await page.locator('#compose-head').click();
      }
      if (opts.key !== undefined) await page.locator('#f-key').fill(opts.key);
      await setEditor(value);
      await page.getByRole('button', { name: 'Send message' }).click();
    });
  },
});

export { expect };
export type { Page, Locator };
