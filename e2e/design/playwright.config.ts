import { resolve } from 'node:path';
import { defineConfig } from '@playwright/test';
import base from '../playwright.config';

// The design gate (plan/01-protocol.md, "Design conformance"): each board of the
// design compared with the live console, element by element, in light and dark at
// 1440 and at the narrow widths the boards draw. Its own broker, seeded the same way
// as the browser suite's (E2E_PORT moves it; `task test:design` gives it its own
// ports so it can run beside test:e2e). See README.md.
process.env.DESIGN_TMP = resolve(import.meta.dirname, '..', '.tmp', `design-${process.env.E2E_PORT ?? 14092}`);

export default defineConfig({
  ...base,
  testDir: '.',
  testMatch: '**/*.spec.ts',
  globalSetup: './global-setup.ts',
  globalTeardown: undefined,
  retries: 0,
  reporter: 'list',
  outputDir: '../.tmp/design-results',
  // The base command starts with `cd ..` from e2e/; this config is one directory down.
  webServer: base.webServer && !Array.isArray(base.webServer) ? { ...base.webServer, cwd: resolve(import.meta.dirname, '..') } : base.webServer,
});
