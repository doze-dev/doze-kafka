import { resolve } from 'node:path';
import { defineConfig } from '@playwright/test';
import base from '../playwright.config';

// The evidence run (plan/01-protocol.md, "Design conformance"): not part of the
// default suite (the default config's testDir is ./tests; this one runs only
// evidence.spec.ts). Same broker, same ports, same E2E_PORT; its own global setup,
// which seeds and nothing else, and no route gate (this run does not test coverage).
export default defineConfig({
  ...base,
  testDir: '.',
  testMatch: 'evidence.spec.ts',
  globalSetup: './global-setup.ts',
  globalTeardown: undefined,
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: 'list',
  outputDir: '../.tmp/evidence-results',
  // The base command starts with `cd ..` from e2e/ (its config's directory); this
  // config is one directory down, where that would not find the repository.
  webServer: base.webServer && !Array.isArray(base.webServer) ? { ...base.webServer, cwd: resolve(import.meta.dirname, '..') } : base.webServer,
});
