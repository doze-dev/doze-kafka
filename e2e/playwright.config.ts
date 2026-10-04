import { defineConfig, devices } from '@playwright/test';

// The suite boots the real binary on its own ports, over a fresh data directory,
// with the .doze zone off: no DNS, no name claimed, nothing shared with a broker
// already running on the machine. E2E_PORT moves it, so two runs can coexist.
const PORT = Number(process.env.E2E_PORT ?? 14092);
export const KAFKA_ADDR = `127.0.0.1:${PORT}`;
export const CONSOLE_PORT = PORT + 1;
export const BASE_URL = `http://127.0.0.1:${CONSOLE_PORT}/_console/`;

export default defineConfig({
  testDir: './tests',
  globalSetup: './global-setup.ts',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 4 : undefined,
  reporter: process.env.CI ? [['github'], ['html', { open: 'never' }]] : 'list',

  use: {
    baseURL: BASE_URL,
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },

  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],

  webServer: {
    // A fresh data directory every run: state left over from a previous run is
    // how a suite's result comes to depend on how often it has been run.
    command:
      `sh -c "cd .. && GOWORK=off go build -o e2e/.tmp/bin/doze-kafka-${PORT} ./cmd/doze-kafka && ` +
      `cd e2e/.tmp && rm -rf data-${PORT} && mkdir -p data-${PORT} && ` +
      `./bin/doze-kafka-${PORT} --listen ${KAFKA_ADDR} --console-addr 127.0.0.1:${CONSOLE_PORT} --zone=false --data-dir data-${PORT}"`,
    url: `http://127.0.0.1:${CONSOLE_PORT}/healthz`,
    reuseExistingServer: false, // a reused server keeps its state, and the wipe above never runs.
    timeout: 120_000,
    stdout: 'pipe',
    stderr: 'pipe',
  },
});
