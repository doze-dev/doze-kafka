import { defineConfig, devices } from '@playwright/test';

// The suite boots the real binary on its own ports, over a fresh data directory,
// on an address (--listen): no DNS, no name claimed, nothing shared with a broker
// already running on the machine. E2E_PORT moves it, so two runs can coexist.
const PORT = Number(process.env.E2E_PORT ?? 14092);
export const CONSOLE_PORT = PORT + 1;
export const REGISTRY_PORT = PORT + 2;

// E2E_IDENTITY=1 runs the suite with nothing tidy about the broker: its listener
// picks its own port (127.0.0.1:0), the console is mounted under a prefix that is
// not the default, the data directory's path has a space in it and the name zone
// is off (plan A8). The address the broker bound is then read from its console
// (global-setup.ts, addr.ts).
//
// The second identity (foundation B11, `task test:e2e:identity`) starts the broker
// itself, with SASL and TLS as well, and says where it is in the environment.
const EXTERNAL = process.env.E2E_EXTERNAL === '1';
export const IDENTITY = !!process.env.E2E_IDENTITY;
export const PREFIX = IDENTITY ? '/ops/kafka-ui' : '/_console';
export const KAFKA_ADDR = EXTERNAL ? process.env.E2E_KAFKA_ADDR! : `127.0.0.1:${PORT}`;
export const REGISTRY_URL = EXTERNAL ? process.env.E2E_REGISTRY_URL! : `http://127.0.0.1:${REGISTRY_PORT}`;
export const BASE_URL = EXTERNAL ? process.env.E2E_CONSOLE_URL! : `http://127.0.0.1:${CONSOLE_PORT}${PREFIX}/`;
const LISTEN = IDENTITY ? '127.0.0.1:0' : KAFKA_ADDR;
const DATA_DIR = IDENTITY ? `data dir ${PORT}` : `data-${PORT}`;

export default defineConfig({
  testDir: './tests',
  globalSetup: './global-setup.ts',
  globalTeardown: './global-teardown.ts',
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

  webServer: EXTERNAL ? undefined : {
    // A fresh data directory every run: state left over from a previous run is
    // how a suite's result comes to depend on how often it has been run.
    command:
      `sh -c "cd .. && GOWORK=off go build -o e2e/.tmp/bin/doze-kafka-${PORT} ./cmd/doze-kafka && ` +
      `cd e2e/.tmp && rm -rf \\\"${DATA_DIR}\\\" && mkdir -p \\\"${DATA_DIR}\\\" && ` +
      `./bin/doze-kafka-${PORT} --listen ${LISTEN} --console-addr 127.0.0.1:${CONSOLE_PORT} --schema-registry-addr 127.0.0.1:${REGISTRY_PORT} --data-dir \\\"${DATA_DIR}\\\""`,
    env: IDENTITY ? { DOZE_ZONE: 'off', DOZE_KAFKA_CONSOLE_PREFIX: PREFIX } : {},
    url: `http://127.0.0.1:${CONSOLE_PORT}/healthz`,
    reuseExistingServer: false, // a reused server keeps its state, and the wipe above never runs.
    timeout: 120_000,
    stdout: 'pipe',
    stderr: 'pipe',
  },
});
