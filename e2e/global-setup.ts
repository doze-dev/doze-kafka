import { execFileSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { BASE_URL, IDENTITY, KAFKA_ADDR, REGISTRY_URL } from './playwright.config';
import { ADDR_FILE } from './addr';

// The broker's address when its listener picked its own port: the console says it
// on the Cluster page ("node 1 · 127.0.0.1:41873").
async function boundAddress(): Promise<string> {
  const page = await (await fetch(`${BASE_URL}cluster`)).text();
  const m = page.match(/node \d+ · ([\w.\-]+):(\d+)/);
  if (!m) throw new Error('the Cluster page does not say where the broker listens');
  return `${m[1]}:${m[2]}`;
}

// Runs once the broker is up: puts the data the read-only specs look at in it
// (see seed/main.go). Specs that change things make their own topics.
export default async function globalSetup() {
  let addr = KAFKA_ADDR;
  if (IDENTITY) {
    addr = await boundAddress();
    mkdirSync(dirname(ADDR_FILE), { recursive: true });
    writeFileSync(ADDR_FILE, addr);
  }
  execFileSync('go', ['run', './e2e/seed', addr, REGISTRY_URL], {
    cwd: '..',
    env: { ...process.env, GOWORK: 'off' },
    stdio: 'inherit',
  });
}
