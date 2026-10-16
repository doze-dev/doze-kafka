import { execFileSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { KAFKA_ADDR, REGISTRY_URL } from '../playwright.config';

// Seeds the broker (e2e/seed), then reads back what it holds (e2e/design/truth): the
// numbers the specs hold the console's to.
export default async function globalSetup() {
  const env = { ...process.env, GOWORK: 'off' };
  execFileSync('go', ['run', './e2e/seed', KAFKA_ADDR, REGISTRY_URL], { cwd: '..', env, stdio: 'inherit' });
  const tmp = process.env.DESIGN_TMP!;
  mkdirSync(tmp, { recursive: true });
  const world = execFileSync('go', ['run', './e2e/design/truth', KAFKA_ADDR, REGISTRY_URL], { cwd: '..', env });
  writeFileSync(join(tmp, 'truth.json'), world);
}
