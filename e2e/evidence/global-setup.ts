import { execFileSync } from 'node:child_process';
import { KAFKA_ADDR, REGISTRY_URL } from '../playwright.config';

// Puts the seed's data in the broker (e2e/seed). The evidence run needs the default
// identity (a fixed listener), so E2E_IDENTITY is not supported here.
export default async function globalSetup() {
  execFileSync('go', ['run', './e2e/seed', KAFKA_ADDR, REGISTRY_URL], {
    cwd: '..',
    env: { ...process.env, GOWORK: 'off' },
    stdio: 'inherit',
  });
}
