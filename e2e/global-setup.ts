import { execFileSync } from 'node:child_process';
import { KAFKA_ADDR, REGISTRY_URL } from './playwright.config';
import { resetSeen } from './route-gate';

// Runs once the broker is up: puts the data the read-only specs look at in it
// (see seed/main.go). Specs that change things make their own topics.
export default function globalSetup() {
  resetSeen(); // the route gate counts this run's requests, not the last one's.
  execFileSync('go', ['run', './e2e/seed', KAFKA_ADDR, REGISTRY_URL], {
    cwd: '..',
    env: { ...process.env, GOWORK: 'off' },
    stdio: 'inherit',
  });
}
