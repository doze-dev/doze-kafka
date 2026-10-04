import { execFileSync } from 'node:child_process';
import { KAFKA_ADDR } from './playwright.config';

// Runs once the broker is up: puts the data the read-only specs look at in it
// (see seed/main.go). Specs that change things make their own topics.
export default function globalSetup() {
  execFileSync('go', ['run', './e2e/seed', KAFKA_ADDR], {
    cwd: '..',
    env: { ...process.env, GOWORK: 'off' },
    stdio: 'inherit',
  });
}
