import { execFileSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import { join, resolve } from 'node:path';

// Runs one of the repository's Go programs and returns what it printed. It is built
// first and the binary run: `go run` may print more than the program does on its
// stdout (a toolchain or shell wrapper's notice), and what is read here is parsed.

const ROOT = resolve(import.meta.dirname, '..', '..', '..');

export function goOutput(pkg: string, args: string[] = []): string {
  const env = { ...process.env, GOWORK: 'off' };
  const dir = join(ROOT, 'e2e', '.tmp', 'bin');
  mkdirSync(dir, { recursive: true });
  const bin = join(dir, `${pkg.replace(/^\.\//, '').replace(/\//g, '-')}-${process.pid}`); // one per process: workers build at once
  execFileSync('go', ['build', '-o', bin, pkg], { cwd: ROOT, env, stdio: ['ignore', 'ignore', 'inherit'] });
  return execFileSync(bin, args, { cwd: ROOT, env }).toString();
}
