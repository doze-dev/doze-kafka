import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import { removeDir } from './cleanup';
import { join, resolve } from 'node:path';
import { PREFIX } from './playwright.config';

// A broker of a spec's own, for a spec that changes what every other spec reads (ACLs,
// which the seeded world counts) or that stops the broker. It runs on E2E_PORT plus an
// offset of the spec's choosing, over a fresh data directory whose path has a space in it.

export class OwnBroker {
  readonly port: number;
  readonly console: string;
  readonly kafka: string;
  private readonly root = resolve(process.cwd(), '..');
  private readonly bin: string;
  private readonly data: string;
  /** The instance name: the console labels the broker with it, unless it is the default, so it is set, not taken from the directory the repository is checked out in. */
  private readonly instance: string;
  private proc?: ChildProcess;

  constructor(name: string, offset: number) {
    this.port = Number(process.env.E2E_PORT ?? 14092) + offset;
    this.console = `http://127.0.0.1:${this.port + 1}${PREFIX}/`;
    this.kafka = `127.0.0.1:${this.port}`;
    this.instance = `e2e-${name}`;
    this.bin = join(this.root, 'e2e', '.tmp', 'bin', `doze-kafka-${name}-${this.port}`);
    this.data = join(this.root, 'e2e', '.tmp', `data ${name} ${this.port}`);
  }

  /** Builds the binary and starts it over an empty data directory. */
  async start(): Promise<void> {
    mkdirSync(join(this.root, 'e2e', '.tmp', 'bin'), { recursive: true });
    execFileSync('go', ['build', '-o', this.bin, './cmd/doze-kafka'], { cwd: this.root, env: { ...process.env, GOWORK: 'off' } });
    removeDir(this.data);
    mkdirSync(this.data, { recursive: true });
    await this.up();
  }

  /** Starts it over the data directory it has. */
  async up(): Promise<void> {
    const p = this.port;
    this.proc = spawn(this.bin, ['--listen', `127.0.0.1:${p}`, '--console-addr', `127.0.0.1:${p + 1}`, '--schema-registry-addr', `127.0.0.1:${p + 2}`, '--data-dir', this.data, '--name', this.instance],
      { cwd: this.root, env: { ...process.env, DOZE_ZONE: 'off', DOZE_KAFKA_CONSOLE_PREFIX: PREFIX }, stdio: 'ignore' });
    for (let i = 0; i < 150; i++) {
      try {
        if ((await fetch(`http://127.0.0.1:${p + 1}/healthz`)).ok) return;
      } catch { /* not yet */ }
      await new Promise((r) => setTimeout(r, 100));
    }
    throw new Error('the broker did not come up');
  }

  /** Kills it (SIGKILL), as a crash would. */
  async kill(): Promise<void> {
    const p = this.proc;
    if (!p) return;
    await new Promise<void>((done) => {
      p.once('exit', () => done());
      p.kill('SIGKILL');
    });
    this.proc = undefined;
  }
}
