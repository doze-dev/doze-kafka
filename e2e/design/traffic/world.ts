// The Traffic boards' world, made real: a broker of its own (its own ports and data
// directory), and the generator's clients (e2e/design/gen) doing what the boards draw,
// each writing a ledger. The scenario is fixed; what it makes the broker see is
// whatever the clients really did, and the ledger says what that was.
//
//   seed-loader     creates harbour.checkout.orders (3), harbour.depot.dispatch (3) and
//                   harbour.loyalty.points (1), denies User:ANONYMOUS the group
//                   g-loyalty-etl, asks Metadata with auto-create allowed, stays connected
//   checkout-api    a transactional producer (checkout-1), one record a second to
//                   harbour.checkout.orders, and one Produce to harbour.checkout.invoices,
//                   which does not exist (refused)
//   order-validator a classic consumer in g-order-validator that reports its metrics
//                   under the Java consumer's names (KIP-714)
//   depot-router    a classic consumer in g-depot-router; depot-router-2 joins at 8 s and
//                   depot-router-3 at 16 s; depot-router-3 is killed at 22 s (its session
//                   expires 6 s later)
//   depot-feed      a producer to harbour.depot.dispatch every 2 s
//   loyalty-etl     a classic consumer of g-loyalty-etl, which it is denied
//   router-next     a KIP-848 consumer (kgo.ServerSideBalancer) in g-router-next;
//                   router-next-2 joins at 8 s and leaves at 22 s
//   quiet-app       asks Metadata every 2 s and reports no metrics

import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { removeDir } from '../../cleanup';
import { appendFileSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { ROOT } from '../lib/render';
import { Ledger } from '../lib/ledger';

export type World = {
  console: string;
  kafka: string;
  ledger: Ledger;
  /** When the scenario's clients began (unix ms). */
  began: number;
  stopBroker(): Promise<void>;
  startBroker(): Promise<void>;
  stopAll(): Promise<void>;
  /** Kills one of the scenario's clients (SIGKILL: it leaves nothing behind). */
  kill(client: string, signal?: NodeJS.Signals): Promise<void>;
  /**
   * Asks every client still running to write a sync line (SIGUSR1) and waits for all of
   * them: the cut to read the ledger at (ledger.read(cut)), after which every request a
   * client has had answered is in its file and the ones it has not are listed.
   */
  sync(): Promise<number>;
  /** Runs the scenario: the clients start, join, leave and die as the boards' world does. */
  scenario(): Promise<void>;
};

const TMP = join(ROOT, 'e2e', '.tmp');

export function ports(base: number) {
  return { kafka: base, console: base + 1, registry: base + 2 };
}

async function until(f: () => Promise<boolean>, ms: number, what: string) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await f().catch(() => false)) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`${what} did not happen in ${ms} ms`);
}

export async function startWorld(base: number, prefix: string): Promise<World> {
  const p = ports(base);
  const bin = join(TMP, 'bin', `doze-kafka-design-${base}`);
  const gen = join(TMP, 'bin', `gen-design-${base}`);
  const data = join(TMP, `data design ${base}`);
  const ledgerDir = join(TMP, `ledger-design-${base}`);
  mkdirSync(join(TMP, 'bin'), { recursive: true });
  execFileSync('go', ['build', '-o', bin, './cmd/doze-kafka'], { cwd: ROOT, env: { ...process.env, GOWORK: 'off' } });
  execFileSync('go', ['build', '-o', gen, './e2e/design/gen'], { cwd: ROOT, env: { ...process.env, GOWORK: 'off' } });
  removeDir(data);
  removeDir(ledgerDir);
  mkdirSync(data, { recursive: true });
  mkdirSync(ledgerDir, { recursive: true });

  let broker: ChildProcess | undefined;
  const procs = new Map<string, ChildProcess>();
  const consoleURL = `http://127.0.0.1:${p.console}${prefix}/`;
  const kafka = `127.0.0.1:${p.kafka}`;

  const startBroker = async () => {
    broker = spawn(bin, ['--listen', kafka, '--console-addr', `127.0.0.1:${p.console}`, '--schema-registry-addr', `127.0.0.1:${p.registry}`,
      '--data-dir', data, '--consumer-session-timeout', '6s'],
    // DESIGN_CONSOLE_DEV=1 (up.ts, for working on the pages) reads the templates and
    // static files from console/ as they are edited.
    { cwd: ROOT, env: { ...process.env, DOZE_ZONE: 'off', DOZE_KAFKA_CONSOLE_PREFIX: prefix, ...(process.env.DESIGN_CONSOLE_DEV ? { DOZE_KAFKA_CONSOLE_DEV: join(ROOT, 'console') } : {}) }, stdio: 'ignore' });
    await until(async () => (await fetch(`http://127.0.0.1:${p.console}/healthz`)).ok, 15_000, 'the broker starting');
  };
  const stopBroker = async () => {
    const b = broker;
    if (!b) return;
    await new Promise<void>((done) => { b.once('exit', () => done()); b.kill('SIGKILL'); });
    broker = undefined;
    appendFileSync(join(ledgerDir, 'broker.jsonl'), JSON.stringify({ ev: 'note', what: 'broker-stopped', t: Date.now() }) + '\n');
  };
  const run = (cmd: string, id: string, args: string[] = [], env: Record<string, string> = {}): Promise<void> =>
    new Promise((ready, fail) => {
      const c = spawn(gen, [cmd, kafka, id, ...args], { cwd: ROOT, env: { ...process.env, LEDGER: ledgerDir, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
      procs.set(id, c);
      let said = false;
      c.stdout!.on('data', (b: Buffer) => { if (!said && b.toString().includes('ready')) { said = true; ready(); } });
      let err = '';
      c.stderr!.on('data', (b: Buffer) => { err += b.toString(); });
      c.once('exit', (code) => { if (!said) fail(new Error(`gen ${cmd} ${id} exited ${code} before it was ready: ${err}`)); });
      setTimeout(() => { if (!said) fail(new Error(`gen ${cmd} ${id} was not ready in 20 s: ${err}`)); }, 20_000);
    });
  const kill = async (id: string, signal: NodeJS.Signals = 'SIGKILL') => {
    const c = procs.get(id);
    if (!c || c.exitCode !== null) return;
    await new Promise<void>((done) => { c.once('exit', () => done()); c.kill(signal); });
    procs.delete(id);
    // A killed client writes nothing more: its ledger says when it died, which is when
    // the system closed its connections.
    if (signal === 'SIGKILL') appendFileSync(join(ledgerDir, `${id}.jsonl`), JSON.stringify({ ev: 'killed', t: Date.now() }) + '\n');
  };
  const stopAll = async () => {
    await Promise.all([...procs.keys()].map((id) => kill(id, 'SIGKILL')));
    await stopBroker();
  };

  const sync = async () => {
    const cut = Date.now();
    const live = [...procs.entries()].filter(([, c]) => c.exitCode === null && c.signalCode === null);
    for (const [, c] of live) c.kill('SIGUSR1');
    await until(async () => {
      world.ledger.read();
      return live.every(([id]) => (world.ledger.syncs.get(id) ?? 0) >= cut);
    }, 30_000, 'every client writing its sync line');
    return cut;
  };

  const scenario = async () => {
    try {
      world.began = Date.now();
      await run('seed', 'seed-loader', [], { NOMETRICS: '1' });
      await Promise.all([
        run('producer', 'checkout-api', ['harbour.checkout.orders', '1000', 'checkout-1'], { REFUSE: 'harbour.checkout.invoices' }),
        run('reporter', 'order-validator', ['g-order-validator', 'harbour.checkout.orders']),
        run('member', 'depot-router', ['g-depot-router', 'harbour.depot.dispatch', 'classic']),
        run('producer', 'depot-feed', ['harbour.depot.dispatch', '2000']),
        run('member', 'loyalty-etl', ['g-loyalty-etl', 'harbour.loyalty.points', 'classic']),
        run('member', 'router-next', ['g-router-next', 'harbour.depot.dispatch', 'next']),
        run('quiet', 'quiet-app'),
      ]);
      const at = (ms: number) => new Promise((r) => setTimeout(r, Math.max(0, world.began + ms - Date.now())));
      await at(8_000);
      await Promise.all([
        run('member', 'depot-router-2', ['g-depot-router', 'harbour.depot.dispatch', 'classic']),
        run('member', 'router-next-2', ['g-router-next', 'harbour.depot.dispatch', 'next']),
      ]);
      await at(16_000);
      await run('member', 'depot-router-3', ['g-depot-router', 'harbour.depot.dispatch', 'classic']);
      await at(22_000);
      await kill('depot-router-3', 'SIGKILL');
      await kill('router-next-2', 'SIGTERM');
      await at(32_000);
    } catch (e) {
      await stopAll();
      throw e;
    }
    world.ledger.read();
  };

  await startBroker();
  appendFileSync(join(ledgerDir, 'broker.jsonl'), JSON.stringify({ ev: 'note', what: 'world', kafka, t: Date.now() }) + '\n');
  const world: World = { console: consoleURL, kafka, ledger: new Ledger(ledgerDir), began: 0, stopBroker, startBroker, stopAll, kill, sync, scenario };
  return world;
}
