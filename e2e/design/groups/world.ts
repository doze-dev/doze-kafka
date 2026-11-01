// The Groups boards' world, made real: a broker of its own and the generator's clients
// (e2e/design/gen) in the state Gr-01, Gr-02 and Gr-03 draw. The broker starts empty
// (Gr-01); then the world is filled:
//
//   seed-loader   creates harbour.depot.dispatch (3 partitions) and the other two topics
//   depot-feed    writes FIRST records, round-robin
//   depot-router  three classic consumers of g-depot-router (client id depot-router),
//                 each HELD: it joins, takes what is on the topic, commits it, and
//                 stops polling, heartbeats still running
//   depot-feed-2  writes MORE records: the group's lag, which the ledger knows
//
// The ledger (what the clients wrote) says which records went to which partition.

import { spawn, type ChildProcess } from 'node:child_process';
import { join } from 'node:path';
import { readFileSync, readdirSync } from 'node:fs';
import { ROOT } from '../lib/render';
import { startWorld, type World } from '../traffic/world';

export const TOPIC = 'harbour.depot.dispatch';
export const GROUP = 'g-depot-router';
export const FIRST = 12;
export const MORE = 14;

export type GroupsWorld = {
  world: World;
  /** Fills the broker: topics, the held group, the lag. */
  fill(): Promise<void>;
  /** Records the ledger says went to each partition of TOPIC. */
  produced(): number[];
  stop(): Promise<void>;
};

export async function startGroupsWorld(base: number, prefix: string): Promise<GroupsWorld> {
  const world = await startWorld(base, prefix);
  const tmp = join(ROOT, 'e2e', '.tmp');
  const gen = join(tmp, 'bin', `gen-design-${base}`);
  const ledger = join(tmp, `ledger-design-${base}`);
  const procs: ChildProcess[] = [];
  const run = (cmd: string, id: string, args: string[], env: Record<string, string> = {}) =>
    new Promise<void>((ready, fail) => {
      const c = spawn(gen, [cmd, world.kafka, id, ...args], { cwd: ROOT, env: { ...process.env, LEDGER: ledger, NOMETRICS: '1', ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
      procs.push(c);
      let said = false, err = '';
      c.stdout!.on('data', (b: Buffer) => { if (b.toString().includes('ready')) { said = true; ready(); } });
      c.stderr!.on('data', (b: Buffer) => { err += b.toString(); });
      c.once('exit', (code) => { if (!said) fail(new Error(`gen ${cmd} ${id} exited ${code}: ${err}`)); });
      setTimeout(() => { if (!said) fail(new Error(`gen ${cmd} ${id} not ready in 40 s: ${err}`)); }, 40_000);
    });
  return {
    world,
    async fill() {
      await run('seed', 'seed-loader', []);
      await run('produce', 'depot-feed', [TOPIC, String(FIRST)]);
      await Promise.all([1, 2, 3].map(() => run('member', 'depot-router', [GROUP, TOPIC, 'classic'], { HOLD: '1'})));
      await run('produce', 'depot-feed-2', [TOPIC, String(MORE)]);
    },
    produced() {
      const per = [0, 0, 0];
      for (const f of readdirSync(ledger).filter((f) => f.startsWith('depot-feed'))) {
        for (const line of readFileSync(join(ledger, f), 'utf8').split('\n')) {
          if (!line.includes('"produced"')) continue;
          per[JSON.parse(line).p]++;
        }
      }
      return per;
    },
    async stop() {
      for (const c of procs) c.kill('SIGKILL');
      await world.stopAll();
    },
  };
}
