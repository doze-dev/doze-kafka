import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
import { checkRoutes } from './route-gate';

// The route gate (foundation B8): the console's route table against what the suite
// requested. It fails the run for a GET never visited or a POST never driven,
// unless e2e/route-gaps.txt says why. A partial run (a file, a title, --grep,
// --last-failed, a shard) cannot have covered everything, so it is not gated.
//
// E2E_ROUTE_GATE=0 turns it off for a local run. In CI that is refused: a CI run
// with the gate off would pass having proved nothing about the routes. The
// workflow also looks for the gate's "every console route" line in the output,
// so a run that skipped the gate any other way fails too.
export default function globalTeardown() {
  if (process.env.E2E_ROUTE_GATE === '0') {
    if (process.env.CI) {
      process.exitCode = 1;
      throw new Error('route gate: E2E_ROUTE_GATE=0 is set in CI, where the gate must run');
    }
    console.log('route gate: off (E2E_ROUTE_GATE=0, a local run)');
    return;
  }
  const partial = process.argv.slice(2).some(
    (a) => a !== 'test' && (!a.startsWith('-') || /^--(grep|grep-invert|last-failed|shard|only-changed|project)\b|^-g$/.test(a)),
  );
  if (partial) {
    console.log('route gate: skipped, this is a partial run');
    return;
  }
  const failures = checkRoutes(join(HERE, '..'), join(HERE, 'route-gaps.txt'));
  if (failures.length > 0) {
    console.error('\nroute gate failed:\n  ' + failures.join('\n  '));
    process.exitCode = 1;
    throw new Error(`route gate: ${failures.length} route(s) not covered`);
  }
  console.log('route gate: every console route was visited or driven');
}
