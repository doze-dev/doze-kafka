// Starts the Traffic world and keeps it running, for working on the pages against it:
//
//	cd e2e && bun design/traffic/up.ts            # prints the console's address
//	DESIGN_WORLD=1 bunx playwright test -c design/playwright.config.ts traffic
//
// The spec then measures this world instead of making its own (the world's address and
// ledger are in e2e/.tmp/design-world.json). Ctrl-C stops it all.

import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { ROOT } from '../lib/render';
import { startWorld } from './world';

const base = Number(process.env.DESIGN_PORT ?? 14160);
const w = await startWorld(base, '/_console');
await w.scenario();
writeFileSync(join(ROOT, 'e2e', '.tmp', 'design-world.json'), JSON.stringify({ console: w.console, kafka: w.kafka, ledger: w.ledger.dir, began: w.began, base }));
console.log(`world up: ${w.console}`);
process.on('SIGINT', async () => { await w.stopAll(); process.exit(0); });
setInterval(() => {}, 1 << 30);
