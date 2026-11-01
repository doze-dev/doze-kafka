// Starts the Groups world and keeps it running, for working on the pages against it:
//   cd e2e && bun design/groups/up.ts     # prints the console's address
import { startGroupsWorld } from './world';
const base = Number(process.env.DESIGN_PORT ?? 14260);
const g = await startGroupsWorld(base, '/_console');
console.log(`empty: ${g.world.console}`);
await g.fill();
console.log(`filled: ${g.world.console}groups`);
process.on('SIGINT', async () => { await g.stop(); process.exit(0); });
setInterval(() => {}, 1 << 30);
