import { readdirSync, rmSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';

/**
 * The specs build their helper programs into e2e/.tmp/bin under names with the process id in
 * them (hold-<pid>, hold-strip-<pid>...), so no run ever reuses a name and nothing removed
 * them: hundreds of 40 MB files, 13 GB in a month. A run starts by removing what is older
 * than a few hours. Not everything: the suite and the design gate may run at the same time,
 * and one must not delete the other's binaries; no single run lasts hours.
 */
export function pruneOldBinaries(maxAgeMs = 3 * 3600 * 1000) {
  const dir = resolve(import.meta.dirname, '.tmp', 'bin');
  let names: string[];
  try { names = readdirSync(dir); } catch { return; }
  const cutoff = Date.now() - maxAgeMs;
  for (const n of names) {
    try {
      if (statSync(join(dir, n)).mtimeMs < cutoff) rmSync(join(dir, n), { recursive: true, force: true });
    } catch { /* another run removed it first */ }
  }
}
