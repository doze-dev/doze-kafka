import { execFileSync } from 'node:child_process';
import { appendFileSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));

// The route gate (foundation B8). console/routes.go lists every route the console
// answers, with its method and whether it mutates. The suite fails for a GET no
// test ever visited and a POST no test ever drove, unless e2e/route-gaps.txt gives
// a reason. The table is read from the binary's own source of truth (e2e/routes),
// and what was requested is recorded by the fixtures (fixtures/console.ts), for the
// browser's own requests and for the `request` fixture.

export const SEEN_DIR = join(HERE, '.tmp', 'routes-seen');
const PREFIX = '/_console';

type Row = { method: string; path: string; mutates: boolean; test: string; note: string };

/** Records one request the suite made to the console. Called from the fixtures. */
export function recordRequest(method: string, rawUrl: string, spec: string) {
  let pathname: string;
  try {
    pathname = new URL(rawUrl).pathname;
  } catch {
    return;
  }
  if (!pathname.startsWith(PREFIX)) return;
  mkdirSync(SEEN_DIR, { recursive: true });
  appendFileSync(join(SEEN_DIR, `${process.pid}.log`), `${method.toUpperCase()} ${pathname.slice(PREFIX.length)} ${spec}\n`);
}

export function resetSeen() {
  rmSync(SEEN_DIR, { recursive: true, force: true });
}

/** A route pattern as a regular expression: {x} is one segment, "/static/" is a subtree. */
function matcher(path: string): RegExp {
  if (path === '/static/') return /^\/static\/.+$/;
  const segs = path
    .split('/')
    .map((seg) => (/^\{[a-z]+\}$/.test(seg) ? '[^/]+' : seg.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  return new RegExp('^' + segs.join('/') + '/?$');
}

function seenRequests(): Map<string, Set<string>> {
  const seen = new Map<string, Set<string>>();
  if (!existsSync(SEEN_DIR)) return seen;
  for (const f of readdirSync(SEEN_DIR)) {
    for (const line of readFileSync(join(SEEN_DIR, f), 'utf8').split('\n')) {
      const [method, path, spec] = line.split(' ');
      if (!method || path === undefined) continue;
      const key = `${method} ${path.split('?')[0] || '/'}`;
      if (!seen.has(key)) seen.set(key, new Set());
      seen.get(key)!.add(spec ?? '');
    }
  }
  return seen;
}

/** Parses route-gaps.txt: `METHOD /path reason...`, `#` comments. */
export function readGaps(file: string): { gaps: Map<string, string>; problems: string[] } {
  const gaps = new Map<string, string>();
  const problems: string[] = [];
  if (!existsSync(file)) return { gaps, problems };
  readFileSync(file, 'utf8').split('\n').forEach((raw, i) => {
    const line = raw.trim();
    if (!line || line.startsWith('#')) return;
    const m = /^(GET|POST)\s+(\S+)\s+(.+)$/.exec(line);
    if (!m) {
      problems.push(`route-gaps.txt:${i + 1}: want "METHOD /path reason", got "${line}"`);
      return;
    }
    gaps.set(`${m[1]} ${m[2]}`, m[3]);
  });
  return { gaps, problems };
}

/** Returns the failures, empty when the gate passes. */
export function checkRoutes(repoRoot: string, gapsFile: string): string[] {
  const table: Row[] = JSON.parse(
    execFileSync('go', ['run', './e2e/routes'], { cwd: repoRoot, env: { ...process.env, GOWORK: 'off' }, encoding: 'utf8' }),
  );
  const seen = seenRequests();
  const { gaps, problems } = readGaps(gapsFile);
  const failures = [...problems];
  const covered = new Set<string>();

  for (const row of table) {
    const key = `${row.method} ${row.path || '/'}`;
    const re = matcher(row.path || '/');
    let hit = false;
    for (const [k] of seen) {
      const [method, path] = k.split(' ');
      if (method === row.method && re.test(path)) {
        hit = true;
        break;
      }
    }
    if (hit) covered.add(key);
    const gapKey = `${row.method} ${row.path || '/'}`;
    if (!hit && !gaps.has(gapKey)) {
      failures.push(
        `${row.method} ${row.path || '/'} (${row.mutates ? 'mutates' : 'reads'}; ${row.note}; declared test ${row.test}) was never ${row.method === 'GET' ? 'visited' : 'driven'}: ` +
          `drive it in a spec, or give the reason in e2e/route-gaps.txt`,
      );
    }
    if (hit && gaps.has(gapKey)) failures.push(`${gapKey} is in route-gaps.txt but a test drives it: remove the line`);
  }
  const known = new Set(table.map((r) => `${r.method} ${r.path || '/'}`));
  for (const k of gaps.keys()) {
    if (!known.has(k)) failures.push(`route-gaps.txt names ${k}, which is not in the route table: remove the line`);
  }
  return failures;
}
