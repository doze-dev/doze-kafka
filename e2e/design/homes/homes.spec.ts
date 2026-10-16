import { test, expect } from '@playwright/test';
import { BASE_URL } from '../../playwright.config';
import { livePage, runBoard, type BoardRun } from '../lib/gate';
import { HOME, SHELL } from '../lib/probes';
import { ALLOW } from './allow';

// The section homes (Tp-Home, Gr-Home, Sc-Home, Tc-Home, Ac-Home, Cl-Home) against
// the live homes, element by element, at 1440 in light and dark. What may differ is
// in allow.ts, each with its reason. The numbers are held to the seeded truth in
// data.spec.ts; the narrow widths in narrow.spec.ts.

const HOMES: [string, string][] = [
  ['Tp-Home', '/topics'], ['Gr-Home', '/groups'], ['Sc-Home', '/schemas'],
  ['Tc-Home', '/trace'], ['Ac-Home', '/access'], ['Cl-Home', '/cluster'],
];

for (const [id, path] of HOMES) {
  test(`${id} matches the live ${path}`, async ({ browser }) => {
    test.setTimeout(120_000);
    const run: BoardRun = { id, file: id, live: livePage(BASE_URL, path), probes: [...SHELL, ...HOME], allow: ALLOW };
    const out = await runBoard(browser, run);
    expect(out.text, out.text).toBe('');
  });
}
