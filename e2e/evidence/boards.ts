// The boards the evidence run covers: Stage 0 (Sy-01 to Sy-08 and the section homes)
// and Stage 2 (Main and Traf-02 to Traf-12), with the live state that stands for each.
//
// The verdicts are copied from "Deviations from the boards" in plan/00-foundation.md
// (that table is the owner of them); this run adds screenshots and measurements, and
// a difference it measures that the table does not mention goes into the table.

export type Size = '1440' | '960' | '390';

export type BoardDef = {
  id: string;
  stage: 'stage-0' | 'stage-2';
  /** The design artifact's file under project/, for the reference render. */
  file: string;
  /** What the live page is made to show. */
  state: string;
  sizes: Size[];
  /** The verdict in plan/00-foundation.md. */
  verdict: string;
  /** The size the board itself is drawn at (default 1440). */
  boardSize?: Size;
  /** The theme the board is drawn in (default light). */
  boardTheme?: 'light' | 'dark';
};

const D = 'DEVIATES';
const M = 'MATCHES';
const C = 'CUT';

export const BOARDS: BoardDef[] = [
  // Stage 0: the shell.
  { id: 'Sy-01', stage: 'stage-0', file: 'Sy-01-Anatomy', state: 'a topic with a message open in the inspector', sizes: ['1440'], verdict: `${M} (specimen values ${D}: inspector 420 vs 400, padding 14 18 and h1 16 where the page boards draw 16 20 0 and 18, which the console follows)` },
  { id: 'Sy-02', stage: 'stage-0', file: 'Sy-02-Dark', state: 'the same page in the dark theme', sizes: ['1440'], boardTheme: 'dark', verdict: `${M} (specimen values ${D} as Sy-01; "Copy as curl" ${C})` },
  { id: 'Sy-03', stage: 'stage-0', file: 'Sy-03-Narrow', state: 'a topic with a message open at 960 (tablet)', sizes: ['960'], boardSize: '960', verdict: M },
  { id: 'Sy-04', stage: 'stage-0', file: 'Sy-04-Phone', state: 'a topic with a message open at 390 (phone)', sizes: ['390'], boardSize: '390', verdict: `${D} (the sheet's arrows and close icon; "Copy as curl" ${C})` },
  { id: 'Sy-05', stage: 'stage-0', file: 'Sy-05-Palette', state: 'the command palette open over a topic, "e" typed (the seed has one name with "ord" in it; a letter in more names shows the four groups of results)', sizes: ['1440'], verdict: `${M} (specimen values ${D} as Sy-01; hint of "Follow a message by trace id" ${D})` },
  { id: 'Sy-06', stage: 'stage-0', file: 'Sy-06-Keys', state: 'the keyboard shortcuts overlay over Traffic', sizes: ['1440'], verdict: M },
  { id: 'Sy-07', stage: 'stage-0', file: 'Sy-07-Themes', state: 'the Topics page in light, dark and high contrast (the contrast file has no board image of its own)', sizes: ['1440'], verdict: M },
  { id: 'Sy-08', stage: 'stage-0', file: 'Sy-08-FirstRun', state: 'a fresh, empty broker: "Your broker is up"', sizes: ['1440', '390'], verdict: `${D} (copy kept where the board's would be false for this build)` },
  { id: 'Tp-Home', stage: 'stage-0', file: 'Tp-Home', state: 'Topics, nothing open', sizes: ['1440'], verdict: M },
  { id: 'Gr-Home', stage: 'stage-0', file: 'Gr-Home', state: 'Groups, nothing open', sizes: ['1440'], verdict: M },
  { id: 'Sc-Home', stage: 'stage-0', file: 'Sc-Home', state: 'Schemas, nothing open', sizes: ['1440'], verdict: `${D} (registry wire dock ${C})` },
  { id: 'Tc-Home', stage: 'stage-0', file: 'Tc-Home', state: 'Trace, the field and nothing traced', sizes: ['1440'], verdict: `${D} (no recent-messages table; owner: trace)` },
  { id: 'Ac-Home', stage: 'stage-0', file: 'Ac-Home', state: 'Access, nothing open', sizes: ['1440'], verdict: M },
  { id: 'Cl-Home', stage: 'stage-0', file: 'Cl-Home', state: 'Cluster, one page with its sections', sizes: ['1440'], verdict: `${D} (sub-pages are sections; the wire dock ${C}, CLU-32; owner: cluster)` },
  { id: 'Tp-24', stage: 'stage-0', file: 'Tp-24-Confirms', state: 'the shell confirm dialog (Purge a topic, cancelled)', sizes: ['1440'], verdict: M },

  // Stage 2: Traffic.
  { id: 'Main', stage: 'stage-2', file: 'Main', state: 'Traffic, live, nothing open', sizes: ['1440', '390'], verdict: `${D} (AD4 to AD6, AD14; Request 130, Client/Topic/Detail share the rest beside the inspector)` },
  { id: 'Traf-02', stage: 'stage-2', file: 'Traf-02-Filter', state: 'filtered to a topic, with "Who touched"', sizes: ['1440'], verdict: M },
  { id: 'Traf-03', stage: 'stage-2', file: 'Traf-03-Errors', state: 'errors only, the most frequent refusal explained', sizes: ['1440'], verdict: `${D} (refusals purple, headline wording)` },
  { id: 'Traf-04', stage: 'stage-2', file: 'Traf-04-Request', state: 'a Produce request open, its decoded fields', sizes: ['1440'], verdict: `${D} (field names in snake case)` },
  { id: 'Traf-05', stage: 'stage-2', file: 'Traf-05-Folded', state: 'a folded heartbeat row open, its ticks', sizes: ['1440'], verdict: `${D} (latest generation, last 64 repeats)` },
  { id: 'Traf-06', stage: 'stage-2', file: 'Traf-06-Clients', state: 'the Clients view, nothing open', sizes: ['1440', '390'], verdict: `${D} (built from CUT; see the table)` },
  { id: 'Traf-07', stage: 'stage-2', file: 'Traf-07-Client', state: 'a client that reports metrics, open', sizes: ['1440'], verdict: `${D} (labels, line not bars, "worth a look" only when non-zero)` },
  { id: 'Traf-08', stage: 'stage-2', file: 'Traf-08-Paused', state: 'paused, with the count of requests missed', sizes: ['1440'], verdict: M },
  { id: 'Traf-09', stage: 'stage-2', file: 'Traf-09-Empty', state: 'a broker nothing has asked anything of', sizes: ['1440'], verdict: `${D} (adds start time and the restart sentence)` },
  { id: 'Traf-10', stage: 'stage-2', file: 'Traf-10-Disconnected', state: 'the broker killed under an open page: rows frozen', sizes: ['1440'], verdict: `${D} (no link to Bad-Down; retry every 5 s)` },
  { id: 'Traf-11', stage: 'stage-2', file: 'Traf-11-Metrics-None', state: 'a client that reports no metrics, open', sizes: ['1440'], verdict: `${D} ("asked and got none" is false)` },
  { id: 'Traf-12', stage: 'stage-2', file: 'Traf-12-Rebalance-Story', state: 'a group filter: the rebalance told in order', sizes: ['1440'], verdict: `${D} (Epochs for 848, recorder detail)` },
];
