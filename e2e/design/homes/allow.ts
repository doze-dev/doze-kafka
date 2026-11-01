import type { Allowed } from '../lib/compare';
import { shellAllow } from '../shell/common-allow';

// What the live section homes and their confirms may draw differently from their
// boards, each with the reason it is allowed. Every entry is one board, one element,
// one property and the two values as measured; an entry that stops matching fails
// the gate. plan/00-foundation.md, "Deviations from the boards", says the same in words.

const BOTH = ['1440-light', '1440-dark'];

const DOT = 'the dot is inside the broker pill, whose address is read to a screen reader and not drawn, and the pill\'s type is 12 px; Sy-04\'s dot sits in the top bar\'s 13 px; a dot has no text';

// ---------- data: the seed's world is not the boards' sample ----------
const TOPIC_COUNT = 'the count is the seed\'s 14 topics; the board\'s sample has 7';
const DATA_DIR = 'it names this broker\'s data directory (data-14192 in the gate\'s run); the board\'s is ./data';
const REGISTRY_ADDR = 'it names the registry\'s address, the gate\'s port; the board\'s is 127.0.0.1:8081';
const GROUP_STATE = 'the state is the seed\'s: its first group is Empty (grey) where the board\'s sample is Stable (green)';
const GROUP_LAG = 'the lag is the seed\'s: its first group is 39 records behind (warn) where the board\'s sample is 0 (ink)';
const ACL_ROWS = 'it is below a table of the seed\'s 6 ACLs; the board\'s sample has 5';
const ACL_DOCK_ROWS = 'the wire\'s rows are the requests about ACLs and the refusals among those the broker keeps; the seed\'s run leaves none in the ring by the time the gate reads it';
const CL_SENTENCE = 'Cl-Home\'s sentence carries this broker\'s address, uptime and counts, and runs to a second line; what follows it is 22 px lower';
const SAMPLE_SELECTED = 'the board draws its first row selected, as a sample of a selected row; on a home nothing is selected (its note says so)';

// ---------- additions (plan/00-foundation.md, Additions to the design) ----------
const AD1_FILTER = 'AD1: a filter field at the top of the list pane, which the board does not draw; the list starts 45 px lower';

// ---------- deviations, each recorded in plan/00-foundation.md ----------
const LIST_UNDERLINE = 'the boards disagree: Tp-Home and Tp-05 set their list items\' text-decoration to none, Gr-Home, Sc-Home and Ac-Home leave them to con.css\'s link underline; the console draws every list pane the one way';
const SY06_C = 'Sy-06 binds c to Create on Topics and Subjects; Sc-Home\'s strip names n, which no board binds';
const HOME_MORE = 'CUT: the board draws a ··· on the home with nothing it holds; the design puts Delete under ··· (Grounding) and a home has nothing to delete; without it the title column is the ···\'s width wider';
const REGISTRY_DOCK = 'CUT: the registry\'s wire (the board\'s dock under Schemas): the registry\'s requests are not recorded by the traffic recorder, so there is no source for it';
const TC_ROWS = 'the board\'s rows carry no tracks (only its header row sets --c), so its own render stacks each row\'s cells in one 930 px column; the console gives the rows the header\'s tracks';
const TC_SHRINK = 'the board\'s page is taller than its 900 px frame, and its column of cards shrinks them to fit (the trace form renders 96 px for 108 px of content); the console\'s page scrolls instead, so what follows the form is lower';
const ACL_DOCK_ROW = 'the wire is the page\'s own row under main, as on a topic and a group (one grid for every page); the board draws Access\'s inside main with negative margins, and its main runs to the foot';
const NEW_TAG = 'the board\'s button carries the design\'s "new" mark, an annotation of what is new to the design (Grounding), not a part of the button';
const CL_KEYS = 'the strip adds j k: ↵ opens the row the cursor is on, and j and k put it there (strip.spec presses each)';
const CL_KAFBAT = 'the board\'s second sentence says what Kafbat UI shows, a claim about another program this project has not recorded; the console keeps the first';
const CL_DOCK = 'CUT (CLU-32): Cl-Home\'s wire is what this page asked the broker, and the recorder leaves the console\'s own requests out at the door, so there is no source for it';

// The board places its two specimen dialogs side by side with absolute positions;
// the console opens one at a time in an overlay that centres it 150 px down.
const ONE_DIALOG = 'Tp-24 draws the purge and the delete side by side as two specimens, placed absolutely over a scrim; the console opens one at a time, centred in an overlay with the board\'s 150 px above it';

const confirm = (board: string, x: string): Allowed[] => [
  ...shellAllow(board),
  { board, el: 'scrim', prop: 'display', board_value: 'block', live_value: 'grid', at: BOTH, reason: ONE_DIALOG },
  { board, el: 'scrim', prop: 'padding', board_value: '0px', live_value: '150px 16px 16px', at: BOTH, reason: ONE_DIALOG },
  { board, el: 'scrim', prop: 'tracks', board_value: '', live_value: '1408px', at: BOTH, reason: ONE_DIALOG },
  { board, el: 'dialog', prop: 'x', board_value: x, live_value: '485', at: BOTH, reason: ONE_DIALOG },
];

export const ALLOW: Allowed[] = [
  ...confirm('Tp-24 purge', '452'),
  ...confirm('Tp-24 delete', '948'),
  ...['Tp-Home', 'Gr-Home', 'Sc-Home', 'Tc-Home', 'Ac-Home', 'Cl-Home'].flatMap((b) => shellAllow(b)),
  // At 960 and 390 (narrow.spec): Sy-03's shell and Sy-04's on every home.
  ...['Tp-Home', 'Gr-Home', 'Sc-Home', 'Tc-Home', 'Ac-Home', 'Cl-Home'].flatMap((b) => [
    ...shellAllow(`${b} at 960`, ['960-dark', '960-light']),
    { board: `${b} at 390`, el: 'broker dot', prop: 'font', board_value: 'IBM Plex Sans 400 13px/20px', live_value: 'IBM Plex Sans 400 12px/20px', at: ['390-dark', '390-light'], reason: DOT } as Allowed,
  ]),
  { board: 'Tp-Home', el: 'list cap', prop: 'w', board_value: "129.1", live_value: "136", at: ['1440-dark', '1440-light'], reason: TOPIC_COUNT },
  { board: 'Tp-Home', el: 'list cap', prop: 'text', board_value: "TOPICS · 7 · MESSAGES", live_value: "TOPICS · 14 · MESSAGES", at: ['1440-dark', '1440-light'], reason: TOPIC_COUNT },
  { board: 'Tp-Home', el: 'list item', prop: 'y', board_value: "89", live_value: "134", at: ['1440-dark', '1440-light'], reason: AD1_FILTER },
  { board: 'Tp-Home', el: 'list name', prop: 'y', board_value: "93.5", live_value: "138.5", at: ['1440-dark', '1440-light'], reason: AD1_FILTER },
  { board: 'Tp-Home', el: 'list number', prop: 'y', board_value: "93.5", live_value: "138.5", at: ['1440-dark', '1440-light'], reason: AD1_FILTER },
  { board: 'Tp-Home', el: 'foot note', prop: 'text', board_value: "Size is what is on disk under ./data. Retention shows the topic's own setting, or the broker default.", live_value: "Size is what is on disk under data-14192. Retention shows the topic's own setting, or the broker default.", at: ['1440-dark', '1440-light'], reason: DATA_DIR },
  { board: 'Gr-Home', el: 'list item', prop: 'y', board_value: "76", live_value: "121", at: ['1440-dark', '1440-light'], reason: AD1_FILTER },
  { board: 'Gr-Home', el: 'list item', prop: 'deco', board_value: "underline", live_value: "none", at: ['1440-dark', '1440-light'], reason: LIST_UNDERLINE },
  { board: 'Gr-Home', el: 'list name', prop: 'y', board_value: "87", live_value: "132", at: ['1440-dark', '1440-light'], reason: AD1_FILTER },
  { board: 'Gr-Home', el: 'list name', prop: 'bg', board_value: "rgb(22, 107, 63)", live_value: "rgb(217, 216, 210)", at: ['1440-light'], reason: GROUP_STATE },
  { board: 'Gr-Home', el: 'list number', prop: 'y', board_value: "80.5", live_value: "125.5", at: ['1440-dark', '1440-light'], reason: AD1_FILTER },
  { board: 'Gr-Home', el: 'list number', prop: 'color', board_value: "rgb(23, 25, 28)", live_value: "rgb(133, 80, 0)", at: ['1440-light'], reason: GROUP_LAG },
  { board: 'Gr-Home', el: 'cell 3', prop: 'color', board_value: "rgb(22, 107, 63)", live_value: "rgb(86, 91, 99)", at: ['1440-light'], reason: GROUP_STATE },
  { board: 'Gr-Home', el: 'list name', prop: 'bg', board_value: "rgb(95, 202, 143)", live_value: "rgb(45, 49, 54)", at: ['1440-dark'], reason: GROUP_STATE },
  { board: 'Gr-Home', el: 'list number', prop: 'color', board_value: "rgb(233, 234, 235)", live_value: "rgb(224, 176, 96)", at: ['1440-dark'], reason: GROUP_LAG },
  { board: 'Gr-Home', el: 'cell 3', prop: 'color', board_value: "rgb(95, 202, 143)", live_value: "rgb(163, 169, 176)", at: ['1440-dark'], reason: GROUP_STATE },
  { board: 'Sc-Home', el: 'keys strip', prop: 'text', board_value: "jkmove ↵open subject /filter nregister schema ⌘Kjump or act", live_value: "jkmove ↵open subject /filter cregister schema ⌘Kjump or act", at: ['1440-dark', '1440-light'], reason: SY06_C },
  { board: 'Sc-Home', el: 'list item', prop: 'deco', board_value: "underline", live_value: "none", at: ['1440-dark', '1440-light'], reason: LIST_UNDERLINE },
  { board: 'Sc-Home', el: 'h1', prop: 'w', board_value: "724.3", live_value: "731.5", at: ['1440-dark', '1440-light'], reason: HOME_MORE },
  { board: 'Sc-Home', el: 'h1 note', prop: 'w', board_value: "724.3", live_value: "731.5", at: ['1440-dark', '1440-light'], reason: HOME_MORE },
  { board: 'Sc-Home', el: 'h1 note', prop: 'text', board_value: "The built-in registry at 127.0.0.1:8081. Confluent's REST API, so stock serializers work with only schema.registry.url changed.", live_value: "The built-in registry at 127.0.0.1:14194. Confluent's REST API, so stock serializers work with only schema.registry.url changed.", at: ['1440-dark', '1440-light'], reason: REGISTRY_ADDR },
  { board: 'Sc-Home', el: 'more button', prop: 'presence', board_value: "present", live_value: "absent", at: ['1440-dark', '1440-light'], reason: HOME_MORE },
  { board: 'Sc-Home', el: 'row', prop: 'bg', board_value: "rgb(233, 232, 226)", live_value: "rgba(0, 0, 0, 0)", at: ['1440-light'], reason: SAMPLE_SELECTED },
  { board: 'Sc-Home', el: 'pill in main', prop: 'presence', board_value: "present", live_value: "absent", at: ['1440-dark', '1440-light'], reason: REGISTRY_DOCK },
  { board: 'Sc-Home', el: 'dock', prop: 'presence', board_value: "present", live_value: "absent", at: ['1440-dark', '1440-light'], reason: REGISTRY_DOCK },
  { board: 'Sc-Home', el: 'dock head', prop: 'presence', board_value: "present", live_value: "absent", at: ['1440-dark', '1440-light'], reason: REGISTRY_DOCK },
  { board: 'Sc-Home', el: 'dock row', prop: 'presence', board_value: "present", live_value: "absent", at: ['1440-dark', '1440-light'], reason: REGISTRY_DOCK },
  { board: 'Sc-Home', el: 'row', prop: 'bg', board_value: "rgb(38, 42, 47)", live_value: "rgba(0, 0, 0, 0)", at: ['1440-dark'], reason: SAMPLE_SELECTED },
  { board: 'Tc-Home', el: 'table head', prop: 'y', board_value: "262.5", live_value: "277", at: ['1440-dark', '1440-light'], reason: TC_SHRINK },
  { board: 'Tc-Home', el: 'head cell', prop: 'y', board_value: "267", live_value: "281.5", at: ['1440-dark', '1440-light'], reason: TC_SHRINK },
  { board: 'Tc-Home', el: 'row', prop: 'y', board_value: "288.5", live_value: "303", at: ['1440-dark', '1440-light'], reason: TC_SHRINK },
  { board: 'Tc-Home', el: 'row', prop: 'h', board_value: "105", live_value: "28", at: ['1440-dark', '1440-light'], reason: TC_ROWS },
  { board: 'Tc-Home', el: 'row', prop: 'bg', board_value: "rgb(233, 232, 226)", live_value: "rgba(0, 0, 0, 0)", at: ['1440-light'], reason: SAMPLE_SELECTED },
  { board: 'Tc-Home', el: 'row', prop: 'tracks', board_value: "930px", live_value: "92px 311px 70px 311px 90px", at: ['1440-dark', '1440-light'], reason: TC_ROWS },
  { board: 'Tc-Home', el: 'cell 1', prop: 'y', board_value: "288.5", live_value: "306.5", at: ['1440-dark', '1440-light'], reason: TC_ROWS },
  { board: 'Tc-Home', el: 'cell 1', prop: 'w', board_value: "930", live_value: "92", at: ['1440-dark', '1440-light'], reason: TC_ROWS },
  { board: 'Tc-Home', el: 'cell 2', prop: 'x', board_value: "477", live_value: "583", at: ['1440-dark', '1440-light'], reason: TC_ROWS },
  { board: 'Tc-Home', el: 'cell 2', prop: 'y', board_value: "308.5", live_value: "306.5", at: ['1440-dark', '1440-light'], reason: TC_ROWS },
  { board: 'Tc-Home', el: 'cell 2', prop: 'w', board_value: "930", live_value: "311", at: ['1440-dark', '1440-light'], reason: TC_ROWS },
  { board: 'Tc-Home', el: 'cell 3', prop: 'x', board_value: "477", live_value: "908", at: ['1440-dark', '1440-light'], reason: TC_ROWS },
  { board: 'Tc-Home', el: 'cell 3', prop: 'y', board_value: "328.5", live_value: "306.5", at: ['1440-dark', '1440-light'], reason: TC_ROWS },
  { board: 'Tc-Home', el: 'cell 3', prop: 'w', board_value: "930", live_value: "70", at: ['1440-dark', '1440-light'], reason: TC_ROWS },
  { board: 'Tc-Home', el: 'row', prop: 'bg', board_value: "rgb(38, 42, 47)", live_value: "rgba(0, 0, 0, 0)", at: ['1440-dark'], reason: SAMPLE_SELECTED },
  { board: 'Ac-Home', el: 'list item on', prop: 'y', board_value: "74", live_value: "119", at: ['1440-dark', '1440-light'], reason: AD1_FILTER },
  { board: 'Ac-Home', el: 'list item on', prop: 'deco', board_value: "underline", live_value: "none", at: ['1440-dark', '1440-light'], reason: LIST_UNDERLINE },
  { board: 'Ac-Home', el: 'main', prop: 'h', board_value: "828", live_value: "765", at: ['1440-dark', '1440-light'], reason: ACL_DOCK_ROW },
  { board: 'Ac-Home', el: 'primary button', prop: 'w', board_value: "125.7", live_value: "89.7", at: ['1440-dark', '1440-light'], reason: NEW_TAG },
  { board: 'Ac-Home', el: 'more button', prop: 'presence', board_value: "present", live_value: "absent", at: ['1440-dark', '1440-light'], reason: HOME_MORE },
  { board: 'Ac-Home', el: 'field', prop: 'y', board_value: "487", live_value: "515", at: ['1440-dark', '1440-light'], reason: ACL_ROWS },
  { board: 'Ac-Home', el: 'pill in main', prop: 'presence', board_value: "present", live_value: "absent", at: ['1440-dark', '1440-light'], reason: ACL_DOCK_ROW },
  { board: 'Ac-Home', el: 'foot note', prop: 'y', board_value: "418", live_value: "446", at: ['1440-dark', '1440-light'], reason: ACL_ROWS },
  { board: 'Ac-Home', el: 'dock', prop: 'y', board_value: "757", live_value: "809", at: ['1440-dark', '1440-light'], reason: ACL_DOCK_ROW },
  { board: 'Ac-Home', el: 'dock', prop: 'h', board_value: "115", live_value: "63", at: ['1440-dark', '1440-light'], reason: ACL_DOCK_ROWS },
  { board: 'Ac-Home', el: 'dock', prop: 'margin', board_value: "0px -20px", live_value: "0px", at: ['1440-dark', '1440-light'], reason: ACL_DOCK_ROW },
  { board: 'Ac-Home', el: 'dock head', prop: 'y', board_value: "758", live_value: "810", at: ['1440-dark', '1440-light'], reason: ACL_DOCK_ROW },
  { board: 'Ac-Home', el: 'dock row', prop: 'presence', board_value: "present", live_value: "absent", at: ['1440-dark', '1440-light'], reason: ACL_DOCK_ROWS },
  { board: 'Cl-Home', el: 'keys strip', prop: 'text', board_value: "gccluster ↵open ccopy bootstrap ⌘Kjump or act", live_value: "gccluster jkmove ↵open ccopy bootstrap ⌘Kjump or act", at: ['1440-dark', '1440-light'], reason: CL_KEYS },
  { board: 'Cl-Home', el: 'more button', prop: 'presence', board_value: "present", live_value: "absent", at: ['1440-dark', '1440-light'], reason: HOME_MORE },
  { board: 'Cl-Home', el: 'table head', prop: 'y', board_value: "178", live_value: "201", at: ['1440-dark', '1440-light'], reason: CL_SENTENCE },
  { board: 'Cl-Home', el: 'head cell', prop: 'y', board_value: "182.5", live_value: "205.5", at: ['1440-dark', '1440-light'], reason: CL_SENTENCE },
  { board: 'Cl-Home', el: 'row', prop: 'y', board_value: "204", live_value: "227", at: ['1440-dark', '1440-light'], reason: CL_SENTENCE },
  { board: 'Cl-Home', el: 'cell 1', prop: 'y', board_value: "207.5", live_value: "230.5", at: ['1440-dark', '1440-light'], reason: CL_SENTENCE },
  { board: 'Cl-Home', el: 'cell 2', prop: 'y', board_value: "207.5", live_value: "230.5", at: ['1440-dark', '1440-light'], reason: CL_SENTENCE },
  { board: 'Cl-Home', el: 'pill in main', prop: 'y', board_value: "452", live_value: "475", at: ['1440-dark', '1440-light'], reason: CL_SENTENCE },
  { board: 'Cl-Home', el: 'foot note', prop: 'y', board_value: "531", live_value: "554", at: ['1440-dark', '1440-light'], reason: CL_SENTENCE },
  { board: 'Cl-Home', el: 'foot note', prop: 'h', board_value: "40", live_value: "20", at: ['1440-dark', '1440-light'], reason: CL_KAFBAT },
  { board: 'Cl-Home', el: 'foot note', prop: 'text', board_value: "Replication factor 1 is what every topic here reports, because there is nowhere to put a second copy. Kafbat UI shows the controller as ZooKeeper and the version as unknown for the same reason.", live_value: "Replication factor 1 is what every topic here reports, because there is nowhere to put a second copy.", at: ['1440-dark', '1440-light'], reason: CL_KAFBAT },
  { board: 'Cl-Home', el: 'dock', prop: 'presence', board_value: "present", live_value: "absent", at: ['1440-dark', '1440-light'], reason: CL_DOCK },
  { board: 'Cl-Home', el: 'dock head', prop: 'presence', board_value: "present", live_value: "absent", at: ['1440-dark', '1440-light'], reason: CL_DOCK },
  { board: 'Cl-Home', el: 'dock row', prop: 'presence', board_value: "present", live_value: "absent", at: ['1440-dark', '1440-light'], reason: CL_DOCK },
];
