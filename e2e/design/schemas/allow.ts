import type { Allowed } from '../lib/compare';
import { shellAllow } from '../shell/common-allow';

// What the live Schemas pages may draw differently from their boards, each with the
// reason it is allowed. One board, one element, one property, the two values as
// measured; an entry that stops matching fails the gate.

const BOTH = ['1440-light', '1440-dark'];
type Prop = Allowed['prop'];
const at = (board: string, el: string, prop: Prop, b: string, l: string, reason: string): Allowed => ({ board, el, prop, board_value: b, live_value: l, at: BOTH, reason });

const DOCK_IN_MAIN = 'the board draws the wire dock inside main (a margin of -20px to the edges); the console\'s dock is the row under main in the panes grid, so main is the dock\'s height shorter and the dock has no margin (groups/allow.ts has the same entry)';
const SY06_C = 'Sy-06 binds c to Create on Topics and Subjects; the Schemas boards\' strips name n, which no board binds (homes/allow.ts has the same entry for Sc-Home)';
const HOME_MORE = 'CUT: the board draws a ··· on the home with nothing it holds; the design puts Delete under ··· (Grounding) and a home has nothing to delete; without it the title column is the ···\'s width wider (homes/allow.ts has the same entry for Sc-Home)';
const NO_SENTENCE_ROW = 'the board draws a second row under the client\'s request, a sentence ("Nothing else has asked the registry yet. [] is the answer to /subjects."); the console\'s dock lists the requests the registry kept and adds no sentence once there is one, so the dock is that row\'s 28 px shorter and everything in it 28 px lower';
const NO_SASL = 'the board\'s producer lines carry the SASL login (basic.auth.credentials.source, basic.auth.user.info, two lines of 19 px); the gate\'s broker has no SASL, so the lines are not there and the card is 38 px shorter, and the row with it';

export const ALLOW: Allowed[] = [
  ...['Sc-01'].flatMap((b) => shellAllow(b)),
  at('Sc-01', 'keys strip', 'text', 'nregister schema ⌘Kjump or act ?keys', 'cregister schema ⌘Kjump or act ?keys', SY06_C),
  at('Sc-01', 'key', 'text', 'n', 'c', SY06_C),
  at('Sc-01', 'h1', 'w', '340.5', '347.7', HOME_MORE),
  at('Sc-01', 'register button', 'x', '1252.6', '1297.4', HOME_MORE),
  at('Sc-01', 'steps row', 'h', '353', '315', NO_SASL),
  at('Sc-01', 'side', 'h', '353', '315', NO_SASL),
  at('Sc-01', 'dock', 'margin', '0px -20px', '0px', DOCK_IN_MAIN),
  at('Sc-01', 'main', 'h', '828', '743', DOCK_IN_MAIN),
  at('Sc-01', 'dock', 'h', '113', '85', NO_SENTENCE_ROW),
  ...([['dock', '759'], ['dock head', '760'], ['dock title', '764.5'], ['dock pill', '764.5'], ['dock note', '764.5'], ['dock table head', '790'],
    ['dock head cell', '794.5'], ['dock row', '816'], ['dock time', '819.5'], ['dock method', '819.5'], ['dock path', '819.5'], ['dock status', '819.5']] as const)
    .map(([el, y]) => at('Sc-01', el, 'y', y, String(Number(y) + 28), NO_SENTENCE_ROW)),
];
