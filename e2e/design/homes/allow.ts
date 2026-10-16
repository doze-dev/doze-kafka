import type { Allowed } from '../lib/compare';
import { shellAllow } from '../shell/common-allow';

// What the live section homes and their confirms may draw differently from their
// boards, each with the reason it is allowed. Every entry is one board, one element,
// one property and the two values as measured; an entry that stops matching fails
// the gate. plan/00-foundation.md, "Deviations from the boards", says the same in words.

const BOTH = ['1440-light', '1440-dark'];

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
];
