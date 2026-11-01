import type { Allowed } from '../lib/compare';
import { shellAllow } from '../shell/common-allow';

// What the live Groups pages may draw differently from their boards, each with the
// reason it is allowed. One board, one element, one property, the two values as
// measured; an entry that stops matching fails the gate.

const AD1_FILTER = 'AD1: a filter field at the top of the list pane, which the board does not draw; the list starts 45 px lower';
const BOTH = ['1440-light', '1440-dark'];
const ad1 = (board: string, el: string, b: string, l: string): Allowed => ({ board, el, prop: 'y', board_value: b, live_value: l, at: BOTH, reason: AD1_FILTER });

export const ALLOW: Allowed[] = [
  ...['Gr-01', 'Gr-02', 'Gr-03'].flatMap((b) => shellAllow(b)),
  ad1('Gr-01', 'list none', '76', '121'),
  ad1('Gr-01', 'list none title', '94', '139'),
  ad1('Gr-01', 'list none note', '118', '163'),
];
