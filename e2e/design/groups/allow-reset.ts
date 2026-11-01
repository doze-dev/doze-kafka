import type { Allowed } from '../lib/compare';
import { shellAllow } from '../shell/common-allow';

// What the live Offsets tab (Gr-06) may draw differently from its board, each with the
// reason it is allowed. An entry that stops matching fails the gate.

const AD1_FILTER = 'AD1: a filter field at the top of the list pane, which the board does not draw; the list starts 45 px lower';
const SAMPLE_LIST = 'a board sample that is not true of the live page: the board lists four groups (g-order-validator above the open one); the world has the one group g-loyalty-etl, and a list the broker holds one group of draws one';
const LIST_UNDERLINE = 'the boards disagree: Tp-Home and Tp-05 set their list items\' text-decoration to none, Gr-Home, Sc-Home and Ac-Home leave them to con.css\'s link underline; the console draws every list pane the one way (homes/allow.ts has the same entry)';
const DOCK_IN_MAIN = 'the board draws the wire dock inside main (a margin of -20px to the edges); the console\'s dock is the row under main in the panes grid, so main is the dock\'s height shorter (79 px here: its rows are the world\'s requests, not the board\'s two)';
const KEYS = 'the strip lists the keys the Offsets tab has (j k over rows, ↵ open, esc close the panel, g g, ?); the board\'s "↵ apply, esc cancel" are not keys of this page: Enter in a field of the form applies it, and Cancel is its button, but the page has no key map entry for either, and a hint for a key that does nothing there is false';
const BOTH = ['1440-light', '1440-dark'];

export const RESET_ALLOW: Allowed[] = [
  ...shellAllow('Gr-06'),
  { board: 'Gr-06', el: 'keys strip', prop: 'text', board_value: 'jkmove ↵apply esccancel ⌘Kjump or act', live_value: 'jkmove ↵open escclose panel gggroups ?all keys', at: BOTH, reason: KEYS },
  { board: 'Gr-06', el: 'list item', prop: 'presence', board_value: 'present', live_value: 'absent', at: BOTH, reason: SAMPLE_LIST },
  { board: 'Gr-06', el: 'list item on', prop: 'y', board_value: '136', live_value: '121', at: BOTH, reason: SAMPLE_LIST + '; and ' + AD1_FILTER },
  { board: 'Gr-06', el: 'list item on', prop: 'deco', board_value: 'underline', live_value: 'none', at: BOTH, reason: LIST_UNDERLINE },
  { board: 'Gr-06', el: 'main', prop: 'h', board_value: '828', live_value: '749', at: BOTH, reason: DOCK_IN_MAIN },
];
