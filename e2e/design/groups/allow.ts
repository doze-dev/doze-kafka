import type { Allowed } from '../lib/compare';
import { shellAllow } from '../shell/common-allow';

// What the live Groups pages may draw differently from their boards, each with the
// reason it is allowed. One board, one element, one property, the two values as
// measured; an entry that stops matching fails the gate.

const AD1_FILTER = 'AD1: a filter field at the top of the list pane, which the board does not draw; the list starts 45 px lower';
const BOTH = ['1440-light', '1440-dark'];
const ad1 = (board: string, el: string, b: string, l: string): Allowed => ({ board, el, prop: 'y', board_value: b, live_value: l, at: BOTH, reason: AD1_FILTER });

const SAMPLE_LIST = 'a board sample that is not true of the live page: the board lists four groups (g-order-validator above the open one); the world has the one group g-depot-router, and a list the broker holds one group of draws one';
const LIST_UNDERLINE = 'the boards disagree: Tp-Home and Tp-05 set their list items\' text-decoration to none, Gr-Home, Sc-Home and Ac-Home leave them to con.css\'s link underline; the console draws every list pane the one way (homes/allow.ts has the same entry)';
const DOCK_IN_MAIN = 'the board draws the wire dock inside main (a margin of -20px to the edges); the console\'s dock is the row under main in the panes grid, so main is the dock\'s 103 px shorter';

const INSP_HEAD = 'the inspector\'s header is the one every kind of selection shares (Sy-01, the inspector contract): the title block and the close control side by side, 12 px apart; the board draws Gr-03\'s header with the title block alone, 4 px between its lines';
const INSP_LEADER = 'the member\'s inspector also says whether it is the leader (GRP-22: the broker holds it for a classic group, and says the consumer protocol has none), a ninth row of 20 px and its 5 px gap that the board does not draw';
const INSP_CODE = 'the board\'s subscription block is three lines (topics, userdata, protocols); the broker holds the protocol names only (the topics are inside the member\'s metadata bytes), so the live block is one line and what follows it is 13 px higher';

export const ALLOW: Allowed[] = [
  ...['Gr-01', 'Gr-02', 'Gr-03'].flatMap((b) => shellAllow(b)),
  ad1('Gr-01', 'list none', '76', '121'),
  ad1('Gr-01', 'list none title', '94', '139'),
  ad1('Gr-01', 'list none note', '118', '163'),
  ...['Gr-02', 'Gr-03'].flatMap((b) => [
    { board: b, el: 'list item', prop: 'presence', board_value: 'present', live_value: 'absent', at: BOTH, reason: SAMPLE_LIST },
    { board: b, el: 'list item on', prop: 'y', board_value: '106', live_value: '121', at: BOTH, reason: SAMPLE_LIST + '; and ' + AD1_FILTER },
    { board: b, el: 'list item on', prop: 'deco', board_value: 'underline', live_value: 'none', at: BOTH, reason: LIST_UNDERLINE },
    { board: b, el: 'main', prop: 'h', board_value: '828', live_value: '725', at: BOTH, reason: DOCK_IN_MAIN },
  ]),
  ...[['inspector head', 'gap', '4px 4px', '12px 12px', INSP_HEAD], ['inspector kv', 'h', '195', '220', INSP_LEADER],
    ['inspector subscription cap', 'y', '350', '375', INSP_LEADER], ['inspector subscription', 'y', '370', '395', INSP_LEADER],
    ['inspector assignment cap', 'y', '461', '448', INSP_LEADER + '; ' + INSP_CODE], ['inspector assignment', 'y', '481', '468', INSP_LEADER + '; ' + INSP_CODE],
    ['inspector buttons', 'y', '553', '540', INSP_LEADER + '; ' + INSP_CODE], ['inspector button', 'y', '553', '540', INSP_LEADER + '; ' + INSP_CODE],
  ].map(([el, prop, b, l, reason]): Allowed => ({ board: 'Gr-03', el, prop: prop as Allowed['prop'], board_value: b, live_value: l, at: BOTH, reason })),
];
