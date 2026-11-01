import type { Allowed } from '../lib/compare';
import { shellAllow } from '../shell/common-allow';

const BOTH = ['1440-light', '1440-dark'];
const TOPIC_COUNT = 'the count is the seed\'s 14 topics; the board\'s sample has 7';
const AD1_FILTER = 'AD1: a filter field at the top of the list pane, which the board does not draw; the list starts 45 px lower';
const KEYS = 'the keys strip is the console\'s one strip for every page (Sy-06 names j k, ↵, / and c); the board draws the keys of its own page (Tab, ⌘↵, Esc), which the form takes from the browser and the shell';

/** The list pane and keys strip every topic board draws differently from the live pages. */
const chrome = (board: string): Allowed[] => [
  ...shellAllow(board),
  { board, el: 'keys strip', prop: 'text', board_value: 'Tabnext field ⌘↵create Esccancel ⌘Kjump or act', live_value: 'jkmove ↵open /filter ccreate topic ⌘Kjump or act', at: BOTH, reason: KEYS },
  { board, el: 'key', prop: 'w', board_value: '31.8', live_value: '18.6', at: BOTH, reason: KEYS },
  { board, el: 'key', prop: 'text', board_value: 'Tab', live_value: 'j', at: BOTH, reason: KEYS },
  { board, el: 'list cap', prop: 'w', board_value: '129.1', live_value: '136', at: BOTH, reason: TOPIC_COUNT },
  { board, el: 'list cap', prop: 'text', board_value: 'TOPICS · 7 · MESSAGES', live_value: 'TOPICS · 14 · MESSAGES', at: BOTH, reason: TOPIC_COUNT },
  { board, el: 'list item', prop: 'y', board_value: '89', live_value: '134', at: BOTH, reason: AD1_FILTER },
  { board, el: 'list name', prop: 'y', board_value: '93.5', live_value: '138.5', at: BOTH, reason: AD1_FILTER },
  { board, el: 'list number', prop: 'y', board_value: '93.5', live_value: '138.5', at: BOTH, reason: AD1_FILTER },
];

const NEW_TAG = 'the board marks the field with the design\'s "new" tag, an annotation of what is new to the design (Grounding), not part of the label';
const newTag = (k: string): Allowed => ({ board: 'Tp-03', el: `cfg label ${k}`, prop: 'text', board_value: `${k} new`, live_value: k, at: BOTH, reason: NEW_TAG });

const KEYS_NONE = 'the keys strip is the console\'s one strip for every page (Sy-06); the board draws the keys of its own page (c, ⌘K, g c)';
const RAIL_TRAFFIC = 'the rail shows Traffic\'s count ("0 open") as it does on every page where the recorder is on; the board\'s rail draws a count on the data sections only';
const MESSAGES_CAP = 'the list\'s caption says what its numbers count ("· messages"), as on every topic board but this one';
const none: Allowed[] = [
  ...shellAllow('Tp-01'),
  { board: 'Tp-01', el: 'keys strip', prop: 'text', board_value: 'ccreate topic ⌘Kjump or act gcconnect an app', live_value: 'jkmove ↵open /filter ccreate topic ⌘Kjump or act', at: BOTH, reason: KEYS_NONE },
  { board: 'Tp-01', el: 'key', prop: 'text', board_value: 'c', live_value: 'j', at: BOTH, reason: KEYS_NONE },
  { board: 'Tp-01', el: 'rail count', prop: 'y', board_value: '172.5', live_value: '84.5', at: BOTH, reason: RAIL_TRAFFIC },
  { board: 'Tp-01', el: 'rail count', prop: 'margin', board_value: '0px 0px 0px 81.8281px', live_value: '0px 0px 0px 50.75px', at: BOTH, reason: RAIL_TRAFFIC },
  { board: 'Tp-01', el: 'list cap', prop: 'text', board_value: 'TOPICS · 0', live_value: 'TOPICS · 0 · MESSAGES', at: BOTH, reason: MESSAGES_CAP },
];

export const ALLOW: Allowed[] = [...chrome('Tp-02'), ...chrome('Tp-03'), ...chrome('Tp-04'), ...none, ...['max.message.bytes', 'cleanup.policy', 'message.timestamp.type'].map(newTag)];
