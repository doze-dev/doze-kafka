import type { Allowed } from '../lib/compare';

// What every board's top bar and rail is allowed to differ in, for every board that
// draws them at 1440. Each is a deviation recorded in plan/00-foundation.md.

/** The theme switch Sy-07 puts in the top bar; the boards drawn before it have none. */
export const THEME_SWITCH = 'Sy-07 draws the theme switch in the top bar ("a switch in the top bar overrides it"); the other boards were drawn before it and have none';

/** The TR chip: the board\'s white on #c85f0a is 4.12:1 (TP\'s on #2f6bff 4.499:1); the console holds text to 4.5:1 and draws those two black. */
export const CHIP_INK = 'TR\'s white letters on #c85f0a are 4.12:1 and TP\'s on #2f6bff 4.499:1; text is held to 4.5:1 (A11), so these two chips are black (5.10 and 4.67:1); the fill is the board\'s';

export function shellAllow(board: string, places: string[] = ['1440-light', '1440-dark']): Allowed[] {
  return [
    { board, el: 'theme switch', prop: 'presence', board_value: 'absent', live_value: 'present', at: places, reason: THEME_SWITCH },
    { board, el: 'rail chip', prop: 'color', board_value: 'rgb(255, 255, 255)', live_value: 'rgb(0, 0, 0)', at: places, reason: CHIP_INK },
  ];
}
