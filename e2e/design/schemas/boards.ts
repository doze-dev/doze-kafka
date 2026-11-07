import type { Probe } from '../lib/compare';
import { SHELL, TEXT_W, TEXT_H } from '../lib/probes';

// The Schemas boards and what the gate compares on each. What is data (the addresses in
// the producer's lines, the registry's requests) is held to the broker in schemas.spec.ts;
// the probes compare the page's shape, type and colour with the board's.

const ADDR = 'its lines carry this broker\'s and this registry\'s addresses, which are the test broker\'s ports, not the board\'s 9092 and 8081';

/** The registry's wire dock at the foot of a Schemas page. */
export const DOCK: Probe[] = [
  { el: 'dock', board: '.dock', live: '#wire-dock' },
  { el: 'dock head', board: '.dock .dh', live: '#wire-dock .dh' },
  { el: 'dock title', board: '.dock .dh > .b', live: '#wire-dock .dh > .b', text: true },
  { el: 'dock pill', board: '.dock .dh > .pill', live: '#wire-dock .dh > .pill', text: true },
  { el: 'dock note', board: '.dock .dh > .note', live: '#wire-dock .dh > .note', text: true },
  { el: 'dock table head', board: '.dock .r.h', live: '#wire-dock .r.h' },
  { el: 'dock head cell', board: '.dock .r.h > span', live: '#wire-dock .r.h > span', text: true },
  { el: 'dock row', board: '.dock .r:not(.h)', live: '#wire-dock .r[data-reg-row]' },
  { el: 'dock time', board: '.dock .r:not(.h) > :nth-child(1)', live: '#wire-dock .r[data-reg-row] > :nth-child(1)', free: { w: TEXT_W } },
  { el: 'dock method', board: '.dock .r:not(.h) > :nth-child(2)', live: '#wire-dock .r[data-reg-row] > :nth-child(2)' },
  { el: 'dock path', board: '.dock .r:not(.h) > :nth-child(3)', live: '#wire-dock .r[data-reg-row] > :nth-child(3)' },
  { el: 'dock status', board: '.dock .r:not(.h) > :nth-child(4)', live: '#wire-dock .r[data-reg-row] > :nth-child(4)' },
];

/** Sc-01: no subject registered. */
export const NONE: Probe[] = [
  ...SHELL,
  { el: 'list', board: '.list', live: 'aside.list' },
  { el: 'list filter', board: '.list > :first-child', live: '.list > .list-filter' },
  { el: 'list none', board: '.list > .col.g4', live: '.list .list-none' },
  { el: 'list none title', board: '.list > .col.g4 > .b', live: '.list .list-none > .b', text: true },
  { el: 'list none note', board: '.list > .col.g4 > .note', live: '.list .list-none > .note', text: true },
  { el: 'main', board: 'main.main', live: 'main.main' },
  { el: 'id band', board: 'main > .row.sb', live: 'main > .idband' },
  { el: 'h1', board: 'main h1', live: 'main h1', text: true },
  { el: 'h1 note', board: 'main h1 + .note', live: 'main h1 + .note', free: { w: TEXT_W, text: ADDR } },
  { el: 'register button', board: 'main > .row.sb .btn.pri', live: 'main > .idband .btn.pri', text: true },
  { el: 'lead', board: 'main > .callout', live: 'main > .sc-none-lead' },
  { el: 'lead title', board: 'main > .callout > .b', live: 'main > .sc-none-lead > .b', text: true },
  { el: 'lead text', board: 'main > .callout > span:last-child', live: 'main > .sc-none-lead > span:last-child', text: true },
  { el: 'steps row', board: 'main > .row.g12', live: 'main > .sc-none' },
  { el: 'steps card', board: 'main > .row.g12 > .card', live: 'main > .sc-none > .card', free: { h: 'its height is the producer\'s lines, which are this broker\'s (with SASL on, the login lines are added)' } },
  { el: 'steps title', board: 'main > .row.g12 > .card .h2', live: 'main > .sc-none > .card .h2', text: true },
  { el: 'step 1', board: 'main > .row.g12 > .card .row.g8', live: 'main > .sc-none > .card .row.g8', text: true },
  { el: 'step number', board: 'main > .row.g12 > .card .num', live: 'main > .sc-none > .card .num', text: true },
  { el: 'producer lines', board: 'main > .row.g12 > .card pre.code', live: 'main > .sc-none > .card pre.code', free: { text: ADDR, w: ADDR, h: 'its lines are this broker\'s: the board draws the SASL login lines, a broker without SASL has none' } },
  { el: 'step 2', board: 'main > .row.g12 > .card .row.g8', live: 'main > .sc-none > .card .row.g8', nth: 1, free: { y: 'it follows the producer\'s lines', h: TEXT_H, text: 'it names a topic this broker has, or <topic> when it has none; the board names harbour.checkout.orders' } },
  { el: 'step 3', board: 'main > .row.g12 > .card .row.g8', live: 'main > .sc-none > .card .row.g8', nth: 2, text: true, free: { y: 'it follows the producer\'s lines' } },
  { el: 'side', board: 'main > .row.g12 > .col', live: 'main > .sc-none > .sc-none-side' },
  { el: 'side callout', board: 'main > .row.g12 > .col .callout', live: 'main > .sc-none > .sc-none-side .callout' },
  { el: 'side callout 2', board: 'main > .row.g12 > .col .callout', live: 'main > .sc-none > .sc-none-side .callout', nth: 1 },
  { el: 'side title', board: 'main > .row.g12 > .col .callout .b', live: 'main > .sc-none > .sc-none-side .callout .b', text: true },
  { el: 'side note', board: 'main > .row.g12 > .col .callout .note', live: 'main > .sc-none > .sc-none-side .callout .note', text: true },
  { el: 'side button', board: 'main > .row.g12 > .col .callout .btn', live: 'main > .sc-none > .sc-none-side .callout .btn', text: true },
  ...DOCK,
];
