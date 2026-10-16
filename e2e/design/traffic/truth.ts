// The truth checks: what the page says that is data (a count, a client, a time) against
// the generator's ledger, which is what the clients really sent. A check returns the
// problems it found, or "again" when the snapshot fell on an edge the ledger cannot
// settle (a request that began within a few milliseconds of a counted window's edge,
// a connection that opened or closed as the page was read); the spec then reads the page
// again.

import type { El } from '../lib/measure';
import type { Ledger, Req } from '../lib/ledger';

export type Snap = Record<string, El[]>;
export type Verdict = { problems: string[]; again?: string };
export type Truth = {
  /** The selectors the check reads, measured in the same snapshot as the pairs. */
  sels: string[];
  check: (snap: Snap, ledger: Ledger, at: number) => Verdict;
};

// When the broker took a request in: after the client began writing it and before the
// client read the answer (the broker's time is when it read the request, which may wait
// behind the one before it on the connection). A count over a window is then known
// exactly when no request has an edge of the window inside that span, and otherwise
// lies between the requests certainly in and those possibly in: the check says both.
const SKEW = 1;
const inside = (r: Req, from: number, to: number) => r.start >= from && r.upper + SKEW < to;
const maybe = (r: Req, from: number, to: number) => r.upper + SKEW >= from && r.start < to;
function bounds(reqs: Req[], from: number, to: number) {
  let lo = 0, hi = 0;
  for (const r of reqs) {
    if (inside(r, from, to)) lo++;
    if (maybe(r, from, to)) hi++;
  }
  return { lo, hi };
}
const within = (n: number, b: { lo: number; hi: number }) => n >= b.lo && n <= b.hi;
const range = (b: { lo: number; hi: number }) => (b.lo === b.hi ? `${b.lo}` : `${b.lo} to ${b.hi}`);

/** The clients with a connection open at t, and their connections. */
function openClients(ledger: Ledger, t: number) {
  const by = new Map<string, string[]>();
  for (const c of ledger.openAt(t)) {
    if (!by.has(c.client)) by.set(c.client, []);
    by.get(c.client)!.push(c.local);
  }
  return by;
}

function edgy(ledger: Ledger, t: number, ms: number): boolean {
  return ledger.conns.some((c) => Math.abs(c.open - t) < ms || (c.close !== undefined && Math.abs(c.close - t) < ms));
}

const ms = (iso: string | undefined) => (iso ? Date.parse(iso) : NaN);

// The errors the scenario makes, which the ledger knows: loyalty-etl is denied its group
// (each FindCoordinator it sends is GROUP_AUTHORIZATION_FAILED), and checkout-api's one
// Produce to a topic that does not exist (its note says when).
function scriptedErrors(ledger: Ledger): Req[] {
  const out = ledger.reqs.filter((r) => r.client === 'loyalty-etl' && r.api === 'FindCoordinator');
  for (const note of ledger.notes) {
    if (note.what !== 'refused-produce') continue;
    const before = ledger.reqs.filter((r) => r.client === 'checkout-api' && r.api === 'Produce' && r.end <= Number(note.t) + 1);
    if (before.length) out.push(before[before.length - 1]);
  }
  return out;
}

export const TRUTHS: Record<string, Truth> = {
  // "8 clients connected · 1342 requests in the last minute · 12 errors · kept: the last
  // 2,000, in memory only", against the ledger over the minute the page says it counted.
  summary: {
    sels: ['#traffic-summary', '#traffic-summary > span'],
    check(snap, ledger, at) {
      const p: string[] = [];
      const row = snap['#traffic-summary'][0];
      if (!row) return { problems: ['no summary line'] };
      const items = snap['#traffic-summary > span'].map((e) => e.t);
      const from = Number(row.attrs?.['data-window-from']), to = Number(row.attrs?.['data-window-to']);
      if (!(to - from === 60_000)) p.push(`the window is ${from} to ${to}, not a minute`);
      if (edgy(ledger, at, 400)) return { problems: [], again: 'a connection opened or closed as the page was read' };
      const clients = openClients(ledger, at).size;
      const want = (n: number, one: string) => `${n} ${one}${n === 1 ? '' : 's'}`;
      if (items[0] !== `${want(clients, 'client')} connected`) p.push(`"${items[0]}": the ledger has ${clients} clients with a connection open`);
      const reqs = bounds(ledger.reqs, from, to);
      // A client killed with requests in flight wrote none of them down; the broker may
      // have read one on each connection it had open.
      for (const [client, t] of ledger.killed) {
        if (t >= from && t < to) reqs.hi += ledger.conns.filter((c) => c.client === client && c.open <= t && c.close === t).length;
      }
      const said = Number(/^(\d+)/.exec(items[2] ?? '')?.[1]);
      if (items.length > 2 && (!within(said, reqs) || items[2] !== `${want(said, 'request')} in the last minute`)) p.push(`"${items[2]}": the ledger has ${range(reqs)} requests that began in [${new Date(from).toISOString()}, ${new Date(to).toISOString()})`);
      if (items.length > 4 && /error/.test(items[4])) {
        const e = bounds(scriptedErrors(ledger), from, to);
        const n = Number(/^(\d+)/.exec(items[4])?.[1]);
        if (!within(n, e) || items[4] !== want(n, 'error')) p.push(`"${items[4]}": the scenario's errors in that minute are ${range(e)}`);
      }
      return { problems: p };
    },
  },

  // Every request row: its client sent that request at that time, and a folded row's
  // count is how many it sent from the row's first to its last.
  rows: {
    sels: ['#traffic-live .tf-rows > .r[data-row]', '#traffic-live .tf-rows > .r[data-row] > span', '#detail'],
    check(snap, ledger) {
      const p: string[] = [];
      const rows = snap['#traffic-live .tf-rows > .r[data-row]'];
      const cells = snap['#traffic-live .tf-rows > .r[data-row] > span'];
      if (!rows.length) return { problems: ['no rows'] };
      const per = cells.length / rows.length;
      if (per !== 7 && per !== 6) return { problems: [`${cells.length} cells in ${rows.length} rows`] };
      const latest = ledger.reqs.length ? ledger.reqs[ledger.reqs.length - 1].start : 0;
      // Each row's client sent that request at that time.
      type Row = { client: string; api: string; at: number; first: number; count: number; rhythm: boolean };
      const seen: Row[] = [];
      for (let i = 0; i < rows.length; i++) {
        const c = cells.slice(i * per, i * per + per);
        const at = ms(c[0].attrs?.datetime);
        const client = c[1].t.replace(/^└─ /, '');
        const m = /^(\S+) v(\d+)(?: ×(\d+))?$/.exec(c[2].t);
        if (!m) { p.push(`row ${i}: request "${c[2].t}"`); continue; }
        const [, api, , count] = m;
        const first = ms(c[per === 7 ? 4 : 3]?.attrs?.datetime);
        seen.push({ client, api, at, first: Number.isNaN(first) ? at : first, count: Number(count ?? 1), rhythm: !Number.isNaN(first) });
        if (at > latest - 50) continue; // the ledger is written after the answer: too new to judge.
        // A client killed with a request in flight never wrote it down; the broker read it.
        const killed = ledger.killedAt(client);
        if (killed !== undefined && at <= killed && at > killed - 1000) continue;
        const sent = ledger.reqs.filter((r) => r.client === client && r.api === api && at >= r.start - SKEW && at <= r.upper + SKEW);
        if (!sent.length) p.push(`row ${i}: ${client} ${api} at ${c[0].attrs?.datetime}: the ledger has no such request`);
      }
      // The counts. A client's requests of one kind fold one row a connection, and the
      // ledger does not say which connection carried a request; so for a client and a
      // request whose rows all say when they began (one request, or a folded beat that
      // says "since"), the rows' counts add up to the ledger's from the first of them to
      // the last, where the page lists every row of that stretch (from its oldest row on).
      const oldest = Math.min(...seen.map((r) => r.at));
      const groups = new Map<string, Row[]>();
      for (const r of seen) {
        const k = `${r.client}\u0000${r.api}`;
        if (!groups.has(k)) groups.set(k, []);
        groups.get(k)!.push(r);
      }
      const page = snap['#detail'][0]?.attrs ?? {};
      const whole = !('data-traffic-filtered' in page) && !('data-traffic-errors' in page);
      for (const [k, rs] of groups) {
        if (!whole) break; // a filtered list leaves rows out: its counts are not the client's.
        if (rs.some((r) => r.count > 1 && !r.rhythm)) continue; // a folded fetch does not say when it began.
        const from = Math.min(...rs.map((r) => r.first)), to = Math.max(...rs.map((r) => r.at));
        if (from < oldest || to > latest - 50) continue;
        const [client, api] = k.split('\u0000');
        const n = bounds(ledger.reqs.filter((r: Req) => r.client === client && r.api === api), from, to + 1);
        const said = rs.reduce((a, r) => a + r.count, 0);
        if (!within(said, n)) p.push(`${client} ${api}: the rows count ${said} from ${new Date(from).toISOString()} to ${new Date(to).toISOString()}, the ledger ${range(n)}`);
      }
      return { problems: p };
    },
  },

  // The connected clients (Main's panel): how many, and each one's software, address
  // and connections.
  connected: {
    sels: ['#clients-list > .tf-conn', '#clients-list > .tf-conn > *', '#inspector-panel .insp-default > .hd > .h2'],
    check(snap, ledger, at) {
      const p: string[] = [];
      if (edgy(ledger, at, 400)) return { problems: [], again: 'a connection opened or closed as the page was read' };
      const open = openClients(ledger, at);
      const conns = [...open.values()].reduce((n, l) => n + l.length, 0);
      const head = snap['#inspector-panel .insp-default > .hd > .h2'][0]?.t;
      const want = open.size === conns ? `${conns} open connection${conns === 1 ? '' : 's'}` : `${open.size} client${open.size === 1 ? '' : 's'} · ${conns} connections`;
      if (head !== want) p.push(`panel head "${head}", the ledger: "${want}"`);
      const blocks = snap['#clients-list > .tf-conn'];
      const lines = snap['#clients-list > .tf-conn > *'];
      if (blocks.length !== open.size) p.push(`${blocks.length} client blocks, the ledger has ${open.size} clients`);
      for (let i = 0; i < blocks.length; i++) {
        const [first, second, third] = [lines[i * 3], lines[i * 3 + 1], lines[i * 3 + 2]];
        const name = first.t.split(' ')[0];
        const locals = open.get(name);
        if (!locals) { p.push(`block ${i}: ${name} has no open connection in the ledger`); continue; }
        const sw = ledger.software.get(name);
        const parts = second.t.split(' · ');
        if (sw && parts[0] !== `${sw.name} ${sw.version}`) p.push(`block ${i}: ${name} software "${parts[0]}", the ledger "${sw.name} ${sw.version}"`);
        if (!locals.includes(parts[2])) p.push(`block ${i}: ${name} address ${parts[2]} is not one of its open connections ${locals.join(', ')}`);
        const n = locals.length;
        const says = /· (\d+) connections$/.exec(third.t);
        if (n > 1 ? Number(says?.[1]) !== n : !!says) p.push(`block ${i}: ${name} "${third.t}", the ledger has ${n} connections`);
      }
      return { problems: p };
    },
  },

  // The broker's address and when it began, on the empty page (Traf-09).
  empty: {
    sels: ['#traffic-empty pre.code', '#traffic-empty .note .m', '#traffic-empty > .col:first-child > .note', '#traffic-empty .col.g6 > .note'],
    check(snap, ledger) {
      const p: string[] = [];
      const kafka = String(ledger.notes.find((n) => n.what === 'world')?.kafka ?? '');
      const line = snap['#traffic-empty pre.code'][0]?.t;
      if (line !== `bootstrap.servers=${kafka}`) p.push(`"${line}", the broker listens on ${kafka}`);
      const kcat = snap['#traffic-empty .note .m'][0]?.t;
      if (kcat !== `kcat -b ${kafka} -L`) p.push(`"${kcat}"`);
      // The first sentence is the board's, with when the broker began (it is the world's
      // start, to the second) and that a restart empties the page.
      const began = ledger.notes.find((n) => n.what === 'world');
      const first = snap['#traffic-empty > .col:first-child > .note'][0]?.t ?? '';
      const m = /^No app has connected, so there is nothing to show\. This is not an error: the broker is up and listening, since (\d\d):(\d\d):(\d\d)\. A restart empties this page: it keeps its last requests in memory only\.$/.exec(first);
      if (!m) p.push(`"${first}"`);
      else if (began) {
        const at = new Date(Number(began.t)); const said = new Date(at); said.setHours(Number(m[1]), Number(m[2]), Number(m[3]), 0);
        if (!(said.getTime() <= at.getTime() && said.getTime() > at.getTime() - 20_000)) p.push(`since ${m.slice(1).join(':')}: the broker began just before ${at.toTimeString().slice(0, 8)}`);
      }
      if (snap['#traffic-empty .col.g6 > .note'].length) p.push('a SASL sentence on a listener without SASL');
      return { problems: p };
    },
  },

  // The time the page last heard from the broker (Traf-10): before the broker stopped,
  // and no more than one poll and its retry before.
  lastgood: {
    sels: ['[data-lastgood]'],
    check(snap, ledger) {
      const p: string[] = [];
      const stopped = ledger.notes.filter((n) => n.what === 'broker-stopped').map((n) => Number(n.t)).pop();
      const shown = snap['[data-lastgood]'].map((e) => e.t).filter(Boolean);
      if (stopped === undefined) {
        if (shown.length) p.push(`the page says it lost the broker at ${shown.join(', ')}, and the broker runs`);
        return { problems: p };
      }
      const want = new Date(stopped);
      for (const t of shown) {
        const [h, m, s] = t.split(':').map(Number);
        const at = new Date(want); at.setHours(h, m, s, 0);
        if (!(at.getTime() <= stopped && at.getTime() > stopped - 6000)) p.push(`last good ${t}: the broker stopped at ${want.toTimeString().slice(0, 8)}`);
      }
      return { problems: p };
    },
  },

  // A topic filter's pills and "N of M requests match" (Traf-02): the pill is the filter,
  // the count is of what the recorder holds, which the ledger does not know (eviction is
  // by last activity), so N is checked against M and against the rows the page lists.
  filtered: {
    sels: ['#filter-pills > *', '#match-count', '#traffic-live .tf-rows > .r[data-row] > span:nth-child(4)', '#traffic-note'],
    check(snap) {
      const p: string[] = [];
      const held = /of (\d+) requests match$/.exec(snap['#match-count'][0]?.t ?? '')?.[1];
      const note = snap['#traffic-note'][0]?.t;
      if (note !== `Filtering looks at the last 2,000 rows kept in memory, ${held} requests in all. Group requests that name this topic are included.`) p.push(`"${note}"`);
      const items = snap['#filter-pills > *'].map((e) => e.t);
      if (items[1] !== 'topic harbour.checkout.orders ×') p.push(`pill "${items[1]}"`);
      const m = /^(\d+) of (\d+) requests match$/.exec(snap['#match-count'][0]?.t ?? '');
      if (!m || Number(m[1]) > Number(m[2])) p.push(`"${snap['#match-count'][0]?.t}"`);
      for (const c of snap['#traffic-live .tf-rows > .r[data-row] > span:nth-child(4)']) {
        if (!c.t.split(', ').includes('harbour.checkout.orders')) p.push(`a row about "${c.t}" in a filter for harbour.checkout.orders`);
      }
      return { problems: p };
    },
  },

  // Who touched the topic (Traf-02's panel): in this world, checkout-api produced to it
  // and order-validator fetched from it, and nobody else.
  touched: {
    sels: ['#touched > .kv > *', '#inspector-panel .insp-default > .hd > .h2'],
    check(snap, ledger) {
      const p: string[] = [];
      const kv = snap['#touched > .kv > *'].map((e) => e.t);
      const get = (k: string) => kv[kv.indexOf(k) + 1];
      if (snap['#inspector-panel .insp-default > .hd > .h2'][0]?.t !== 'Who touched harbour.checkout.orders') p.push('the heading');
      if (get('Producers') !== 'checkout-api') p.push(`producers "${get('Producers')}"`);
      if (get('Consumers') !== 'order-validator') p.push(`consumers "${get('Consumers')}"`);
      const produced = ledger.reqs.filter((r) => r.client === 'checkout-api' && r.api === 'Produce').length;
      if (!(Number(get('Produce requests')) <= produced)) p.push(`produce requests ${get('Produce requests')}, the ledger has ${produced} in all`);
      if (get('Errors') !== '0') p.push(`errors "${get('Errors')}": nothing about this topic was refused`);
      return { problems: p };
    },
  },

  // The errors view (Traf-03): the refusal seen most in this world is checkout-api's
  // Produce to harbour.checkout.invoices, which is not there (one a round); the refusals
  // listed are what the count beside the field says.
  errors: {
    sels: ['#refused-count', '#traffic-live .tf-rows > .r.errrow', '#error-summary > .kv > *', '#inspector-panel .insp-default > .hd > *', '#error-summary > .callout > .note'],
    check(snap, ledger) {
      const p: string[] = [];
      const listed = snap['#traffic-live .tf-rows > .r.errrow'].length;
      if (snap['#refused-count'][0]?.t !== `${listed} refused`) p.push(`"${snap['#refused-count'][0]?.t}" beside ${listed} error rows`);
      const hd = snap['#inspector-panel .insp-default > .hd > *'].map((e) => e.t);
      if (hd[1] !== 'Produce to a topic that does not exist' || hd[2] !== 'UNKNOWN_TOPIC_OR_PARTITION') p.push(`heading "${hd.join(' / ')}"`);
      const kv = snap['#error-summary > .kv > *'].map((e) => e.t);
      const get = (k: string) => kv[kv.indexOf(k) + 1];
      if (get('Client') !== 'checkout-api') p.push(`client "${get('Client')}"`);
      if (get('Nearest topic') !== 'harbour.checkout.orders') p.push(`nearest topic "${get('Nearest topic')}"`);
      const seen = Number(/^(\d+) times?/.exec(get('Seen') ?? '')?.[1]);
      const sent = ledger.notes.filter((n) => n.client === 'checkout-api' && n.what === 'refused-produce' && Number(n.code) === 3).length;
      if (!(seen > 0 && seen <= sent)) p.push(`seen "${get('Seen')}", the client was refused ${sent} times in all`);
      const said = snap['#error-summary > .callout > .note'][0]?.t;
      if (said !== 'checkout-api produced to harbour.checkout.invoices, which is not on this broker. Real Kafka would refuse the same call.') p.push(`"${said}"`);
      return { problems: p };
    },
  },

  // An open request (Traf-04, Traf-08): who sent it, from where, and its sizes, against
  // the client's own count of what it wrote and read.
  request: {
    sels: ['#inspector > .hd > .m.mut', '#inspector > .hd > .row > *', '#inspector > .bd > .kv > *', '#result-per-partition .card > .r:not(.h) > span'],
    check(snap, ledger) {
      const p: string[] = [];
      const hd = snap['#inspector > .hd > .m.mut'];
      const title = snap['#inspector > .hd > .row > *'].map((e) => e.t);
      const kv = snap['#inspector > .bd > .kv > *'].map((e) => e.t);
      const get = (k: string) => (kv.includes(k) ? kv[kv.indexOf(k) + 1] : undefined);
      const client = get('Client')!, api = title[0].split(' ')[0];
      const at = ms(hd[hd.length - 1]?.attrs?.datetime);
      const sent = ledger.reqs.filter((r) => r.client === client && r.api === api && at >= r.start - SKEW && at <= r.upper + SKEW);
      if (sent.length !== 1) { p.push(`${client} ${api} at ${hd[hd.length - 1]?.attrs?.datetime}: ${sent.length} such requests in the ledger`); return { problems: p }; }
      const r = sent[0];
      const sw = ledger.software.get(client);
      if (sw && get('Software') !== `${sw.name} ${sw.version}`) p.push(`software "${get('Software')}"`);
      if (get('Principal') !== 'User:ANONYMOUS') p.push(`principal "${get('Principal')}"`);
      const locals = ledger.conns.filter((c) => c.client === client).map((c) => c.local);
      if (!locals.includes(get('Remote') ?? '')) p.push(`remote "${get('Remote')}" is none of ${client}'s connections`);
      const size = get('Size in / out');
      const b = (n: number) => (n < 1024 ? `${n} B` : n < 1 << 20 ? `${(n / 1024).toFixed(1)} KiB` : `${(n / (1 << 20)).toFixed(1)} MiB`);
      if (size !== undefined && size !== `${b(r.wr)} / ${b(r.rd)}`) p.push(`size "${size}": the client wrote ${r.wr} bytes and read ${r.rd}`);
      const took = Number(/(\d+) ms$/.exec(title[1] ?? '')?.[1]);
      if (!(took <= Math.round(r.end - r.start) + 1)) p.push(`"${title[1]}": the client measured ${(r.end - r.start).toFixed(1)} ms end to end`);
      if (api === 'Produce') {
        if (get('Transactional id') !== 'checkout-1') p.push(`transactional id "${get('Transactional id')}"`);
        if (get('Acks') !== '-1 (all)') p.push(`acks "${get('Acks')}"`);
        const cells = snap['#result-per-partition .card > .r:not(.h) > span'].map((e) => e.t);
        const produced = ledger.notes.filter((n) => n.client === client && n.what === 'produced' && Number(n.t) >= r.end - 1 && Number(n.t) <= r.end + 200)[0];
        if (!produced) p.push('the ledger has no record of what the produce wrote');
        else if (cells[1] !== String(produced.p) || cells[2] !== Number(produced.off).toLocaleString('en-US') || cells[3] !== 'ok') p.push(`partition "${cells.join(' ')}", the client was told p${produced.p} offset ${produced.off}`);
      }
      return { problems: p };
    },
  },

  // A folded heartbeat (Traf-05): its count is the ledger's from its first to its last,
  // every second, and its generation the one order-validator was in all along.
  folded: {
    sels: ['#inspector > .hd > .row > *', '#inspector > .hd > *', '#inspector > .bd > .kv > *', '#inspector > .bd > .row > .btn', '#fold-sentence'],
    check(snap, ledger) {
      const p: string[] = [];
      const title = snap['#inspector > .hd > .row > *'].map((e) => e.t);
      const count = Number(/×(\d+)/.exec(title[0])?.[1]);
      const kv = snap['#inspector > .bd > .kv > *'];
      const get = (k: string) => kv[kv.findIndex((e) => e.t === k) + 1];
      const first = ms(get('First')?.attrs?.datetime), last = ms(get('Last')?.attrs?.datetime);
      const n = bounds(ledger.reqs.filter((r) => r.client === 'order-validator' && r.api === 'Heartbeat'), first, last + 1);
      if (!within(count, n)) p.push(`×${count} from ${get('First')?.attrs?.datetime} to ${get('Last')?.attrs?.datetime}: the ledger has ${range(n)}`);
      if (get('Interval')?.t !== 'every 1 s') p.push(`interval "${get('Interval')?.t}": the client beats every second`);
      if (get('Generation')?.t !== 'generation 1 throughout') p.push(`generation "${get('Generation')?.t}": order-validator is the group's one member, generation 1`);
      const sub = snap['#inspector > .hd > *'].map((e) => e.t)[2];
      if (sub !== 'order-validator · g-order-validator') p.push(`"${sub}"`);
      if (snap['#fold-sentence'][0]?.t !== `${count} identical heartbeats are shown as one row. Each one asked the same thing and got the same answer.`) p.push('the fold sentence');
      const unfold = snap['#inspector > .bd > .row > .btn'][0]?.t;
      if (unfold !== (count > 64 ? 'Unfold the last 64' : `Unfold these ${count}`)) p.push(`"${unfold}"`);
      return { problems: p };
    },
  },

  // The clients view (Traf-06, 07, 11): each row a client with a connection open, its
  // software, address and requests a minute against the ledger.
  clients: {
    sels: ['#clients-table .tf-rows > .r', '#clients-table .tf-rows > .r > span', '#detail > .tabs > .tab', '#traffic-summary', '#attention > .b'],
    check(snap, ledger, at) {
      const p: string[] = [];
      if (edgy(ledger, at, 400)) return { problems: [], again: 'a connection opened or closed as the page was read' };
      const open = openClients(ledger, at);
      const tab = snap['#detail > .tabs > .tab'].map((e) => e.t)[1];
      if (tab !== `Clients ${open.size}`) p.push(`tab "${tab}", the ledger has ${open.size} clients`);
      const rows = snap['#clients-table .tf-rows > .r'];
      const cells = snap['#clients-table .tf-rows > .r > span'];
      const per = rows.length ? cells.length / rows.length : 0;
      if (rows.length !== open.size) p.push(`${rows.length} rows, ${open.size} clients in the ledger`);
      const win = snap['#traffic-summary'][0]?.attrs;
      for (let i = 0; i < rows.length; i++) {
        const c = cells.slice(i * per, i * per + per).map((e) => e.t);
        const name = c[0];
        if (!open.has(name)) { p.push(`row ${i}: ${name} has no open connection`); continue; }
        const sw = ledger.software.get(name);
        if (sw && c[1] !== `${sw.name} ${sw.version}`) p.push(`row ${i}: ${name} software "${c[1]}"`);
        if (per === 9) {
          const addr = c[3].split(' ')[0];
          if (!open.get(name)!.includes(addr)) p.push(`row ${i}: ${name} address ${addr}`);
          if (win) {
            const b = bounds(ledger.reqs.filter((r) => r.client === name), Number(win['data-window-from']), Number(win['data-window-to']));
            if (!within(Number(c[6]), b)) p.push(`row ${i}: ${name} ${c[6]} a minute, the ledger ${range(b)}`);
          }
        }
      }
      // loyalty-etl is denied its group's coordinator and asks Metadata in between: it is
      // raised by the five-minute rule, not by a streak.
      const look = snap['#attention > .b'][0]?.t ?? '';
      const lm = /^loyalty-etl had (\d+) errors among its (\d+) requests in the last five minutes$/.exec(look);
      if (!look) return { problems: p }; // a client is open: the board draws no callout (Traf-07, 11).
      if (!lm) p.push(`callout "${look}"`);
      else if (win) {
        const to = Number(win['data-window-to']), from = to - 300_000;
        const errs = bounds(ledger.reqs.filter((r) => r.client === 'loyalty-etl' && r.api === 'FindCoordinator'), from, to);
        const all = bounds(ledger.reqs.filter((r) => r.client === 'loyalty-etl'), from, to);
        if (!within(Number(lm[1]), errs) || !within(Number(lm[2]), all)) p.push(`callout "${look}": the ledger has ${range(errs)} denied of ${range(all)}`);
      }
      return { problems: p };
    },
  },

  // An open client (Traf-07, Traf-11): its software and address, and the metrics it
  // reported, against what it pushed.
  client: {
    sels: ['#inspector > .hd > *', '#inspector > .hd > .row > *', '#inspector .tf-metrics > *', '#keys > span', '#inspector > .bd > .col:first-child > .note'],
    check(snap, ledger, at) {
      const p: string[] = [];
      const title = snap['#inspector > .hd > .row > *'].map((e) => e.t);
      const name = title[0];
      const sw = ledger.software.get(name);
      const sub = snap['#inspector > .hd > *'].map((e) => e.t)[2] ?? '';
      if (sw && !sub.startsWith(`${sw.name} ${sw.version} · User:ANONYMOUS · `)) p.push(`"${sub}"`);
      const cells = snap['#inspector .tf-metrics > *'].map((e) => e.t);
      const m = /^mall (\d+) metrics$/.exec(snap['#keys > span'][2]?.t ?? '');
      const metrics = /, (\d+) metrics?, last report/.exec(snap['#inspector > .bd > .col:first-child > .note'][0]?.t ?? '')?.[1];
      if (snap['#keys > span'].length > 2 && name !== 'quiet-app' && (!m || m[1] !== metrics)) p.push(`keys "${snap['#keys > span'][2]?.t}", the client reports ${metrics} metrics`);
      // The value is a report's: the newest the broker had when the page was drawn, which
      // is one of the client's last reports before the page was read.
      const pushes = ledger.pushes.filter((x) => x.client === name && x.t <= at).slice(-3);
      if (name === 'quiet-app' && pushes.length) p.push('quiet-app pushed metrics');
      if (pushes.length) {
        for (let i = 0; i + 2 < cells.length; i += 3) {
          const label = cells[i], value = cells[i + 2];
          const values = pushes.map((x) => x.metrics.find((m) => labelOf(m.name) === label)).filter(Boolean).map((m) => valueOf(m!.value, m!.unit));
          if (!values.length) { p.push(`metric "${label}" was not pushed`); continue; }
          if (!values.includes(value)) p.push(`${label} "${value}", the client's last reports: ${values.join(', ')}`);
        }
      }
      return { problems: p };
    },
  },

  // Paused (Traf-08): how many requests came after the newest row shown.
  paused: {
    sels: ['#paused-banner [data-new-n]', '#paused-banner [data-paused-since]', '#traffic-live'],
    check(snap, ledger, at) {
      const p: string[] = [];
      const since = Number(snap['#traffic-live'][0]?.attrs?.['data-since']);
      const said = Number(/^(?:at least )?(\d+)$/.exec(snap['#paused-banner [data-new-n]'][0]?.t ?? '')?.[1]);
      // The banner is asked every 2.5 s: what it says is the count at its last answer.
      const b = bounds(ledger.reqs, since + 0.001, at);
      const lo = bounds(ledger.reqs, since + 0.001, at - 3000).lo;
      if (!(said >= lo && said <= b.hi)) p.push(`"${said} new requests": the ledger has ${lo} to ${b.hi} since ${new Date(since).toISOString()}`);
      return { problems: p };
    },
  },

  // A group's story (Traf-12): g-depot-router's generations as the scenario made them:
  // depot-router alone (1), depot-router-2 joins (2), depot-router-3 joins (3), and
  // depot-router-3, killed, expires (4).
  story: {
    sels: ['#story-count', '#group-summary .card > .r:not(.h) > span', '#traffic-story .tf-rebalance > span', '#group-summary > .callout > *'],
    check(snap) {
      const p: string[] = [];
      const gens = snap['#group-summary .card > .r:not(.h) > span'].map((e) => e.t);
      const rows: string[][] = [];
      for (let i = 0; i < gens.length; i += 4) rows.push(gens.slice(i, i + 4));
      const want = [['1', '1'], ['2', '2'], ['3', '3'], ['4', '2']];
      if (rows.length !== 4 || rows.some((r, i) => r[0] !== want[i][0] || r[2] !== want[i][1])) p.push(`generations ${rows.map((r) => `${r[0]}:${r[2]}`).join(' ')}, the scenario made 1:1 2:2 3:3 4:2`);
      const count = snap['#story-count'][0]?.t ?? '';
      // The group forming is a rebalance too (an empty group's first join prepares one).
      if (!/^\d+ requests( \(\d+ folded\))?, 4 rebalances$/.test(count)) p.push(`count "${count}": the scenario made 4 rebalances (the group forming, two joins, an expiry)`);
      const why = snap['#group-summary > .callout > *'].map((e) => e.t);
      if (!/expired/.test(why[1] ?? '')) p.push(`why "${why[1]}": the newest rebalance is depot-router-3's session expiring`);
      if (why.some((w) => /any join/.test(w))) p.push('the note about joins under a rebalance an expiry began');
      return { problems: p };
    },
  },
};

// What a pushed metric is called on the page, by its whole name (A3): the client's own
// name in words, for the ones the boards name.
function labelOf(name: string): string {
  const short = name.replace(/^org\.apache\.kafka\.(producer|consumer|client)\./, '');
  const known: Record<string, string> = {
    'request.error.rate': 'Request error rate',
    'request.retry.rate': 'Request retries',
    'request.latency.avg': 'Request latency, average',
    'poll.idle.ratio.avg': 'Poll idle ratio',
    'connection.count': 'Connection count',
    'fetch.manager.bytes.consumed.rate': 'Bytes fetched',
  };
  return known[short] ?? short;
}

// A metric's value as the boards write it: "1.4 ms", "0.94", "1", "1.2 KiB/s".
function valueOf(v: number, unit: string): string {
  const num = (x: number) => {
    const a = Math.abs(x);
    let s = a >= 100 ? x.toFixed(0) : a >= 1 ? x.toFixed(2) : x === 0 ? '0' : x.toPrecision(3);
    if (s.includes('.')) s = s.replace(/0+$/, '').replace(/\.$/, '');
    return s;
  };
  if (unit === 'By/s') {
    const n = Math.round(v);
    return (n < 1024 ? `${n} B` : n < 1 << 20 ? `${(n / 1024).toFixed(1)} KiB` : `${(n / (1 << 20)).toFixed(1)} MiB`) + '/s';
  }
  if (unit === '1/s') return `${num(v)} /s`;
  return unit && unit !== '1' ? `${num(v)} ${unit}` : num(v);
}
