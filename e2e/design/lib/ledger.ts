// The generator's ledger (e2e/design/gen): what each client sent, read back. The
// gate's counts, names and times come from here, never from the page.

import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

export type Req = {
  client: string; api: string; key: number;
  /** When the client began writing it, and read its answer. */
  start: number; end: number;
  /**
   * The latest the broker can have read it: its end, or for a request the client gave up
   * on (err: the connection closed under it) a second on, since the broker may still have
   * read what was written.
   */
  upper: number;
  err?: string; wr: number; rd: number;
  /**
   * Written to the broker and not answered when the client last synced (read(cut)): the
   * broker may have read it and the page shown it, and the client has not yet said when
   * it ended (end and upper are then unbounded).
   */
  flying?: boolean;
};
export type Conn = { client: string; local: string; open: number; close?: number };
export type Push = { client: string; t: number; metrics: { name: string; unit: string; value: number }[] };

export class Ledger {
  reqs: Req[] = [];
  conns: Conn[] = [];
  pushes: Push[] = [];
  software = new Map<string, { name: string; version: string }>();
  notes: { client: string; t: number; [k: string]: unknown }[] = [];
  /** When a client was killed (SIGKILL), by its id. */
  killed = new Map<string, number>();

  killedAt(client: string): number | undefined {
    return this.killed.get(client);
  }

  constructor(readonly dir: string) {
    this.read();
  }

  /**
   * The newest sync line of each client (they write one on SIGUSR1; see gen): everything
   * that ended before it is above it, and what had not ended is listed with it.
   */
  syncs = new Map<string, number>();
  /** Set by read(cut): the ledger is a consistent cut taken at each client's sync. */
  cut?: number;

  /**
   * Reads every client's file again (they grow while the clients run). With cut, a client's
   * file is read up to its first sync line written at or after cut, and the requests that
   * line lists as flying are added: the ledger is then complete as of that moment, the
   * same for every client, and the page can be held to it exactly. A client with no such
   * line (one that is gone) is read to its end.
   */
  read(cut?: number): this {
    this.cut = cut;
    this.syncs.clear();
    this.reqs = []; this.conns = []; this.pushes = []; this.notes = []; this.software.clear(); this.killed.clear();
    if (!existsSync(this.dir)) return this;
    for (const f of readdirSync(this.dir).filter((f) => f.endsWith('.jsonl'))) {
      const client = f.replace(/\.jsonl$/, '');
      const open = new Map<string, Conn>();
      lines: for (const line of readFileSync(join(this.dir, f), 'utf8').split('\n')) {
        if (!line.trim()) continue;
        let m: any;
        try { m = JSON.parse(line); } catch { continue; } // a line being written.
        switch (m.ev) {
          case 'sync':
            this.syncs.set(client, m.t);
            if (cut !== undefined && m.t >= cut) {
              for (const o of m.open ?? []) this.reqs.push({ client, api: o.api, key: o.key, start: o.start, end: Infinity, upper: Infinity, wr: 0, rd: 0, flying: true });
              break lines;
            }
            break;
          case 'req': this.reqs.push({ client, api: m.api, key: m.key, start: m.start, end: m.end, upper: m.err ? m.start + 1000 : m.end, err: m.err, wr: m.wr, rd: m.rd }); break;
          case 'connect': { const c = { client, local: m.local, open: m.t }; open.set(m.local, c); this.conns.push(c); break; }
          case 'disconnect': { const c = open.get(m.local); if (c) c.close = m.t; break; }
          case 'software': this.software.set(client, { name: m.name, version: m.version }); break;
          case 'push': this.pushes.push({ client, t: m.t, metrics: m.metrics }); break;
          // The process was killed (SIGKILL): it wrote nothing more, and the system closed
          // its connections as it died.
          case 'killed': for (const c of open.values()) if (c.close === undefined) c.close = m.t; this.killed.set(client, m.t); break;
          default: this.notes.push({ ...m, client });
        }
      }
    }
    this.reqs.sort((a, b) => a.start - b.start);
    return this;
  }

  /** The requests that began in [from, to), optionally of one client or API. */
  between(from: number, to: number, f: { client?: string; api?: string } = {}): Req[] {
    return this.reqs.filter((r) => r.start >= from && r.start < to && (!f.client || r.client === f.client) && (!f.api || r.api === f.api));
  }

  /** The connections open at t. */
  openAt(t: number): Conn[] {
    return this.conns.filter((c) => c.open <= t && (c.close === undefined || c.close > t));
  }

  /** The requests whose start lies within ms of t: what makes a count at a boundary ambiguous. */
  near(t: number, ms: number): Req[] {
    return this.reqs.filter((r) => Math.abs(r.start - t) < ms);
  }
}
