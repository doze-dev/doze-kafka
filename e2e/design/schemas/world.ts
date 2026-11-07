import { OwnBroker } from '../../ownbroker';

// The Schemas boards' world, on a broker of the spec's own: the subjects the boards
// draw, under the boards' names, registered in the order that gives the boards' ids
// (orders v1 is 1, orders-key 2, dispatch v1 3, orders v2 4, points 5, dispatch v2 6,
// orders v3 7, inventory 8 and 10, with 9 taken and deleted: Sc-Home's "id 9 is not
// in use"), and the topics they are named for. Sc-18's subject is registered later,
// by a client (registerByClient), so that the boards before it list five subjects.

export const ORDERS = 'harbour.checkout.orders-value';
export const ORDERS_KEY = 'harbour.checkout.orders-key';
export const DISPATCH = 'harbour.depot.dispatch-value';
export const POINTS = 'harbour.loyalty.points-value';
export const LEVELS = 'harbour.inventory.levels-value';
export const AUDIT = 'harbour.audit.events-value';

const order = (fields: string) => JSON.stringify(JSON.parse(`{"type":"record","name":"Order","namespace":"harbour.checkout","fields":[${fields}]}`));
const ORDER_V1 = '{"name":"orderId","type":"string"},{"name":"lines","type":"int"},{"name":"total","type":"double"}';
const ORDER_V2 = `${ORDER_V1},{"name":"currency","type":"string","default":"EUR"}`;
const ORDER_V3 = `${ORDER_V2},{"name":"coupon","type":["null","string"],"default":null}`;
const DISPATCH_V1 = 'syntax = "proto3";\npackage harbour.depot;\n\nmessage Dispatch {\n  string order_id = 1;\n  string depot = 2;\n  int32 parcels = 3;\n}\n';
const DISPATCH_V2 = 'syntax = "proto3";\npackage harbour.depot;\n\nmessage Dispatch {\n  string order_id = 1;\n  string depot = 2;\n  int32 parcels = 3;\n  string carrier = 4;\n}\n';
const POINTS_V1 = JSON.stringify({
  $schema: 'http://json-schema.org/draft-07/schema#', title: 'PointsEvent', type: 'object',
  properties: { memberId: { type: 'string' }, points: { type: 'integer', minimum: 0 }, reason: { type: 'string' }, orderId: { type: 'string' } },
  required: ['memberId', 'points'], additionalProperties: false,
});
const level = (fields: string) => JSON.stringify(JSON.parse(`{"type":"record","name":"Level","namespace":"harbour.inventory","fields":[${fields}]}`));
const AUDIT_V1 = JSON.stringify({ type: 'record', name: 'AuditEvent', namespace: 'harbour.audit', fields: [{ name: 'actor', type: 'string' }, { name: 'action', type: 'string' }, { name: 'at', type: 'long' }] });

/** Sc-02's draft: a first version of a new subject. */
export const INVOICE = `{
  "type": "record",
  "name": "Invoice",
  "namespace": "harbour.checkout",
  "fields": [
    {"name": "invoiceId", "type": "string"},
    {"name": "orderId", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string", "default": "EUR"}
  ]
}`;
/** Sc-03's draft: dispatch v3, a field with a new number. */
export const DISPATCH_V3 = 'syntax = "proto3";\npackage harbour.depot;\n\nmessage Dispatch {\n  string order_id = 1;\n  string depot = 2;\n  int32 parcels = 3;\n  string carrier = 4;\n  string slot = 5;\n}\n';

// Avro's binary encoding, enough for an Order: zig-zag varints, strings, doubles.
function zigzag(n: number): number[] {
  let v = BigInt.asUintN(64, (BigInt(n) << 1n) ^ (BigInt(n) >> 63n));
  const out: number[] = [];
  do { let b = Number(v & 0x7fn); v >>= 7n; if (v) b |= 0x80; out.push(b); } while (v);
  return out;
}
const avroString = (s: string) => { const b = [...Buffer.from(s, 'utf8')]; return [...zigzag(b.length), ...b]; };
const avroDouble = (d: number) => { const b = Buffer.alloc(8); b.writeDoubleLE(d); return [...b]; };
/** An Order as version v writes it, framed for the registry with id. */
export function orderRecord(v: 1 | 2 | 3, id: number, orderId: string): string {
  const body = [...avroString(orderId), ...zigzag(3), ...avroDouble(84.2)];
  if (v >= 2) body.push(...avroString('EUR'));
  if (v >= 3) body.push(...zigzag(0)); // coupon: the union's null branch
  const frame = Buffer.alloc(5);
  frame.writeUInt32BE(id, 1);
  return Buffer.concat([frame, Buffer.from(body)]).toString('base64');
}

export class SchemasWorld {
  readonly broker: OwnBroker;
  constructor(offset: number) { this.broker = new OwnBroker('schemas-boards', offset); }
  get registry() { return `http://127.0.0.1:${this.broker.port + 2}`; }
  get console() { return this.broker.console; }

  async call(method: string, path: string, body?: unknown, agent = 'doze-kafka-console (the design gate, registering as the console does)'): Promise<{ status: number; json: any }> {
    const r = await fetch(this.registry + path, {
      method, headers: { 'Content-Type': 'application/vnd.schemaregistry.v1+json', 'User-Agent': agent },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await r.text();
    return { status: r.status, json: text ? JSON.parse(text) : null };
  }

  private async register(subject: string, schema: string, type?: string, agent?: string): Promise<number> {
    const r = await this.call('POST', `/subjects/${encodeURIComponent(subject)}/versions`, type ? { schema, schemaType: type } : { schema }, agent);
    if (r.status !== 200) throw new Error(`register ${subject}: ${r.status} ${JSON.stringify(r.json)}`);
    return r.json.id;
  }

  async start(): Promise<void> {
    await this.broker.start();
    for (const t of ['harbour.checkout.orders', 'harbour.checkout.orders.dlq', 'harbour.depot.dispatch', 'harbour.loyalty.points', 'harbour.inventory.levels', 'harbour.audit.events']) {
      await fetch(this.console + 'new/topic', { method: 'POST', body: new URLSearchParams({ name: t, partitions: '3' }) });
    }
    const ids = [
      await this.register(ORDERS, order(ORDER_V1)),
      await this.register(ORDERS_KEY, JSON.stringify({ type: 'record', name: 'OrderKey', namespace: 'harbour.checkout', fields: [{ name: 'orderId', type: 'string' }] })),
      await this.register(DISPATCH, DISPATCH_V1, 'PROTOBUF'),
      await this.register(ORDERS, order(ORDER_V2)),
      await this.register(POINTS, POINTS_V1, 'JSON'),
      await this.register(DISPATCH, DISPATCH_V2, 'PROTOBUF'),
      await this.register(ORDERS, order(ORDER_V3)),
      await this.register(LEVELS, level('{"name":"sku","type":"string"},{"name":"onHand","type":"int"}')),
      await this.register('harbour.scratch-value', level('{"name":"x","type":"string"}')),
      await this.register(LEVELS, level('{"name":"sku","type":"string"},{"name":"onHand","type":"int"},{"name":"reserved","type":"int","default":0}')),
    ];
    // One record on the orders topic, encoded with v3 (id 7) by the console's composer:
    // what Sc-05's "Messages on this id" opens.
    const produced = await fetch(this.console + 'topics/harbour.checkout.orders/produce', { method: 'POST', body: new URLSearchParams({
      key: 'HRB-2026-04811', value: '{"orderId":"HRB-2026-04811","lines":3,"total":84.2,"currency":"EUR","coupon":null}', schema_subject: ORDERS, partition: '1' }) });
    if (!produced.ok) throw new Error(`produce: ${produced.status} ${await produced.text()}`);
    // Sc-11's: messages on each version's id, on the topic and its dead-letter topic.
    for (const [topic, v, id, n] of [['harbour.checkout.orders', 1, 1, 6], ['harbour.checkout.orders', 2, 4, 4], ['harbour.checkout.orders', 3, 7, 7],
      ['harbour.checkout.orders.dlq', 3, 7, 3], ['harbour.checkout.orders.dlq', 2, 4, 1]] as const) {
      for (let i = 0; i < n; i++) await this.produce(topic, `HRB-2026-0${4700 + i}`, orderRecord(v, id, `HRB-2026-0${4700 + i}`));
    }
    if (ids.join(',') !== '1,2,3,4,5,6,7,8,9,10') throw new Error(`the boards' ids are 1 to 10 in order, got ${ids}`);
    // The orders subject has BACKWARD as its own level, as Sc-04 and Sc-09 draw it.
    await this.call('PUT', `/config/${encodeURIComponent(ORDERS)}`, { compatibility: 'BACKWARD' });
    await this.call('DELETE', '/subjects/harbour.scratch-value');
    await this.call('DELETE', '/subjects/harbour.scratch-value?permanent=true');
  }

  async produce(topic: string, key: string, base64: string): Promise<void> {
    const r = await fetch(this.console + `topics/${encodeURIComponent(topic)}/produce`, { method: 'POST', body: new URLSearchParams({ key, value: base64, value_enc: 'base64', partition: '0' }) });
    if (!r.ok) throw new Error(`produce to ${topic}: ${r.status} ${await r.text()}`);
  }

  /** Sc-17: a message on id 9, which the world deleted for good. */
  async produceOrphan(): Promise<void> {
    await this.produce('harbour.checkout.orders', 'HRB-2026-04798', orderRecord(2, 9, 'HRB-2026-04798'));
  }

  /** Sc-18: the audit subject, as a Java client's serializer registers it on its first produce. */
  async registerByClient(): Promise<number> {
    return this.register(AUDIT, AUDIT_V1, undefined, 'Java/21.0.2');
  }

  async stop(): Promise<void> { await this.broker.kill(); }
}
