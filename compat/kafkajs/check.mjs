import { Kafka, logLevel } from 'kafkajs';
import fs from 'node:fs';
const secure = process.env.SECURITY === 'scram-tls';
const kafka = new Kafka({
  clientId: 'compat-kafkajs', brokers: [process.env.BOOTSTRAP], logLevel: logLevel.NOTHING,
  ...(secure ? { ssl: { ca: [fs.readFileSync(process.env.CA_CERT)] }, sasl: { mechanism: 'scram-sha-512', username: process.env.SASL_USER, password: process.env.SASL_PASS } } : {}),
});
const ok = (m) => console.log('ok  ', m);
const fail = (m) => { console.log('FAIL', m); process.exit(1); };
const tag = Math.random().toString(16).slice(2, 8);
const topic = `js-${tag}`;

const admin = kafka.admin();
await admin.connect();
if (!(await admin.createTopics({ topics: [{ topic, numPartitions: 3 }], waitForLeaders: true }))) fail('createTopics said it created nothing');
const meta = await admin.fetchTopicMetadata({ topics: [topic] });
if (meta.topics[0].partitions.length !== 3) fail('metadata disagrees about partitions');
ok('admin: create and describe');

const producer = kafka.producer({ idempotent: true });
await producer.connect();
await producer.send({ topic, messages: Array.from({ length: 15 }, (_, i) => ({ key: `k${i}`, value: `v${i}`, headers: { trace: 't1' } })) });
ok('idempotent produce of 15 records');

const consumer = kafka.consumer({ groupId: `g-${tag}` });
await consumer.connect();
await consumer.subscribe({ topic, fromBeginning: true });
const got = [];
await new Promise((resolve, reject) => {
  const t = setTimeout(() => reject(new Error(`consumed ${got.length} of 15`)), 20000);
  consumer.run({ eachMessage: async ({ message }) => { got.push(message); if (got.length === 15) { clearTimeout(t); resolve(); } } });
}).catch((e) => fail(e.message));
if (got[0].headers.trace.toString() !== 't1') fail('headers did not round trip');
ok('group consume 15 records, headers intact');
await consumer.disconnect();
const offsets = await admin.fetchOffsets({ groupId: `g-${tag}`, topics: [topic] });
if (offsets[0].partitions.reduce((n, p) => n + Number(p.offset), 0) !== 15) fail('the group did not commit its position');
ok('the group committed everything it read');

await producer.disconnect();
await admin.deleteTopics({ topics: [topic] });
ok('admin: delete topic');
await admin.disconnect();
