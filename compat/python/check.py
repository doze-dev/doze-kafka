import os, sys, time, uuid
from confluent_kafka import Producer, Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions

bootstrap = os.environ["BOOTSTRAP"]
conf = {"bootstrap.servers": bootstrap}
if os.environ["SECURITY"] == "scram-tls":
    conf.update({"security.protocol": "SASL_SSL", "sasl.mechanism": os.environ["SASL_MECHANISM"],
                 "sasl.username": os.environ["SASL_USER"], "sasl.password": os.environ["SASL_PASS"],
                 "ssl.ca.location": os.environ["CA_CERT"]})

def ok(msg): print("ok  ", msg, flush=True)
def fail(msg): print("FAIL", msg, flush=True); sys.exit(1)

tag = uuid.uuid4().hex[:6]
topic = f"py-{tag}"
admin = AdminClient(conf)

# admin: create with config, describe, grow, list
for t, f in admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1, config={"retention.ms": "3600000"})]).items():
    f.result(15)
md = admin.list_topics(topic, timeout=10)
if len(md.topics[topic].partitions) != 2: fail("created 2 partitions, metadata disagrees")
for t, f in admin.create_partitions([NewPartitions(topic, 4)]).items():
    f.result(15)
time.sleep(0.5)
if len(admin.list_topics(topic, timeout=10).topics[topic].partitions) != 4: fail("CreatePartitions did not grow the topic")
ok("admin: create with config, list, grow")

# produce with key and headers
p = Producer({**conf, "enable.idempotence": True, "linger.ms": 5})
errs = []
for i in range(20):
    p.produce(topic, key=f"k{i}", value=f"v{i}".encode(), headers=[("trace", b"t1")], partition=i % 4,
              on_delivery=lambda e, m: errs.append(e) if e else None)
p.flush(15)
if errs: fail(f"delivery errors: {errs}")
ok("idempotent produce of 20 records")

# consume with a group, commit, resume
def consume(group, want, **extra):
    c = Consumer({**conf, "group.id": group, "auto.offset.reset": "earliest", "enable.auto.commit": False, **extra})
    c.subscribe([topic])
    got, end = [], time.time() + 20
    while len(got) < want and time.time() < end:
        m = c.poll(1)
        if m is None: continue
        if m.error(): fail(f"consumer error {m.error()}")
        got.append(m)
    return c, got
g = f"g-{tag}"
c, got = consume(g, 20)
if len(got) != 20: fail(f"consumed {len(got)} of 20")
if dict(got[0].headers() or []).get("trace") != b"t1": fail("headers did not round trip")
c.commit(asynchronous=False); c.close()
ok("group consume 20 records, headers intact, committed")
c2 = Consumer({**conf, "group.id": g, "auto.offset.reset": "earliest"})
c2.subscribe([topic]); time.sleep(3)
m = c2.poll(3)
if m is not None and m.error() is None: fail("a resumed group re-read a committed record")
c2.close()
ok("a group resumes after its commit")

# the new consumer protocol, when the client has it
try:
    c3, got3 = consume(f"gn-{tag}", 20, **{"group.protocol": "consumer"})
    if len(got3) != 20: fail(f"group.protocol=consumer consumed {len(got3)} of 20")
    c3.close(); ok("group.protocol=consumer (KIP-848) consumes 20 records")
except KafkaException as e:
    fail(f"group.protocol=consumer: {e}")

# a transaction: commit one, abort one; a read_committed consumer sees only the first
tx = Producer({**conf, "transactional.id": f"tx-{tag}"})
tx.init_transactions(15)
tx.begin_transaction(); tx.produce(topic, value=b"committed", partition=0); tx.commit_transaction(15)
# flushed first: an abort discards what has not been sent yet, and the point is a record that WAS written
tx.begin_transaction(); tx.produce(topic, value=b"aborted", partition=0); tx.flush(15); tx.abort_transaction(15)
rc = Consumer({**conf, "group.id": f"rc-{tag}", "auto.offset.reset": "earliest", "isolation.level": "read_committed"})
rc.subscribe([topic]); seen, end = [], time.time() + 15
while time.time() < end:
    m = rc.poll(1)
    if m and not m.error(): seen.append(m.value())
    if b"committed" in seen and len(seen) >= 21: break
rc.close()
if b"committed" not in seen or b"aborted" in seen: fail(f"read_committed saw {seen[-3:]}")
ru = Consumer({**conf, "group.id": f"ru-{tag}", "auto.offset.reset": "earliest", "isolation.level": "read_uncommitted"})
ru.subscribe([topic]); seen_u, end = [], time.time() + 15
while time.time() < end and b"aborted" not in seen_u:
    m = ru.poll(1)
    if m and not m.error(): seen_u.append(m.value())
ru.close()
if b"aborted" not in seen_u: fail("read_uncommitted did not see the aborted record that was written")
ok("transaction: committed record visible, aborted record not (and visible to read_uncommitted)")

# delete
for t, f in admin.delete_topics([topic]).items(): f.result(15)
ok("admin: delete topic")
print("all good")
