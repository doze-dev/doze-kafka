"""confluent-kafka's Avro, JSON Schema and Protobuf serializers against the built-in registry."""
import os, sys, time, uuid, json
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer, JSONDeserializer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer, ProtobufDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

conf = {"bootstrap.servers": os.environ["BOOTSTRAP"]}
reg = {"url": os.environ["REGISTRY_URL"]}
if os.environ["SECURITY"] == "scram-tls":
    conf.update({"security.protocol": "SASL_SSL", "sasl.mechanism": os.environ["SASL_MECHANISM"],
                 "sasl.username": os.environ["SASL_USER"], "sasl.password": os.environ["SASL_PASS"],
                 "ssl.ca.location": os.environ["CA_CERT"]})
    reg["basic.auth.user.info"] = f'{os.environ["REGISTRY_USER"]}:{os.environ["REGISTRY_PASS"]}'

def ok(m): print("ok  ", m, flush=True)
def fail(m): print("FAIL", m, flush=True); sys.exit(1)

tag = uuid.uuid4().hex[:6]
sr = SchemaRegistryClient(reg)
admin = AdminClient(conf)

def topic(kind):
    t = f"py-{kind}-{tag}"
    for f in admin.create_topics([NewTopic(t, 1, 1)]).values(): f.result(15)
    return t

def roundtrip(t, ser, de, value, check):
    p = Producer(conf)
    p.produce(t, key=b"k", value=ser(value, SerializationContext(t, MessageField.VALUE)))
    p.flush(15)
    c = Consumer({**conf, "group.id": f"g-{uuid.uuid4().hex[:6]}", "auto.offset.reset": "earliest"}); c.subscribe([t])
    end = time.time() + 20
    while time.time() < end:
        m = c.poll(1)
        if m and not m.error():
            got = de(m.value(), SerializationContext(t, MessageField.VALUE)); c.close()
            if not check(got): fail(f"{t}: read back {got!r}")
            return
    fail(f"{t}: nothing consumed")

# Avro
avro = '{"type":"record","name":"Order","fields":[{"name":"id","type":"long"},{"name":"sku","type":"string"}]}'
t = topic("avro")
roundtrip(t, AvroSerializer(sr, avro), AvroDeserializer(sr), {"id": 5, "sku": "abc"}, lambda v: v == {"id": 5, "sku": "abc"})
ok("Avro: AvroSerializer and AvroDeserializer round trip")
# An incompatible evolution is refused, a compatible one is not.
compatible = '{"type":"record","name":"Order","fields":[{"name":"id","type":"long"},{"name":"sku","type":"string"},{"name":"note","type":["null","string"],"default":null}]}'
if not sr.test_compatibility(f"{t}-value", Schema(compatible, "AVRO")): fail("a compatible Avro evolution was refused")
incompatible = '{"type":"record","name":"Order","fields":[{"name":"id","type":"long"},{"name":"sku","type":"string"},{"name":"must","type":"string"}]}'
if sr.test_compatibility(f"{t}-value", Schema(incompatible, "AVRO")): fail("an incompatible Avro evolution was accepted")
try:
    sr.register_schema(f"{t}-value", Schema(incompatible, "AVRO")); fail("registering an incompatible schema succeeded")
except SchemaRegistryError as e:
    if e.http_status_code != 409: fail(f"incompatible registration failed with {e.http_status_code}, want 409")
ok("Avro: compatibility check agrees; an incompatible registration is a 409")

# JSON Schema
js = '{"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}},"required":["id"]}'
t = topic("json")
roundtrip(t, JSONSerializer(js, sr), JSONDeserializer(js, schema_registry_client=sr), {"id": 7, "email": "a@b.c"}, lambda v: v == {"id": 7, "email": "a@b.c"})
ok("JSON Schema: JSONSerializer and JSONDeserializer round trip")

# Protobuf, from a descriptor built in code (no protoc needed)
fdp = descriptor_pb2.FileDescriptorProto(name=f"event_{tag}.proto", package="doze", syntax="proto3")
msg = fdp.message_type.add(name="Event")
msg.field.add(name="name", number=1, type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING, label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL)
msg.field.add(name="at", number=2, type=descriptor_pb2.FieldDescriptorProto.TYPE_INT64, label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL)
pool = descriptor_pool.DescriptorPool(); pool.Add(fdp)
Event = message_factory.GetMessageClass(pool.FindMessageTypeByName("doze.Event"))
t = topic("proto")
roundtrip(t, ProtobufSerializer(Event, sr, {"use.deprecated.format": False}), ProtobufDeserializer(Event, {"use.deprecated.format": False}),
          Event(name="signup", at=1234), lambda v: v.name == "signup" and v.at == 1234)
ok("Protobuf: ProtobufSerializer and ProtobufDeserializer round trip")
# Protobuf, a message whose field's type is in another file: the serializer registers
# the imported file first and the message's own file with a reference to it (SCH-7).
dep = descriptor_pb2.FileDescriptorProto(name=f"addr_{tag}.proto", package="dep", syntax="proto3")
addr = dep.message_type.add(name="Addr")
addr.field.add(name="city", number=1, type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING, label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL)
main = descriptor_pb2.FileDescriptorProto(name=f"person_{tag}.proto", package="app", syntax="proto3", dependency=[dep.name])
person = main.message_type.add(name="Person")
person.field.add(name="home", number=1, type=descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE, type_name=".dep.Addr", label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL)
pool2 = descriptor_pool.DescriptorPool(); pool2.Add(dep); pool2.Add(main)
Person = message_factory.GetMessageClass(pool2.FindMessageTypeByName("app.Person"))
Addr = message_factory.GetMessageClass(pool2.FindMessageTypeByName("dep.Addr"))
t = topic("proto-import")
roundtrip(t, ProtobufSerializer(Person, sr, {"use.deprecated.format": False}), ProtobufDeserializer(Person, {"use.deprecated.format": False}),
          Person(home=Addr(city="Lisbon")), lambda v: v.home.city == "Lisbon")
refs = sr.get_latest_version(f"{t}-value").schema.references
if not refs or refs[0].subject != dep.name: fail(f"the message's schema does not reference its import: {refs!r}")
ok("Protobuf: a message importing another file registers both, with a reference, and round trips")

# Naming strategies: the subject is the record's full name, or the topic and the record's name (SCH-26).
from confluent_kafka.schema_registry import record_subject_name_strategy, topic_record_subject_name_strategy
named = '{"type":"record","name":"Named' + tag + '","namespace":"dev.doze","fields":[{"name":"id","type":"long"}]}'
for i, (strategy, subject_of) in enumerate(((record_subject_name_strategy, lambda t: f"dev.doze.Named{tag}"), (topic_record_subject_name_strategy, lambda t: f"{t}-dev.doze.Named{tag}"))):
    t = topic(f"strategy{i}")
    roundtrip(t, AvroSerializer(sr, named, conf={"subject.name.strategy": strategy}), AvroDeserializer(sr), {"id": 3}, lambda v: v == {"id": 3})
    if subject_of(t) not in sr.get_subjects(): fail(f"{strategy.__name__}: no subject {subject_of(t)} in {sr.get_subjects()}")
ok("Avro: RecordNameStrategy and TopicRecordNameStrategy register under the record's name, and round trip")

# READONLY: a schema the subject holds still serializes (the serializer registers it again
# and gets its id), a new one is refused with 42205.
import urllib.request, base64
t = topic("readonly")
roundtrip(t, AvroSerializer(sr, avro), AvroDeserializer(sr), {"id": 1, "sku": "a"}, lambda v: v["id"] == 1)
req = urllib.request.Request(f'{reg["url"]}/mode/{t}-value', data=b'{"mode":"READONLY"}', method="PUT", headers={"Content-Type": "application/json"})
if "basic.auth.user.info" in reg: req.add_header("Authorization", "Basic " + base64.b64encode(reg["basic.auth.user.info"].encode()).decode())
urllib.request.urlopen(req, timeout=10).read()
# A new topic, written under the same (now READONLY) subject.
held = f"{t}-value"
roundtrip(topic("readonly2"), AvroSerializer(sr, avro, conf={"auto.register.schemas": True, "subject.name.strategy": lambda ctx, name: held}),
          AvroDeserializer(sr), {"id": 2, "sku": "b"}, lambda v: v["id"] == 2)
try:
    sr.register_schema(f"{t}-value", Schema(compatible, "AVRO")); fail("a new schema was registered in a READONLY subject")
except SchemaRegistryError as e:
    if e.error_code != 42205: fail(f"a new schema in a READONLY subject failed with {e.error_code}, want 42205")
ok("READONLY: a held schema still serializes; a new one is refused with 42205")
print("all good")

# use.latest.version with auto.register.schemas off: the latest id, nothing registered.
t = topic("latest")
sr.register_schema(f"{t}-value", Schema(avro, "AVRO"))
latest = sr.register_schema(f"{t}-value", Schema(compatible, "AVRO"))
before = sr.get_versions(f"{t}-value")
ser = AvroSerializer(sr, avro, conf={"auto.register.schemas": False, "use.latest.version": True})
framed = ser({"id": 1, "sku": "x", "note": None}, SerializationContext(t, MessageField.VALUE))
if framed[0] != 0 or int.from_bytes(framed[1:5], "big") != latest: fail(f"use.latest.version framed id {int.from_bytes(framed[1:5], 'big')}, want {latest}")
if sr.get_versions(f"{t}-value") != before: fail("use.latest.version registered a version")
ok("Avro: use.latest.version with auto.register.schemas off writes the latest id and registers nothing")

# normalize.schemas: the same schema with its attributes in another order is the id it has.
reordered = '{"fields":[{"type":"long","name":"id"},{"type":"string","name":"sku"},{"default":null,"type":["null","string"],"name":"note"}],"name":"Order","type":"record"}'
ser = AvroSerializer(sr, reordered, conf={"normalize.schemas": True})
framed = ser({"id": 2, "sku": "y", "note": None}, SerializationContext(t, MessageField.VALUE))
if int.from_bytes(framed[1:5], "big") != latest or sr.get_versions(f"{t}-value") != before:
    fail(f"normalize.schemas: the reordered schema got id {int.from_bytes(framed[1:5], 'big')} and versions {sr.get_versions(f'{t}-value')}, want id {latest} and {before}")
ok("Avro: normalize.schemas registers a reordered schema as the id it has")

# A wrong password is a 401 (the registry asks for one under SASL).
if "basic.auth.user.info" in reg:
    wrong = SchemaRegistryClient({**reg, "basic.auth.user.info": os.environ["REGISTRY_USER"] + ":not-the-password"})
    try:
        wrong.get_subjects(); fail("a wrong password listed the subjects")
    except SchemaRegistryError as e:
        if e.http_status_code != 401: fail(f"a wrong password failed with {e.http_status_code}, want 401")
    ok("a wrong registry password is refused with 401")
