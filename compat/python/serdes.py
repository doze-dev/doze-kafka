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
print("all good")
