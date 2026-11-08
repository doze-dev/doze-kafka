package dev.doze.compat;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import static dev.doze.compat.Check.*;

/** Confluent's own serializers (Avro, Protobuf, JSON Schema) against the built-in registry:
 *  only schema.registry.url changed. */
final class SerdeCheck {
  static Map<String, Object> registry() {
    Map<String, Object> m = new HashMap<>();
    m.put("schema.registry.url", System.getenv("REGISTRY_URL"));
    if ("scram-tls".equals(System.getenv("SECURITY"))) {
      m.put("basic.auth.credentials.source", "USER_INFO");
      m.put("basic.auth.user.info", System.getenv("REGISTRY_USER") + ":" + System.getenv("REGISTRY_PASS"));
    }
    return m;
  }

  static <V> KafkaProducer<String, V> producer(Class<?> ser, Object... kv) {
    Properties p = Check.with("key.serializer", StringSerializer.class.getName(), "value.serializer", ser.getName());
    registry().forEach(p::put);
    for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
    return new KafkaProducer<>(p);
  }

  /** The RestClientException somewhere in e's causes, or null. */
  static RestClientException restCause(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) if (t instanceof RestClientException rce) return rce;
    return null;
  }

  static <V> KafkaConsumer<String, V> consumer(Class<?> de, Object... kv) {
    Properties p = Check.with("group.id", "sd-" + System.nanoTime(), "auto.offset.reset", "earliest",
        "key.deserializer", StringDeserializer.class.getName(), "value.deserializer", de.getName());
    registry().forEach(p::put);
    for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
    return new KafkaConsumer<>(p);
  }

  static <V> List<ConsumerRecord<String, V>> read(KafkaConsumer<String, V> c, String topic, int want) {
    c.subscribe(List.of(topic));
    List<ConsumerRecord<String, V>> got = new ArrayList<>();
    long end = System.nanoTime() + Duration.ofSeconds(25).toNanos();
    while (got.size() < want && System.nanoTime() < end) c.poll(Duration.ofMillis(500)).forEach(got::add);
    return got;
  }

  static void run() throws Exception {
    check(System.getenv("REGISTRY_URL") != null && !System.getenv("REGISTRY_URL").isEmpty(), "no REGISTRY_URL given");
    String tag = Check.TAG;
    String avro = "av-" + tag, proto = "pb-" + tag, json = "js-" + tag;
    try (Admin a = Admin.create(Check.base())) {
      a.createTopics(List.of(new NewTopic(avro, 1, (short) 1), new NewTopic(proto, 1, (short) 1), new NewTopic(json, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
    }
    SchemaRegistryClient sr = new CachedSchemaRegistryClient(System.getenv("REGISTRY_URL"), 50, registry().entrySet().stream()
        .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    // ---- Avro: round trip, then an evolution the registry must accept and one it must refuse
    Schema v1 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"sku\",\"type\":\"string\"}]}");
    try (KafkaProducer<String, Object> p = producer(KafkaAvroSerializer.class)) {
      for (int i = 0; i < 3; i++) {
        GenericRecord r = new GenericData.Record(v1);
        r.put("id", (long) i); r.put("sku", "sku-" + i);
        p.send(new ProducerRecord<>(avro, "k" + i, r));
      }
      p.flush();
    }
    try (KafkaConsumer<String, Object> c = consumer(KafkaAvroDeserializer.class)) {
      List<ConsumerRecord<String, Object>> got = read(c, avro, 3);
      check(got.size() == 3, "avro: consumed " + got.size() + " of 3");
      GenericRecord r = (GenericRecord) got.get(2).value();
      check(Long.valueOf(2).equals(r.get("id")) && "sku-2".equals(r.get("sku").toString()), "avro: the record read back is " + r);
    }
    ok("Avro: KafkaAvroSerializer and Deserializer round trip");

    Schema v2 = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"sku\",\"type\":\"string\"},{\"name\":\"note\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
    try (KafkaProducer<String, Object> p = producer(KafkaAvroSerializer.class)) {
      GenericRecord r = new GenericData.Record(v2);
      r.put("id", 9L); r.put("sku", "x"); r.put("note", "hello");
      p.send(new ProducerRecord<>(avro, "k9", r)).get(10, TimeUnit.SECONDS);
    }
    check(sr.getAllVersions(avro + "-value").size() == 2, "avro: a compatible evolution registered version 2");
    Schema bad = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"sku\",\"type\":\"string\"},{\"name\":\"must\",\"type\":\"string\"}]}");
    try (KafkaProducer<String, Object> p = producer(KafkaAvroSerializer.class)) {
      GenericRecord r = new GenericData.Record(bad);
      r.put("id", 1L); r.put("sku", "x"); r.put("must", "y");
      try { p.send(new ProducerRecord<>(avro, "bad", r)).get(10, TimeUnit.SECONDS); check(false, "avro: an incompatible schema was registered"); }
      catch (Exception e) {
        Throwable t = e; boolean conflict = false;
        while (t != null) { if (t instanceof RestClientException rce && rce.getStatus() == 409) conflict = true; t = t.getCause(); }
        check(conflict, "avro: an incompatible evolution should fail with 409, failed with " + e);
        // The reason is Confluent's own, typed (SCH-6).
        RestClientException rce = restCause(e);
        check(rce != null && rce.getMessage().contains("READER_FIELD_MISSING_DEFAULT_VALUE") && rce.getMessage().contains("'/fields/2'"),
            "avro: the 409 does not give Confluent's reason: " + (rce == null ? e : rce.getMessage()));
      }
    }
    ok("Avro: a compatible evolution is accepted, an incompatible one is refused with 409 and Confluent's reason");

    // ---- Avro under RecordNameStrategy and TopicRecordNameStrategy (SCH-26)
    Schema named = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Named" + tag + "\",\"namespace\":\"dev.doze\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}");
    String[][] strategies = {
        {"io.confluent.kafka.serializers.subject.RecordNameStrategy", "dev.doze.Named" + tag},
        {"io.confluent.kafka.serializers.subject.TopicRecordNameStrategy", null}};
    for (int i = 0; i < strategies.length; i++) {
      String t = "ns" + i + "-" + tag;
      try (Admin a = Admin.create(Check.base())) { a.createTopics(List.of(new NewTopic(t, 1, (short) 1))).all().get(20, TimeUnit.SECONDS); }
      try (KafkaProducer<String, Object> p = producer(KafkaAvroSerializer.class, "value.subject.name.strategy", strategies[i][0])) {
        GenericRecord r = new GenericData.Record(named);
        r.put("id", 3L);
        p.send(new ProducerRecord<>(t, "k", r)).get(10, TimeUnit.SECONDS);
      }
      try (KafkaConsumer<String, Object> c = consumer(KafkaAvroDeserializer.class)) {
        List<ConsumerRecord<String, Object>> got = read(c, t, 1);
        check(got.size() == 1 && Long.valueOf(3).equals(((GenericRecord) got.get(0).value()).get("id")), strategies[i][0] + ": read back " + got);
      }
      String subject = strategies[i][1] != null ? strategies[i][1] : t + "-dev.doze.Named" + tag;
      check(sr.getAllSubjects().contains(subject), strategies[i][0] + ": no subject " + subject);
    }
    ok("Avro: RecordNameStrategy and TopicRecordNameStrategy register under the record's name and round trip");

    // ---- a record framed with an id the registry does not hold: the deserializer says 40403
    String unknown = "unk-" + tag;
    try (Admin a = Admin.create(Check.base())) { a.createTopics(List.of(new NewTopic(unknown, 1, (short) 1))).all().get(20, TimeUnit.SECONDS); }
    Properties raw = Check.with("key.serializer", StringSerializer.class.getName(), "value.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
    try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(raw)) {
      p.send(new ProducerRecord<>(unknown, "k", new byte[] {0, 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xf0, 2, 'h', 'i'})).get(10, TimeUnit.SECONDS);
    }
    try (KafkaConsumer<String, Object> c = consumer(KafkaAvroDeserializer.class)) {
      c.subscribe(List.of(unknown));
      Throwable failed = null;
      long end = System.nanoTime() + Duration.ofSeconds(25).toNanos();
      while (failed == null && System.nanoTime() < end) {
        try { c.poll(Duration.ofMillis(500)); } catch (Exception e) { failed = e; }
      }
      RestClientException rce = restCause(failed);
      check(rce != null && rce.getErrorCode() == 40403, "an unknown schema id: the deserializer failed with " + failed);
    }
    ok("an unknown schema id: the Avro deserializer fails with 40403");

    // ---- use.latest.version: with auto.register.schemas off, the serializer writes the
    // subject's latest id (v2 here) and registers nothing.
    String avroSubject = avro + "-value";
    int latestId = sr.getLatestSchemaMetadata(avroSubject).getId();
    int versionsBefore = sr.getAllVersions(avroSubject).size();
    try (KafkaProducer<String, Object> p = producer(KafkaAvroSerializer.class, "auto.register.schemas", false, "use.latest.version", true)) {
      GenericRecord r = new GenericData.Record(v2);
      r.put("id", 11L); r.put("sku", "latest"); r.put("note", null);
      p.send(new ProducerRecord<>(avro, "latest", r)).get(10, TimeUnit.SECONDS);
    }
    Properties rawRead = Check.with("group.id", "raw-" + System.nanoTime(), "auto.offset.reset", "earliest",
        "key.deserializer", StringDeserializer.class.getName(), "value.deserializer", org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
    try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(rawRead)) {
      byte[] last = read(c, avro, 5).stream().filter(x -> "latest".equals(x.key())).findFirst().map(ConsumerRecord::value).orElse(null);
      check(last != null && last.length > 5 && last[0] == 0 && java.nio.ByteBuffer.wrap(last, 1, 4).getInt() == latestId,
          "use.latest.version: the record is not framed with the latest id " + latestId);
    }
    check(sr.getAllVersions(avroSubject).size() == versionsBefore, "use.latest.version: a version was registered");
    ok("Avro: use.latest.version with auto.register.schemas off writes the latest id and registers nothing");

    // ---- normalize.schemas: the registry takes ?normalize=true; the same schema is the same id
    try (KafkaProducer<String, Object> p = producer(KafkaAvroSerializer.class, "normalize.schemas", true)) {
      GenericRecord r = new GenericData.Record(v2);
      r.put("id", 12L); r.put("sku", "normalized"); r.put("note", "n");
      p.send(new ProducerRecord<>(avro, "normalized", r)).get(10, TimeUnit.SECONDS);
    }
    check(sr.getAllVersions(avroSubject).size() == versionsBefore && sr.getLatestSchemaMetadata(avroSubject).getId() == latestId,
        "normalize.schemas: the schema the subject holds was registered again as a new version or id");
    ok("Avro: normalize.schemas registers a held schema as the id it has");

    // ---- READONLY: a new version is refused with 42205 (SCH-14); the held one still serializes
    String ro = "ro-" + tag;
    try (Admin a = Admin.create(Check.base())) { a.createTopics(List.of(new NewTopic(ro, 1, (short) 1))).all().get(20, TimeUnit.SECONDS); }
    try (KafkaProducer<String, Object> p = producer(KafkaAvroSerializer.class)) {
      GenericRecord r = new GenericData.Record(v1);
      r.put("id", 1L); r.put("sku", "ro");
      p.send(new ProducerRecord<>(ro, "k", r)).get(10, TimeUnit.SECONDS);
    }
    sr.setMode("READONLY", ro + "-value");
    try (KafkaProducer<String, Object> p = producer(KafkaAvroSerializer.class)) {
      GenericRecord held = new GenericData.Record(v1);
      held.put("id", 2L); held.put("sku", "held");
      p.send(new ProducerRecord<>(ro, "k", held)).get(10, TimeUnit.SECONDS);
      GenericRecord r = new GenericData.Record(v2);
      r.put("id", 3L); r.put("sku", "new"); r.put("note", null);
      try { p.send(new ProducerRecord<>(ro, "k", r)).get(10, TimeUnit.SECONDS); check(false, "READONLY: a new version was registered"); }
      catch (Exception e) {
        RestClientException rce = restCause(e);
        check(rce != null && rce.getErrorCode() == 42205, "READONLY: a new version failed with " + e + ", want 42205");
      }
    }
    ok("READONLY: a held schema still serializes; a new version is refused with 42205");

    // ---- a wrong password is the client's 401 (the registry asks for one under SASL)
    if ("scram-tls".equals(System.getenv("SECURITY"))) {
      Map<String, Object> wrong = new HashMap<>(registry());
      wrong.put("basic.auth.user.info", System.getenv("REGISTRY_USER") + ":not-the-password");
      SchemaRegistryClient wrongClient = new CachedSchemaRegistryClient(System.getenv("REGISTRY_URL"), 10, wrong);
      try { wrongClient.getAllSubjects(); check(false, "a wrong password listed the subjects"); }
      catch (RestClientException e) { check(e.getStatus() == 401, "a wrong password failed with " + e.getStatus() + ", want 401"); }
      ok("a wrong registry password is refused with 401");
    }

    // ---- Protobuf: a DynamicMessage from a schema, round trip
    ProtobufSchema ps = new ProtobufSchema("syntax = \"proto3\";\nmessage Event { string name = 1; int64 at = 2; repeated string tags = 3; }\n");
    try (KafkaProducer<String, Object> p = producer(KafkaProtobufSerializer.class)) {
      DynamicMessage m = DynamicMessage.newBuilder(ps.toDescriptor()).setField(ps.toDescriptor().findFieldByName("name"), "signup")
          .setField(ps.toDescriptor().findFieldByName("at"), 1234L).addRepeatedField(ps.toDescriptor().findFieldByName("tags"), "web").build();
      p.send(new ProducerRecord<>(proto, "k", m)).get(10, TimeUnit.SECONDS);
    }
    try (KafkaConsumer<String, Object> c = consumer(KafkaProtobufDeserializer.class)) {
      List<ConsumerRecord<String, Object>> got = read(c, proto, 1);
      check(got.size() == 1, "protobuf: consumed " + got.size() + " of 1");
      DynamicMessage m = (DynamicMessage) got.get(0).value();
      check(m.toString().contains("signup") && m.toString().contains("1234") && m.toString().contains("web"), "protobuf: read back " + m);
    }
    ok("Protobuf: KafkaProtobufSerializer and Deserializer round trip");

    // ---- JSON Schema: an envelope (schema + value), round trip
    String js = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"email\":{\"type\":\"string\"}},\"required\":[\"id\"]}";
    ObjectMapper om = new ObjectMapper();
    ObjectNode val = om.createObjectNode().put("id", 7).put("email", "a@b.c");
    try (KafkaProducer<String, Object> p = producer(KafkaJsonSchemaSerializer.class)) {
      p.send(new ProducerRecord<>(json, "k", JsonSchemaUtils.envelope(new JsonSchema(js), val))).get(10, TimeUnit.SECONDS);
    }
    try (KafkaConsumer<String, Object> c = consumer(KafkaJsonSchemaDeserializer.class)) {
      List<ConsumerRecord<String, Object>> got = read(c, json, 1);
      check(got.size() == 1, "json schema: consumed " + got.size() + " of 1");
      check(got.get(0).value().toString().contains("a@b.c"), "json schema: read back " + got.get(0).value());
    }
    check(sr.getAllSubjects().containsAll(List.of(avro + "-value", proto + "-value", json + "-value")), "all three subjects are registered");
    ok("JSON Schema: KafkaJsonSchemaSerializer and Deserializer round trip");
  }
}
