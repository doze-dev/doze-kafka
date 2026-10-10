package dev.doze.compat;

import java.nio.file.Files;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

/** Real Kafka clients against the broker under test: the Java client's admin, producer, consumer
 *  (classic and the KIP-848 protocol) and transactions, then a Kafka Streams word count. */
public class Check {
  static final String TAG = Long.toHexString(System.nanoTime()).substring(0, 6);

  static Properties base() {
    Properties p = new Properties();
    p.put("bootstrap.servers", System.getenv("BOOTSTRAP"));
    if ("scram-tls".equals(System.getenv("SECURITY"))) {
      p.put("security.protocol", "SASL_SSL");
      p.put("sasl.mechanism", System.getenv("SASL_MECHANISM"));
      p.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""
          + System.getenv("SASL_USER") + "\" password=\"" + System.getenv("SASL_PASS") + "\";");
      p.put("ssl.truststore.type", "PEM");
      p.put("ssl.truststore.location", System.getenv("CA_CERT"));
    }
    return p;
  }

  static Properties with(Object... kv) {
    Properties p = base();
    for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
    return p;
  }

  static void ok(String m) { System.out.println("ok   " + m); }

  static void check(boolean cond, String m) {
    if (!cond) { System.out.println("FAIL " + m); System.exit(1); }
  }

  static <T> T eventually(String what, Duration within, Supplier<T> probe, java.util.function.Predicate<T> done) {
    long end = System.nanoTime() + within.toNanos();
    T v = probe.get();
    while (!done.test(v)) {
      check(System.nanoTime() < end, what + " did not happen within " + within + " (last: " + v + ")");
      try { Thread.sleep(200); } catch (InterruptedException e) { throw new RuntimeException(e); }
      v = probe.get();
    }
    return v;
  }

  static KafkaConsumer<String, String> consumer(String group, Object... kv) {
    Properties p = with("group.id", group, "auto.offset.reset", "earliest", "enable.auto.commit", "false",
        "key.deserializer", StringDeserializer.class.getName(), "value.deserializer", StringDeserializer.class.getName());
    for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
    return new KafkaConsumer<>(p);
  }

  static KafkaProducer<String, String> producer(Object... kv) {
    Properties p = with("key.serializer", StringSerializer.class.getName(), "value.serializer", StringSerializer.class.getName());
    for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
    return new KafkaProducer<>(p);
  }

  static List<ConsumerRecord<String, String>> readAll(KafkaConsumer<String, String> c, int want, Duration within) {
    List<ConsumerRecord<String, String>> got = new ArrayList<>();
    long end = System.nanoTime() + within.toNanos();
    while (got.size() < want && System.nanoTime() < end) c.poll(Duration.ofMillis(500)).forEach(got::add);
    return got;
  }

  public static void main(String[] args) throws Exception {
    Set<String> sections = new HashSet<>(Arrays.asList(args));
    if (sections.contains("clients")) clients();
    if (sections.contains("streams")) streams();
    if (sections.contains("serdes")) SerdeCheck.run();
    System.out.println("all good");
  }

  // ------------------------------------------------------------------ clients

  static void clients() throws Exception {
    String topic = "java-" + TAG;
    try (Admin admin = Admin.create(base())) {
      admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1).configs(Map.of("retention.ms", "3600000")))).all().get(20, TimeUnit.SECONDS);
      TopicDescription d = admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
      check(d.partitions().size() == 1, "describeTopics: one partition");
      ConfigResource res = new ConfigResource(ConfigResource.Type.TOPIC, topic);
      Config cfg = admin.describeConfigs(List.of(res)).all().get(10, TimeUnit.SECONDS).get(res);
      check("3600000".equals(cfg.get("retention.ms").value()), "describeConfigs: the retention given at creation");
      admin.incrementalAlterConfigs(Map.of(res, List.of(new AlterConfigOp(new ConfigEntry("retention.ms", "7200000"), AlterConfigOp.OpType.SET)))).all().get(10, TimeUnit.SECONDS);
      cfg = admin.describeConfigs(List.of(res)).all().get(10, TimeUnit.SECONDS).get(res);
      check("7200000".equals(cfg.get("retention.ms").value()), "incrementalAlterConfigs changed the retention");
      admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(3))).all().get(10, TimeUnit.SECONDS);
      check(admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic).partitions().size() == 3, "createPartitions grew the topic");
      check(admin.listTopics().names().get(10, TimeUnit.SECONDS).contains(topic), "listTopics");
      ok("admin: create with config, describe, alter config, grow, list");

      // produce: idempotent, headers, explicit timestamps on partition 0
      long t0 = System.currentTimeMillis() - 60_000;
      try (KafkaProducer<String, String> p = producer("enable.idempotence", "true", "acks", "all")) {
        for (int i = 0; i < 30; i++) {
          ProducerRecord<String, String> r = new ProducerRecord<>(topic, 0, t0 + i * 1000L, "k" + i, "v" + i);
          r.headers().add("trace", "t1".getBytes());
          p.send(r);
        }
        p.flush();
      }
      ok("idempotent produce of 30 records");

      // timestamps and delete-records
      try (KafkaConsumer<String, String> c = consumer("unused")) {
        TopicPartition tp = new TopicPartition(topic, 0);
        Map<TopicPartition, OffsetAndTimestamp> at = c.offsetsForTimes(Map.of(tp, t0 + 10_000));
        check(at.get(tp) != null && at.get(tp).offset() == 10, "offsetsForTimes found offset 10, got " + at.get(tp));
        ok("offsetsForTimes");
        admin.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(5))).all().get(10, TimeUnit.SECONDS);
        check(c.beginningOffsets(List.of(tp)).get(tp) == 5L, "deleteRecords moved the start to 5");
        ok("deleteRecords");
      }

      // classic group: consume, commit, resume
      String g = "g-" + TAG;
      try (KafkaConsumer<String, String> c = consumer(g)) {
        c.subscribe(List.of(topic));
        List<ConsumerRecord<String, String>> got = readAll(c, 25, Duration.ofSeconds(25));
        check(got.size() == 25, "classic group consumed " + got.size() + " of 25");
        check(new String(got.get(0).headers().lastHeader("trace").value()).equals("t1"), "headers round trip");
        c.commitSync();
      }
      try (KafkaConsumer<String, String> c = consumer(g)) {
        c.subscribe(List.of(topic));
        check(readAll(c, 1, Duration.ofSeconds(4)).isEmpty(), "a resumed group re-read a committed record");
      }
      ok("classic group: consume 25, commit, resume");
      var listed = admin.listConsumerGroups().all().get(10, TimeUnit.SECONDS);
      check(listed.stream().anyMatch(x -> x.groupId().equals(g)), "listConsumerGroups shows the group");
      check(admin.describeConsumerGroups(List.of(g)).all().get(10, TimeUnit.SECONDS).get(g).state() == ConsumerGroupState.EMPTY
          || admin.describeConsumerGroups(List.of(g)).all().get(10, TimeUnit.SECONDS).get(g).groupState() != null, "describeConsumerGroups");
      admin.deleteConsumerGroups(List.of(g)).all().get(10, TimeUnit.SECONDS);
      ok("admin: list, describe and delete a group");

      // the KIP-848 protocol
      try (KafkaConsumer<String, String> c = consumer("gn-" + TAG, "group.protocol", "consumer")) {
        c.subscribe(List.of(topic));
        List<ConsumerRecord<String, String>> got = readAll(c, 25, Duration.ofSeconds(25));
        check(got.size() == 25, "group.protocol=consumer consumed " + got.size() + " of 25");
      }
      ok("group.protocol=consumer (KIP-848)");

      // transactions: commit one, abort one; read_committed sees only the first
      String txTopic = "java-tx-" + TAG;
      admin.createTopics(List.of(new NewTopic(txTopic, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
      try (KafkaProducer<String, String> p = producer("transactional.id", "tx-" + TAG)) {
        p.initTransactions();
        p.beginTransaction(); p.send(new ProducerRecord<>(txTopic, "k", "committed")); p.commitTransaction();
        // Flushed first: an abort discards what has not been sent yet, and the point is a record
        // that WAS written and then aborted.
        p.beginTransaction(); p.send(new ProducerRecord<>(txTopic, "k", "aborted")); p.flush(); p.abortTransaction();
        p.beginTransaction(); p.send(new ProducerRecord<>(txTopic, "k", "committed-too")); p.commitTransaction();
      }
      try (KafkaConsumer<String, String> c = consumer("rc-" + TAG, "isolation.level", "read_committed")) {
        c.subscribe(List.of(txTopic));
        List<String> vals = new ArrayList<>();
        readAll(c, 2, Duration.ofSeconds(20)).forEach(r -> vals.add(r.value()));
        check(vals.equals(List.of("committed", "committed-too")), "read_committed saw " + vals);
      }
      try (KafkaConsumer<String, String> c = consumer("ru-" + TAG, "isolation.level", "read_uncommitted")) {
        c.subscribe(List.of(txTopic));
        List<String> vals = new ArrayList<>();
        readAll(c, 5, Duration.ofSeconds(6)).stream().filter(r -> !r.value().isEmpty()).forEach(r -> vals.add(r.value()));
        check(vals.contains("aborted"), "read_uncommitted should still see the aborted record, saw " + vals);
      }
      ok("transactions: aborted record hidden from read_committed");

      // consume-transform-produce: offsets commit with the transaction
      String out = "java-out-" + TAG;
      admin.createTopics(List.of(new NewTopic(out, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
      try (KafkaConsumer<String, String> c = consumer("ctp-" + TAG, "isolation.level", "read_committed");
           KafkaProducer<String, String> p = producer("transactional.id", "ctp-" + TAG)) {
        p.initTransactions();
        c.subscribe(List.of(topic));
        List<ConsumerRecord<String, String>> in = readAll(c, 10, Duration.ofSeconds(20));
        check(in.size() >= 10, "ctp read " + in.size());
        p.beginTransaction();
        for (ConsumerRecord<String, String> r : in) p.send(new ProducerRecord<>(out, r.key(), r.value().toUpperCase()));
        Map<TopicPartition, OffsetAndMetadata> offs = new HashMap<>();
        for (ConsumerRecord<String, String> r : in) offs.merge(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1),
            (a, b) -> a.offset() > b.offset() ? a : b);
        p.sendOffsetsToTransaction(offs, c.groupMetadata());
        p.commitTransaction();
        Map<TopicPartition, OffsetAndMetadata> committed = c.committed(offs.keySet());
        check(committed.values().stream().allMatch(Objects::nonNull), "the transaction committed the consumer's offsets");
      }
      ok("transactions: consume-transform-produce with offsets");

      admin.deleteTopics(List.of(topic, txTopic, out)).all().get(20, TimeUnit.SECONDS);
      ok("admin: delete topics");
    }
  }

  // ------------------------------------------------------------------ streams

  static Properties streamsProps(String app, String stateDir) {
    Properties p = base();
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, app);
    p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    p.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
    p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
    return p;
  }

  static Topology wordCount(String in, String out) {
    StreamsBuilder b = new StreamsBuilder();
    b.<String, String>stream(in)
        .flatMapValues(v -> Arrays.asList(v.toLowerCase().split("\\W+")))
        .groupBy((k, w) -> w)                       // a repartition topic
        .count(Materialized.as("counts"))           // a changelog topic, compacted
        .toStream()
        .to(out, Produced.with(Serdes.String(), Serdes.Long()));
    return b.build();
  }

  static Map<String, Long> counts(String out, int words) {
    Map<String, Long> latest = new HashMap<>();
    try (KafkaConsumer<String, Long> c = new KafkaConsumer<>(with("group.id", "wc-read-" + System.nanoTime(), "auto.offset.reset", "earliest",
        "isolation.level", "read_committed", "key.deserializer", StringDeserializer.class.getName(), "value.deserializer", LongDeserializer.class.getName()))) {
      c.subscribe(List.of(out));
      long end = System.nanoTime() + Duration.ofSeconds(40).toNanos();
      while (latest.size() < words && System.nanoTime() < end) c.poll(Duration.ofMillis(500)).forEach(r -> latest.put(r.key(), r.value()));
      // keep reading a moment: later counts replace earlier ones
      end = System.nanoTime() + Duration.ofSeconds(3).toNanos();
      while (System.nanoTime() < end) c.poll(Duration.ofMillis(300)).forEach(r -> latest.put(r.key(), r.value()));
    }
    return latest;
  }

  static void streams() throws Exception {
    String in = "wc-in-" + TAG, out = "wc-out-" + TAG, app = "wc-" + TAG;
    try (Admin admin = Admin.create(base())) {
      admin.createTopics(List.of(new NewTopic(in, 2, (short) 1), new NewTopic(out, 2, (short) 1))).all().get(20, TimeUnit.SECONDS);
    }
    try (KafkaProducer<String, String> p = producer()) {
      for (String line : List.of("the quick brown fox", "the lazy dog", "the fox")) p.send(new ProducerRecord<>(in, line));
      p.flush();
    }
    String state1 = Files.createTempDirectory("streams1").toString();
    KafkaStreams s = new KafkaStreams(wordCount(in, out), streamsProps(app, state1));
    s.start();
    Map<String, Long> first = counts(out, 6);
    s.close(Duration.ofSeconds(20));
    check(Long.valueOf(3).equals(first.get("the")) && Long.valueOf(2).equals(first.get("fox")) && Long.valueOf(1).equals(first.get("dog")),
        "word counts after the first run: " + first);
    ok("streams: word count, exactly_once_v2, repartition and changelog topics");

    try (Admin admin = Admin.create(base())) {
      Set<String> names = admin.listTopics().names().get(10, TimeUnit.SECONDS);
      check(names.stream().anyMatch(n -> n.startsWith(app) && n.endsWith("-repartition")), "streams created a repartition topic: " + names);
      String changelog = names.stream().filter(n -> n.startsWith(app) && n.endsWith("-changelog")).findFirst().orElse(null);
      check(changelog != null, "streams created a changelog topic: " + names);
      ConfigResource res = new ConfigResource(ConfigResource.Type.TOPIC, changelog);
      String policy = admin.describeConfigs(List.of(res)).all().get(10, TimeUnit.SECONDS).get(res).get("cleanup.policy").value();
      check(policy.contains("compact"), "the changelog is compacted, got " + policy);
    }
    ok("streams: the changelog is a compacted topic");

    // A new instance with an empty state directory must rebuild its counts from the changelog
    // and carry on: 'the' is 3 and becomes 5 (the new line has it twice), which only works if the restore was right.
    try (KafkaProducer<String, String> p = producer()) {
      p.send(new ProducerRecord<>(in, "the end of the dog"));
      p.flush();
    }
    String state2 = Files.createTempDirectory("streams2").toString();
    KafkaStreams s2 = new KafkaStreams(wordCount(in, out), streamsProps(app, state2));
    s2.start();
    Map<String, Long> second;
    long end = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    do { second = counts(out, 8); } while (!Long.valueOf(5).equals(second.get("the")) && System.nanoTime() < end);
    s2.close(Duration.ofSeconds(20));
    check(Long.valueOf(5).equals(second.get("the")) && Long.valueOf(2).equals(second.get("dog")) && Long.valueOf(1).equals(second.get("end")),
        "word counts after restoring state from the changelog: " + second);
    ok("streams: a new instance restores its state from the compacted changelog and carries on");
  }
}
