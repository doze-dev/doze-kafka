package dev.doze.compat.spring;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.apache.kafka.clients.admin.NewTopic;

/** A Spring Boot app that does what Spring apps do: declares topics as beans (KafkaAdmin creates
 *  them, and grows them when the declaration grows) and consumes in a transaction. */
@SpringBootApplication
public class Check {
  public static final String TAG = Long.toHexString(System.nanoTime()).substring(0, 6);
  public static final String IN = "sp-in-" + TAG;
  static final String OUT = "sp-out-" + TAG, TOPIC = "sp-orders-" + TAG;
  static final AtomicInteger poisonSeen = new AtomicInteger();

  static void ok(String m) { System.out.println("ok   " + m); }
  static void check(boolean c, String m) { if (!c) { System.out.println("FAIL " + m); System.exit(1); } }

  @Bean NewTopic orders(@org.springframework.beans.factory.annotation.Value("${check.partitions}") int partitions) {
    return TopicBuilder.name(TOPIC).partitions(partitions).replicas(1).config("retention.ms", "3600000").build();
  }
  @org.springframework.beans.factory.annotation.Autowired KafkaTemplate<String, String> template;

  @Bean NewTopic in() { return TopicBuilder.name(IN).partitions(1).replicas(1).build(); }
  @Bean NewTopic out() { return TopicBuilder.name(OUT).partitions(1).replicas(1).build(); }

  // Runs inside the container's Kafka transaction: what it sends commits with the offsets it read,
  // and a thrown exception aborts both and redelivers.
  @KafkaListener(topics = "#{T(dev.doze.compat.spring.Check).IN}", groupId = "#{'spring-' + T(dev.doze.compat.spring.Check).TAG}")
  void listen(String value) {
    if (value.equals("boom") && poisonSeen.getAndIncrement() == 0) throw new IllegalStateException("poison, first delivery");
    template.send(OUT, value.toUpperCase());
  }

  static String[] props(int partitions) {
    List<String> a = new ArrayList<>(List.of(
        "--spring.main.web-application-type=none", "--spring.main.banner-mode=off", "--logging.level.root=OFF",
        "--check.partitions=" + partitions,
        "--spring.kafka.bootstrap-servers=" + System.getenv("BOOTSTRAP"),
        "--spring.kafka.producer.transaction-id-prefix=tx-" + TAG + "-",
        "--spring.kafka.consumer.auto-offset-reset=earliest",
        "--spring.kafka.consumer.isolation-level=read_committed",
        "--spring.kafka.consumer.enable-auto-commit=false",
        "--spring.kafka.admin.fail-fast=true"));
    if ("scram-tls".equals(System.getenv("SECURITY"))) {
      a.addAll(List.of("--spring.kafka.security.protocol=SASL_SSL",
          "--spring.kafka.properties.sasl.mechanism=" + System.getenv("SASL_MECHANISM"),
          "--spring.kafka.properties.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\""
              + System.getenv("SASL_USER") + "\" password=\"" + System.getenv("SASL_PASS") + "\";",
          "--spring.kafka.ssl.trust-store-type=PEM", "--spring.kafka.ssl.trust-store-location=file:" + System.getenv("CA_CERT")));
    }
    return a.toArray(new String[0]);
  }

  static Properties adminProps() {
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

  static int partitionsOf(String topic) throws Exception {
    try (Admin a = Admin.create(adminProps())) {
      return a.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic).partitions().size();
    }
  }

  public static void main(String[] args) throws Exception {
    // First start: KafkaAdmin creates the three topics from the beans.
    try (ConfigurableApplicationContext ctx = SpringApplication.run(Check.class, props(2))) {
      check(partitionsOf(TOPIC) == 2, "KafkaAdmin created the topic with 2 partitions");
      ok("KafkaAdmin created the NewTopic beans");

      // A transactional listener: three inputs, one of them poison on its first delivery.
      KafkaTemplate<String, String> t = ctx.getBean(KafkaTemplate.class);
      t.executeInTransaction(ops -> { for (String v : List.of("a", "boom", "c")) ops.send(IN, v); return null; });
      Properties cp = adminProps();
      cp.put("group.id", "spring-check-" + TAG); cp.put("auto.offset.reset", "earliest"); cp.put("isolation.level", "read_committed");
      cp.put("key.deserializer", StringDeserializer.class.getName()); cp.put("value.deserializer", StringDeserializer.class.getName());
      List<String> got = new ArrayList<>();
      try (KafkaConsumer<String, String> c = new KafkaConsumer<>(cp)) {
        c.subscribe(List.of(OUT));
        long end = System.nanoTime() + Duration.ofSeconds(45).toNanos();
        while (got.size() < 3 && System.nanoTime() < end) c.poll(Duration.ofMillis(500)).forEach(r -> got.add(r.value()));
        // a moment more, to be sure nothing is duplicated
        end = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < end) c.poll(Duration.ofMillis(300)).forEach(r -> got.add(r.value()));
      }
      Collections.sort(got);
      check(got.equals(List.of("A", "BOOM", "C")), "each input produced exactly one output, despite the abort and redelivery: " + got);
      check(poisonSeen.get() >= 2, "the poison record was delivered again after its transaction aborted");
      ok("transactional listener: an aborted delivery is redelivered, outputs are exactly once");
    }

    // Second start: the declaration grew, and KafkaAdmin raises the partition count.
    try (ConfigurableApplicationContext ctx = SpringApplication.run(Check.class, props(4))) {
      check(partitionsOf(TOPIC) == 4, "KafkaAdmin did not grow the topic to 4 partitions, it has " + partitionsOf(TOPIC));
      ok("KafkaAdmin grew a topic when its NewTopic bean asked for more partitions");
    }
    try (Admin a = Admin.create(adminProps())) { a.deleteTopics(List.of(TOPIC, IN, OUT)).all().get(20, TimeUnit.SECONDS); }
    System.out.println("all good");
  }
}
