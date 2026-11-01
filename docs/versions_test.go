package docs

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The "Version tested" column of clients.md's compatibility table is what the
// compat cells run: each number is read from the file that pins it, so a bump
// there that the page does not follow fails here. kcat and its librdkafka are pinned
// in compat/versions.env too, which scripts/install-kcat.sh builds in CI.
func TestClientVersionsInTheTableAreThePinnedOnes(t *testing.T) {
	read := func(parts ...string) string {
		raw, err := os.ReadFile(filepath.Join(append([]string{"..", "compat"}, parts...)...))
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	find := func(src, re, what string) string {
		m := regexp.MustCompile(re).FindStringSubmatch(src)
		if m == nil {
			t.Fatalf("no %s pinned (%s)", what, re)
		}
		return m[1]
	}
	javaPOM := read("java", "pom.xml")
	kafka := find(javaPOM, `<kafka.version>([^<]+)<`, "Kafka client version in compat/java/pom.xml")
	confluent := find(javaPOM, `<confluent.version>([^<]+)<`, "Confluent serializer version in compat/java/pom.xml")
	boot := find(read("spring", "pom.xml"), `spring-boot-starter-parent</artifactId>\s*<version>([^<]+)<`, "Spring Boot version in compat/spring/pom.xml")
	kafkajs := find(read("kafkajs", "package.json"), `"kafkajs":\s*"([^"]+)"`, "kafkajs version in compat/kafkajs/package.json")
	versions := read("versions.env")
	python := find(versions, `(?m)^CONFLUENT_KAFKA_PY=(\S+)$`, "confluent-kafka version in compat/versions.env")
	kcat := find(versions, `(?m)^KCAT_VERSION=(\S+)$`, "kcat version in compat/versions.env")
	librdkafka := find(versions, `(?m)^LIBRDKAFKA_VERSION=(\S+)$`, "librdkafka version in compat/versions.env")

	raw, err := os.ReadFile("clients.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)
	row := func(client string) string {
		for _, l := range strings.Split(doc, "\n") {
			if strings.HasPrefix(l, "| ") && strings.Contains(strings.SplitN(l[2:], "|", 2)[0], client) {
				return strings.TrimSpace(strings.Split(l, "|")[2])
			}
		}
		t.Fatalf("clients.md's table has no %s row", client)
		return ""
	}
	for client, want := range map[string]string{
		"**Java** client":       kafka,
		"**Kafka Streams**":     kafka,
		"**Spring Kafka**":      "Spring Boot " + boot,
		"**confluent-kafka**":   python,
		"**KafkaJS**":           kafkajs,
		"**kcat** (librdkafka)": kcat + " with librdkafka " + librdkafka,
	} {
		if got := row(client); got != want && !strings.HasPrefix(got, want+" ") {
			t.Errorf("clients.md gives %s as %q; compat runs %q", client, got, want)
		}
	}
	if !strings.Contains(doc, "serializers ("+confluent+")") {
		t.Errorf("clients.md does not give the Confluent serializers as %s, the version compat/java/pom.xml pins", confluent)
	}
}
