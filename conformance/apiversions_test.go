package conformance

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// The ApiVersions answer of a real broker of each Kafka major doze-kafka has a
// profile for (--kafka-version 1 to 4), recorded once and committed as
// recordings/api-versions.json. internal/protocol holds each profile's table to
// it (TestNoProfileServesWhatARealBrokerOfThatMajorDoesNot): a profile may offer
// less than the broker it emulates, never a version or an API that broker does
// not serve.
//
// Which broker stands for a major is a choice, written here and in the file: the
// last minor of the major there is an image of.
//
//   - 4: apache/kafka:4.3.1, the image every other recording is of.
//   - 3: apache/kafka:3.9.1, Apache's own image of the last 3.x.
//   - 2 and 1: Apache published no images before 3.7. Confluent's community
//     images are the Apache broker under Confluent's packaging: cp-kafka:6.2.15
//     is Confluent Platform 6.2, which Confluent documents as Apache Kafka 2.8
//     (docs.confluent.io, "Supported Versions and Interoperability"), and
//     cp-kafka:4.1.4 ships kafka_2.11-1.1.1-cp4.jar, Apache Kafka 1.1.1. Each
//     entry records the broker jar found in the container, so the file says what
//     answered. Both run against ZooKeeper, in a second container.
type versionBroker struct {
	Major int
	Image string
	// Zookeeper, when set, is the image of the ZooKeeper the broker needs.
	Zookeeper string
}

var versionBrokers = []versionBroker{
	{Major: 1, Image: "confluentinc/cp-kafka:4.1.4", Zookeeper: "confluentinc/cp-zookeeper:4.1.4"},
	{Major: 2, Image: "confluentinc/cp-kafka:6.2.15", Zookeeper: "confluentinc/cp-zookeeper:6.2.15"},
	{Major: 3, Image: "apache/kafka:3.9.1"},
	{Major: 4, Image: KafkaImage},
}

// APIVersionsRecording is recordings/api-versions.json.
type APIVersionsRecording struct {
	Note    string               `json:"note"`
	Brokers []APIVersionsOfMajor `json:"brokers"`
}

// APIVersionsOfMajor is what one real broker answered to ApiVersions v0.
type APIVersionsOfMajor struct {
	Major int    `json:"major"`
	Image string `json:"image"`
	// Jar is the broker jar in the container: the version that answered.
	Jar  string         `json:"jar"`
	APIs []APIVersionOf `json:"apis"`
}

// APIVersionOf is one line of an ApiVersions answer.
type APIVersionOf struct {
	Key  int16  `json:"key"`
	Name string `json:"name"`
	Min  int16  `json:"min"`
	Max  int16  `json:"max"`
}

const apiVersionsFile = "api-versions.json"

// TestRecordApiVersions is the recorder. Nothing is written without -record; with
// -only it runs only for -only=api-versions.
func TestRecordApiVersions(t *testing.T) {
	if !*record {
		t.Skip("recorder: run with -record (go tool task conformance:record); needs Docker")
	}
	if *only != "" && *only != "api-versions" {
		t.Skip("-only names another recording")
	}
	if !DockerAvailable() {
		t.Skip("Docker is not available: nothing was recorded")
	}
	rec := APIVersionsRecording{Note: "ApiVersions v0 of a real broker of each Kafka major, on the client listener of a single node with nothing configured (conformance/apiversions_test.go says which image stands for which major and why). SaslHandshake and SaslAuthenticate are listed whether or not SASL is on."}
	for _, b := range versionBrokers {
		addr, jar, stop, err := startVersionBroker(b)
		if err != nil {
			t.Fatalf("Kafka %d (%s): %v", b.Major, b.Image, err)
		}
		w, err := dial(addr)
		if err != nil {
			stop()
			t.Fatal(err)
		}
		resp, err := w.roundTrip(kmsg.NewPtrApiVersionsRequest(), 0)
		w.close()
		stop()
		if err != nil {
			t.Fatalf("Kafka %d (%s): ApiVersions: %v", b.Major, b.Image, err)
		}
		av := resp.(*kmsg.ApiVersionsResponse)
		if av.ErrorCode != 0 || len(av.ApiKeys) == 0 {
			t.Fatalf("Kafka %d (%s): ApiVersions answered error %d and %d APIs", b.Major, b.Image, av.ErrorCode, len(av.ApiKeys))
		}
		m := APIVersionsOfMajor{Major: b.Major, Image: b.Image, Jar: jar}
		for _, k := range av.ApiKeys {
			m.APIs = append(m.APIs, APIVersionOf{Key: k.ApiKey, Name: kmsg.NameForKey(k.ApiKey), Min: k.MinVersion, Max: k.MaxVersion})
		}
		sort.Slice(m.APIs, func(i, j int) bool { return m.APIs[i].Key < m.APIs[j].Key })
		rec.Brokers = append(rec.Brokers, m)
		t.Logf("Kafka %d: %s (%s): %d APIs", b.Major, b.Image, jar, len(m.APIs))
	}
	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(recordingsDir, apiVersionsFile), append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// startVersionBroker runs one broker of a major and returns its address, the broker
// jar in its container, and how to remove it.
func startVersionBroker(b versionBroker) (addr, jar string, stop func(), err error) {
	if b.Zookeeper == "" {
		t, err := startKafka(b.Image, 0, nil, "", "")
		if err != nil {
			return "", "", nil, err
		}
		name := t.(*dockerTarget).name
		return t.Addr(), brokerJar(name), t.Close, nil
	}
	port, err := freePort()
	if err != nil {
		return "", "", nil, err
	}
	id := fmt.Sprintf("doze-conformance-%d", time.Now().UnixNano())
	network, zk, kafka := id+"-net", id+"-zk", id
	stop = func() {
		exec.Command("docker", "rm", "-f", kafka, zk).Run()
		exec.Command("docker", "network", "rm", network).Run()
	}
	run := func(args ...string) error {
		if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
			return fmt.Errorf("docker %s: %v: %s", args[0], err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	if err := run("network", "create", network); err != nil {
		return "", "", nil, err
	}
	if err := run("run", "-d", "--name", zk, "--network", network, "--platform", "linux/amd64",
		"-e", "ZOOKEEPER_CLIENT_PORT=2181", b.Zookeeper); err != nil {
		stop()
		return "", "", nil, err
	}
	if err := run("run", "-d", "--name", kafka, "--network", network, "--platform", "linux/amd64",
		"-p", fmt.Sprintf("127.0.0.1:%d:9092", port),
		"-e", "KAFKA_BROKER_ID=1",
		"-e", "KAFKA_ZOOKEEPER_CONNECT="+zk+":2181",
		// Two listeners: the broker's controller reaches the broker through an
		// advertised address, and the host's is not one the container can reach.
		"-e", "KAFKA_LISTENERS=INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092",
		"-e", fmt.Sprintf("KAFKA_ADVERTISED_LISTENERS=INTERNAL://%s:29092,EXTERNAL://127.0.0.1:%d", kafka, port),
		"-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT",
		"-e", "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
		"-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		b.Image); err != nil {
		stop()
		return "", "", nil, err
	}
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	// Metadata v1 is in every major; the readiness probe of the other recordings
	// (v9) is not.
	var last error
	for deadline := time.Now().Add(5 * time.Minute); ; time.Sleep(time.Second) {
		w, err := dialOnce(addr)
		if err == nil {
			w.fast = true
			var resp kmsg.Response
			resp, err = w.roundTrip(kmsg.NewPtrMetadataRequest(), 1)
			w.close()
			if err == nil && len(resp.(*kmsg.MetadataResponse).Brokers) > 0 {
				return addr, brokerJar(kafka), stop, nil
			}
		}
		if err != nil {
			last = err
		}
		// The image gives ZooKeeper forty seconds to answer and exits if it has
		// not: under emulation it sometimes has not. Start the broker again.
		if out, _ := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", kafka).Output(); strings.TrimSpace(string(out)) == "false" {
			exec.Command("docker", "start", kafka).Run()
		}
		if time.Now().After(deadline) {
			logs, _ := exec.Command("docker", "logs", "--tail", "5", kafka).CombinedOutput()
			stop()
			return "", "", nil, fmt.Errorf("the broker did not become ready (%v)\n%s", last, logs)
		}
	}
}

// brokerJar is the name of the broker's own jar in a container (kafka_2.13-4.3.1.jar),
// wherever the image keeps it.
func brokerJar(container string) string {
	out, _ := exec.Command("docker", "exec", container, "sh", "-c",
		"ls /opt/kafka/libs /usr/share/java/kafka 2>/dev/null | grep -E '^kafka_[0-9.]+-[0-9].*[0-9a-z]\\.jar$' | grep -v -e sources -e test -e javadoc -e scaladoc | head -1").Output()
	return strings.TrimSpace(string(out))
}

// The committed file names a broker for every profile, each with APIs, and the one
// for the newest major is the image every other recording is of.
func TestApiVersionsRecordingCoversEveryMajor(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(recordingsDir, apiVersionsFile))
	if err != nil {
		t.Fatalf("%v (go tool task conformance:record -- -only=api-versions)", err)
	}
	var rec APIVersionsRecording
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	if len(rec.Brokers) != len(versionBrokers) {
		t.Fatalf("the recording has %d brokers, the recorder lists %d: record it again", len(rec.Brokers), len(versionBrokers))
	}
	for i, b := range versionBrokers {
		got := rec.Brokers[i]
		if got.Major != b.Major || got.Image != b.Image {
			t.Errorf("broker %d is Kafka %d from %s, the recorder lists Kafka %d from %s: record it again", i, got.Major, got.Image, b.Major, b.Image)
		}
		if len(got.APIs) < 30 || got.Jar == "" {
			t.Errorf("Kafka %d: %d APIs and jar %q: not an ApiVersions answer of a broker", b.Major, len(got.APIs), got.Jar)
		}
	}
}
