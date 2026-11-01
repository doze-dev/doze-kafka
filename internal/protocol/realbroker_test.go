package protocol

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// realBrokers is conformance/recordings/api-versions.json: the ApiVersions answer of
// a real broker of each Kafka major there is a profile for (which image stands for
// which major, and why, is in conformance/apiversions_test.go, which records it).
type realBrokers struct {
	Brokers []struct {
		Major int    `json:"major"`
		Image string `json:"image"`
		APIs  []struct {
			Key int16 `json:"key"`
			Min int16 `json:"min"`
			Max int16 `json:"max"`
		} `json:"apis"`
	} `json:"brokers"`
}

// notListedByARealBroker are the rows of the table a real broker of that major does
// not list in ApiVersions, each with why it is offered all the same. An entry that
// the recording does list fails the test, so the list cannot go stale.
var notListedByARealBroker = map[int]map[APIKey]string{
	4: {
		// claim: traffic R5 (conformance/recordings/client-telemetry.json).
		kmsg.GetTelemetrySubscriptions: "Kafka lists the two telemetry APIs only when a broker plugin implementing ClientTelemetry is configured, and answers them when sent either way; here they are listed unless --client-telemetry=false (docs/limitations.md)",
		kmsg.PushTelemetry:             "as GetTelemetrySubscriptions",
	},
}

// A profile is the Kafka major it is named for, less what this broker does not
// implement: it may offer fewer APIs and narrower ranges than the real broker, and
// never an API or a version the real broker does not serve. A client that
// negotiated such a version would be using something no Kafka of that major
// answers (Kafka 4 closes the connection: recorded, versions-not-served), and this
// broker would be more forgiving than the one it stands in for.
//
// Held against the recorded ApiVersions answer of a real broker of each major, so
// the table cannot drift from it: before this the Kafka 4 column offered nine
// versions Kafka 4 removed (KIP-896), and the Kafka 1 column six that 1.1.1 never
// had.
func TestNoProfileServesWhatARealBrokerOfThatMajorDoesNot(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "conformance", "recordings", "api-versions.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rec realBrokers
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	real := map[int]map[APIKey]VersionRange{}
	image := map[int]string{}
	for _, b := range rec.Brokers {
		real[b.Major] = map[APIKey]VersionRange{}
		image[b.Major] = b.Image
		for _, a := range b.APIs {
			real[b.Major][APIKey(a.Key)] = VersionRange{Min: a.Min, Max: a.Max}
		}
	}
	for _, major := range SupportedKafkaMajors() {
		listed, ok := real[major]
		if !ok || len(listed) == 0 {
			t.Errorf("Kafka %d: no recorded ApiVersions of a real broker (go tool task conformance:record -- -only=api-versions)", major)
			continue
		}
		p, _ := ProfileFor(major)
		excused := notListedByARealBroker[major]
		for _, a := range APIs {
			ours, offered := p.RangeFor(a.Key)
			if !offered {
				continue
			}
			theirs, there := listed[a.Key]
			reason, isExcused := excused[a.Key]
			switch {
			case !there && !isExcused:
				t.Errorf("Kafka %d: %s is offered, and %s does not list it", major, APIName(a.Key), image[major])
			case !there:
				if reason == "" {
					t.Errorf("Kafka %d: %s is excused with no reason", major, APIName(a.Key))
				}
			case isExcused:
				t.Errorf("Kafka %d: %s is listed by %s (v%d–%d) and still excused as not listed: remove it from notListedByARealBroker", major, APIName(a.Key), image[major], theirs.Min, theirs.Max)
			default:
				if ours.Min < theirs.Min {
					t.Errorf("Kafka %d: %s is offered from v%d, and %s serves it from v%d: v%d–%d are versions that broker closes the connection on",
						major, APIName(a.Key), ours.Min, image[major], theirs.Min, ours.Min, theirs.Min-1)
				}
				if ours.Max > theirs.Max {
					t.Errorf("Kafka %d: %s is offered up to v%d, and %s serves it up to v%d", major, APIName(a.Key), ours.Max, image[major], theirs.Max)
				}
			}
		}
		for key := range excused {
			if _, offered := p.RangeFor(key); !offered {
				t.Errorf("Kafka %d: %s is excused and not offered: remove it from notListedByARealBroker", major, APIName(key))
			}
		}
	}
}
