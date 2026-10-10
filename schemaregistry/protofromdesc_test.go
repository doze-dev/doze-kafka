package schemaregistry

import (
	"encoding/json"
	"os"
	"testing"
)

// confluent-kafka for Python registers a Protobuf message as the base64 of its
// FileDescriptorProto, and Confluent's registry accepts that. These descriptors were
// registered with a real Confluent Schema Registry 7.7.1 and the schema text it stored
// recorded (testdata/conformance/protodesc.json): the same string must come out here, so
// the same message sent either way is one schema with one id.
func TestADescriptorIsStoredAsConfluentStoresIt(t *testing.T) {
	raw, err := os.ReadFile("testdata/conformance/protodesc.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name       string `json:"name"`
		Descriptor string `json:"descriptor"`
		Canonical  string `json:"canonical"`
		Error      string `json:"error"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) < 5 {
		t.Fatalf("%d recorded cases", len(cases))
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			got, err := parseProtobuf(c.Descriptor, nil)
			if c.Error != "" {
				if err == nil {
					t.Errorf("Confluent refused it (%s) and so should we", c.Error)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused a descriptor Confluent accepts: %v", err)
			}
			if got.Canonical(false) != c.Canonical {
				t.Errorf("stored differently\n--- Confluent\n%s\n--- here\n%s", c.Canonical, got.Canonical(false))
			}
		})
	}
}

// Garbage is still an error, and a descriptor is only read as one when the text does not parse.
func TestNotAProtobufSchemaAtAll(t *testing.T) {
	for _, bad := range []string{"not a schema", "bm90IGEgZGVzY3JpcHRvcg==", "AAAA"} {
		if _, err := parseProtobuf(bad, nil); err == nil {
			t.Errorf("%q was accepted as a Protobuf schema", bad)
		}
	}
}
