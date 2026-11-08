package schemaregistry

import (
	"strings"
	"testing"
)

func TestInlineAvroForADraft(t *testing.T) {
	key := `{"type":"record","name":"OrderKey","namespace":"harbour.checkout","fields":[{"name":"orderId","type":"string"}]}`
	draft := `{"type":"record","name":"Order","namespace":"harbour.checkout","fields":[{"name":"key","type":["null","harbour.checkout.OrderKey"],"default":null}]}`
	got, err := InlineAvro(draft, []Ref{{Name: "harbour.checkout.OrderKey", Type: TypeAvro, Text: key}})
	if err != nil || !strings.Contains(got, `"name":"harbour.checkout.OrderKey"`) || !strings.Contains(got, `"orderId"`) {
		t.Fatalf("%s %v", got, err)
	}
	if _, err := InlineAvro(draft, []Ref{{Name: "x", Text: "{nope"}}); err == nil {
		t.Error("a reference that does not parse")
	}
}
