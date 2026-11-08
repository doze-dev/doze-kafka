package console

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
)

// On a one-partition topic there is nowhere else for a key to go, and the preview says
// so; on a topic with several the key's partition is named, and a keyless send is "in
// turn".
func TestThePreviewNamesTheOnlyPartitionOrWhereAKeyLands(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"solo"}, "partitions": {"1"}})
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"tri"}, "partitions": {"3"}})

	for _, form := range []url.Values{{"key": {"k"}, "value": {"x"}, "acks": {"1"}}, {"value": {"x"}, "acks": {"1"}}} {
		p := preview(t, c, "solo", form)
		if p.Partition != "p0, the only one" || p.Lands != "Acks 1." {
			t.Errorf("one partition, form %v: %q / %q", form, p.Partition, p.Lands)
		}
		if strings.Contains(p.PartNote, "lands on") {
			t.Errorf("one partition, form %v: the note names a landing: %q", form, p.PartNote)
		}
	}
	p := preview(t, c, "tri", url.Values{"value": {"x"}, "acks": {"1"}})
	if p.Partition != "in turn" || p.Lands != "Acks 1." {
		t.Errorf("no key on three partitions: %q / %q", p.Partition, p.Lands)
	}
	want := c.be.keyPartition("tri", []byte("k"), 3)
	p = preview(t, c, "tri", url.Values{"key": {"k"}, "value": {"x"}, "acks": {"1"}})
	if p.Partition != fmt.Sprintf("p%d, by key", want) || p.Lands != fmt.Sprintf("Lands on p%d by key. Acks 1.", want) {
		t.Errorf("a key on three partitions: %q / %q", p.Partition, p.Lands)
	}
}

// The note under the Partition control shows a short plain key as it is, and a long one
// by its size.
func TestThePartitionNoteShowsAKeyOnlyWhenItIsShort(t *testing.T) {
	c := newTestConsole(t)
	k24 := strings.Repeat("k", 24)
	if note := c.partitionNote(4, []byte(k24)); !strings.Contains(note, " "+k24+" lands on p") {
		t.Errorf("a key of 24 bytes: %q", note)
	}
	if note := c.partitionNote(4, []byte(k24+"k")); !strings.Contains(note, "this key (25 B) lands on p") {
		t.Errorf("a key of 25 bytes: %q", note)
	}
	if note := c.partitionNote(1, []byte("k")); strings.Contains(note, "lands on") {
		t.Errorf("a topic of one partition: %q", note)
	}
}

// A base64 value cannot be checked against a subject, and the preview says why.
func TestAPreviewOfABase64ValueUnderASubjectSaysItIsSentAsRawBytes(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"raw"}, "partitions": {"1"}})
	p := preview(t, c, "raw", url.Values{"value": {"aGk="}, "value_enc": {"base64"}, "schema_subject": {"orders-value"}})
	want := "A base64 value is sent as raw bytes; untick b64 to write JSON for orders-value."
	if p.Schema == nil || p.Schema.OK || p.Schema.Message != want {
		t.Errorf("the schema answer: %+v", p.Schema)
	}
}

// A record schema's fields are listed up to twelve of them, and not beyond.
func TestARecordSchemaOfTwelveFieldsIsSummarisedAndOfThirteenIsNot(t *testing.T) {
	record := func(n int) schemaVersion {
		fields := make([]string, n)
		for i := range fields {
			fields[i] = fmt.Sprintf(`{"name":"f%d","type":"int"}`, i)
		}
		return schemaVersion{Schema: `{"type":"record","name":"R","fields":[` + strings.Join(fields, ",") + `]}`}
	}
	if got := avroFields(record(12)); !strings.HasPrefix(got, "f0 int, f1 int") || !strings.HasSuffix(got, "f11 int") {
		t.Errorf("twelve fields: %q", got)
	}
	if got := avroFields(record(13)); got != "" {
		t.Errorf("thirteen fields: %q", got)
	}
}
