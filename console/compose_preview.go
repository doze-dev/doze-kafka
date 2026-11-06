package console

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// Tp-10 and Tp-11: "What will be sent". The composer asks, as it is typed in, what the
// send would do: which partition the key lands on (the real partitioner, not a copy of
// it in the page), what each part weighs, what the batch weighs, and, for a value
// written as JSON under a registry subject, whether the schema accepts it and the bytes
// it becomes. Nothing is written.

// composePreview is the answer.
type composePreview struct {
	// Error is the form's own complaint (bad base64, a header with no key), as it would be
	// said on Send.
	Error string `json:"error,omitempty"`

	Partition string         `json:"partition"` // "p1, by key", "p0, fixed", "in turn".
	PartNote  string         `json:"partNote"`  // the sentence under the Partition control.
	Key       string         `json:"key"`       // "14 B", "empty", "none".
	Value     string         `json:"value"`     // "69 B", "null", "none".
	Headers   string         `json:"headers"`   // "2 · 84 B", "none".
	Batch     string         `json:"batch"`     // "1 record, 220 B".
	Lands     string         `json:"lands"`     // the foot sentence for Tp-11: "Lands on p1 by key. Acks 1."
	Schema    *schemaPreview `json:"schema,omitempty"`
}

// schemaPreview is the registry's answer to a value written for a subject.
type schemaPreview struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"` // "Valid against orders-value v3: orderId string, …" or the refusal.
	ID      int    `json:"id,omitempty"`
	Version int    `json:"version,omitempty"`
	Type    string `json:"type,omitempty"`
	Size    int    `json:"size,omitempty"`  // bytes on disk.
	Body    int    `json:"body,omitempty"`  // bytes after the 5-byte framing.
	Hex     string `json:"hex,omitempty"`   // the dump of the encoded value.
	Bytes   string `json:"bytes,omitempty"` // "00 00 00 00 03", the framing's id bytes.
}

func (c *Console) topicComposePreview(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	_, count, err := c.be.TopicShape(r.Context(), name)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	out := composePreview{}
	in, perr := parseProduceForm(r, count)
	if perr != nil {
		out.Error = perr.Error()
		writeJSON(w, out)
		return
	}
	value := in.Value
	if subject := strings.TrimSpace(r.FormValue("schema_subject")); subject != "" && value != nil && c.reg != nil {
		sp := &schemaPreview{}
		out.Schema = sp
		switch {
		case r.FormValue("value_enc") == "base64":
			sp.Message = "A base64 value is sent as raw bytes; untick b64 to write JSON for " + subject + "."
		case strings.TrimSpace(string(value)) == "":
			sp.Message = "Write the value as JSON."
		default:
			v, verr := c.reg.Version(r.Context(), subject, 0)
			if verr != nil {
				sp.Message = refusalFor(verr).Message
				break
			}
			enc, eerr := c.reg.encode(r.Context(), subject, string(value))
			if eerr != nil {
				sp.Message = refusalFor(eerr).Message
				break
			}
			dump, _ := hexDump(enc, hexLimit)
			sp.OK, sp.ID, sp.Version, sp.Type, sp.Size, sp.Body, sp.Hex = true, v.ID, v.Version, v.TypeLabel(), len(enc), max(len(enc)-5, 0), dump
			sp.Bytes = fmt.Sprintf("%02x %02x %02x %02x", enc[1], enc[2], enc[3], enc[4])
			sp.Message = fmt.Sprintf("Valid against %s v%d", subject, v.Version)
			if f := avroFields(v); f != "" {
				sp.Message += ": " + f
			}
			sp.Message += "."
			value = enc
		}
	}

	part := in.Partition
	switch {
	case !in.Auto:
		out.Partition = fmt.Sprintf("p%d, fixed", part)
	case in.Key != nil && count > 1:
		part = c.be.keyPartition(name, in.Key, count)
		out.Partition = fmt.Sprintf("p%d, by key", part)
	case count <= 1:
		out.Partition = "p0, the only one"
	default:
		out.Partition = "in turn"
	}
	out.PartNote = c.partitionNote(count, in.Key)
	out.Key = sizeWord(in.Key, "none", "empty")
	out.Value = sizeWord(value, "none", "empty")
	if in.Value == nil {
		out.Value = "null, a tombstone"
	}
	if len(in.Headers) == 0 {
		out.Headers = "none"
	} else {
		out.Headers = fmt.Sprintf("%d · %d B", len(in.Headers), headersBytes(in.Headers))
	}
	out.Batch = fmt.Sprintf("1 record, %s B", groupDigits(int64(batchSize(in.Key, value, in.Headers))))
	switch {
	case !in.Auto:
		out.Lands = fmt.Sprintf("Lands on p%d. Acks %s.", part, in.Acks)
	case in.Key != nil && count > 1:
		out.Lands = fmt.Sprintf("Lands on p%d by key. Acks %s.", part, in.Acks)
	default:
		out.Lands = fmt.Sprintf("Acks %s.", in.Acks)
	}
	writeJSON(w, out)
}

// partitionNote is the sentence under the Partition control. With a key it names the
// real partition; it says what the rule is, from the partitioner the console uses.
func (c *Console) partitionNote(count int32, key []byte) string {
	s := fmt.Sprintf("With a key, the partition is murmur2 of the key bytes modulo %d, the same as the Java client.", count)
	if key != nil && count > 1 {
		shown := string(key)
		if len(shown) > 24 || !isPlain(key) {
			shown = fmt.Sprintf("this key (%d B)", len(key))
		}
		s += fmt.Sprintf(" %s lands on p%d.", shown, c.be.keyPartition("", key, count))
	}
	return s + " With no key, the console spreads records across the partitions in turn."
}

func isPlain(b []byte) bool { return !looksBinary(b) }

func sizeWord(b []byte, none, empty string) string {
	switch {
	case b == nil:
		return none
	case len(b) == 0:
		return empty
	}
	return fmt.Sprintf("%s B", groupDigits(int64(len(b))))
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

// avroFields summarises a record schema's fields as "orderId string, lines int", for the
// line that says the value fits. Anything it cannot summarise plainly it leaves out.
func avroFields(v schemaVersion) string {
	if v.Type() != "AVRO" {
		return ""
	}
	var s struct {
		Type   string `json:"type"`
		Fields []struct {
			Name string          `json:"name"`
			Type json.RawMessage `json:"type"`
		} `json:"fields"`
	}
	if json.Unmarshal([]byte(v.Schema), &s) != nil || s.Type != "record" || len(s.Fields) == 0 || len(s.Fields) > 12 {
		return ""
	}
	parts := make([]string, 0, len(s.Fields))
	for _, f := range s.Fields {
		var t string
		if json.Unmarshal(f.Type, &t) != nil {
			t = "…"
		}
		parts = append(parts, f.Name+" "+t)
	}
	return strings.Join(parts, ", ")
}
