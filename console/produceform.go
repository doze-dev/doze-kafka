package console

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// produceInput is a validated compose-panel submission.
type produceInput struct {
	Key, Value []byte // nil Key = null key; nil Value = tombstone.
	Headers    []MessageHeader
	Partition  int32
	Auto       bool // choose the partition from the key.
	Acks       string
}

// inputError is a message for the person at the form. It satisfies
// errors.Is(err, errBadInput) without the sentinel's own text in front of it.
type inputError struct{ msg string }

func (e *inputError) Error() string        { return e.msg }
func (e *inputError) Is(target error) bool { return target == errBadInput }

func badInput(format string, args ...any) error {
	return &inputError{fmt.Sprintf(format, args...)}
}

// decodeField turns a form field into bytes under its declared encoding.
func decodeField(name, raw, enc string) ([]byte, error) {
	switch enc {
	case "", "text":
		return []byte(raw), nil
	case "base64":
		b, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			// Be forgiving about the padding and the URL-safe alphabet people paste.
			trimmed := trimPadding(raw)
			if b2, err2 := base64.RawStdEncoding.DecodeString(trimmed); err2 == nil {
				return b2, nil
			}
			if b2, err2 := base64.RawURLEncoding.DecodeString(trimmed); err2 == nil {
				return b2, nil
			}
			return nil, badInput("the %s is not valid base64: %v", name, err)
		}
		return b, nil
	default:
		return nil, badInput("unknown encoding %q for the %s", enc, name)
	}
}

func trimPadding(s string) string {
	for len(s) > 0 && s[len(s)-1] == '=' {
		s = s[:len(s)-1]
	}
	return s
}

// parseProduceForm validates what the compose panel sent. Every problem is
// reported, where it used to be quietly coerced: bad header JSON dropped every
// header, and a partition that was not a number became partition 0.
func parseProduceForm(r *http.Request, partitions int32) (produceInput, error) {
	var in produceInput

	if k := r.FormValue("key"); k != "" {
		key, err := decodeField("key", k, r.FormValue("key_enc"))
		if err != nil {
			return in, err
		}
		in.Key = key
	}

	if r.FormValue("tombstone") == "1" {
		in.Value = nil // a null value: how a delete is expressed on a compacted topic.
	} else {
		v, err := decodeField("value", r.FormValue("value"), r.FormValue("value_enc"))
		if err != nil {
			return in, err
		}
		if v == nil {
			v = []byte{}
		}
		in.Value = v
	}

	if raw := r.FormValue("headers"); raw != "" {
		var rows []struct {
			K   string `json:"k"`
			V   string `json:"v"`
			B64 bool   `json:"b64"`
		}
		if err := json.Unmarshal([]byte(raw), &rows); err != nil {
			return in, badInput("the headers are not valid: %v", err)
		}
		for i, row := range rows {
			if row.K == "" {
				if row.V != "" {
					return in, badInput("header %d has a value but no key", i+1)
				}
				continue
			}
			enc := "text"
			if row.B64 {
				enc = "base64"
			}
			v, err := decodeField("header "+strconv.Quote(row.K), row.V, enc)
			if err != nil {
				return in, err
			}
			in.Headers = append(in.Headers, MessageHeader{Key: row.K, Value: v})
		}
	}

	switch p := r.FormValue("partition"); p {
	case "", "auto":
		in.Auto = true
	default:
		n, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return in, badInput("%q is not a partition number", p)
		}
		if n < 0 || n >= int64(partitions) { // compared before narrowing: 4294967296 is not partition 0.
			return in, badInput("partition %d does not exist; this topic has %d (0–%d)", n, partitions, partitions-1)
		}
		in.Partition = int32(n)
	}

	switch a := r.FormValue("acks"); a {
	case "", "1":
		in.Acks = "1"
	case "0", "all":
		in.Acks = a
	default:
		return in, badInput("acks must be 0, 1 or all, not %q", a)
	}
	return in, nil
}
