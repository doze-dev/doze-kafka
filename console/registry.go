package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

// The console talks to the schema registry the way any client does: over its REST
// API. It shares nothing with the registry's code but the payload codec, so it works
// the same against a registry in another process (or another vendor's, as long as it
// speaks Confluent's API).

// regError is an answer from the registry that was not success.
type regError struct {
	Status  int
	Code    int
	Message string
}

func (e *regError) Error() string { return e.Message }

// Is sorts a registry refusal into the console's own kinds: a missing thing is a 404
// page, and a schema the registry refused is something the person can fix.
func (e *regError) Is(target error) bool {
	switch target {
	case errNotFound:
		return e.Status == http.StatusNotFound
	case errBadInput:
		return e.Status == http.StatusConflict || e.Status == http.StatusUnprocessableEntity || e.Status == http.StatusBadRequest
	}
	return false
}

type registryClient struct {
	base, user, pass string
	hc               *http.Client

	mu     sync.Mutex
	codecs map[int]*codecEntry
}

// codecEntry is what is known about one schema id for decoding payloads: its codec,
// or why there is none (cached briefly, so a registry that is down is not asked again
// for every row).
type codecEntry struct {
	codec   *schemaregistry.Codec
	subject string
	version int
	err     error
	at      time.Time
}

func newRegistryClient(base, user, pass string) *registryClient {
	if base == "" {
		return nil
	}
	return &registryClient{
		base: strings.TrimRight(base, "/"), user: user, pass: pass,
		hc:     &http.Client{Timeout: 10 * time.Second},
		codecs: map[int]*codecEntry{},
	}
}

func (rc *registryClient) call(ctx context.Context, method, path string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, rc.base+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	if rc.user != "" {
		req.SetBasicAuth(rc.user, rc.pass)
	}
	resp, err := rc.hc.Do(req)
	if err != nil {
		return fmt.Errorf("the schema registry is not reachable: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode >= 300 {
		re := &regError{Status: resp.StatusCode, Message: strings.TrimSpace(string(data))}
		var e struct {
			Code    int    `json:"error_code"`
			Message string `json:"message"`
		}
		if json.Unmarshal(data, &e) == nil && e.Message != "" {
			re.Code, re.Message = e.Code, e.Message
		}
		return re
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

func segment(s string) string { return url.PathEscape(s) }

// schemaVersion is one registered version of a subject.
type schemaVersion struct {
	Subject    string `json:"subject"`
	Version    int    `json:"version"`
	ID         int    `json:"id"`
	SchemaType string `json:"schemaType"`
	Schema     string `json:"schema"`
	References []struct {
		Name    string `json:"name"`
		Subject string `json:"subject"`
		Version int    `json:"version"`
	} `json:"references"`
}

// Type is the schema language, Avro when the registry leaves it out.
func (v schemaVersion) Type() string {
	if v.SchemaType == "" {
		return schemaregistry.TypeAvro
	}
	return v.SchemaType
}

// Pretty is the schema as shown: JSON re-indented, Protobuf as it is.
func (v schemaVersion) Pretty() string {
	if v.Type() != schemaregistry.TypeProtobuf {
		if p, ok := indentJSON([]byte(v.Schema)); ok {
			return p
		}
	}
	return strings.TrimRight(v.Schema, "\n")
}

// TypeLabel is the language in words.
func (v schemaVersion) TypeLabel() string { return typeLabel(v.Type()) }

func typeLabel(t string) string {
	switch t {
	case schemaregistry.TypeJSON:
		return "JSON Schema"
	case schemaregistry.TypeProtobuf:
		return "Protobuf"
	}
	return "Avro"
}

// SubjectRow is a subject in the list.
type SubjectRow struct {
	Name, Type, TypeLabel string
	Latest                int
}

func (rc *registryClient) Subjects(ctx context.Context) ([]SubjectRow, error) {
	var latest []schemaVersion
	if err := rc.call(ctx, "GET", "/schemas?latestOnly=true", nil, &latest); err != nil {
		return nil, err
	}
	out := make([]SubjectRow, 0, len(latest))
	for _, v := range latest {
		out = append(out, SubjectRow{Name: v.Subject, Type: v.Type(), TypeLabel: v.TypeLabel(), Latest: v.Version})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (rc *registryClient) Version(ctx context.Context, subject string, version int) (schemaVersion, error) {
	var v schemaVersion
	n := "latest"
	if version > 0 {
		n = fmt.Sprint(version)
	}
	err := rc.call(ctx, "GET", "/subjects/"+segment(subject)+"/versions/"+n, nil, &v)
	return v, err
}

func (rc *registryClient) Versions(ctx context.Context, subject string) ([]int, error) {
	var vs []int
	err := rc.call(ctx, "GET", "/subjects/"+segment(subject)+"/versions", nil, &vs)
	return vs, err
}

// Compat is a subject's compatibility level, and whether it is the subject's own.
func (rc *registryClient) Compat(ctx context.Context, subject string) (level string, own bool, err error) {
	var out struct {
		Level string `json:"compatibilityLevel"`
	}
	err = rc.call(ctx, "GET", "/config/"+segment(subject), nil, &out)
	if err == nil {
		return out.Level, true, nil
	}
	var re *regError
	if errors.As(err, &re) && re.Code == 40408 {
		err = rc.call(ctx, "GET", "/config", nil, &out)
		return out.Level, false, err
	}
	return "", false, err
}

func (rc *registryClient) SetCompat(ctx context.Context, subject, level string) error {
	return rc.call(ctx, "PUT", "/config/"+segment(subject), map[string]string{"compatibility": level}, nil)
}

func (rc *registryClient) ClearCompat(ctx context.Context, subject string) error {
	return rc.call(ctx, "DELETE", "/config/"+segment(subject), nil, nil)
}

type schemaBody struct {
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType,omitempty"`
	References any    `json:"references,omitempty"`
}

// Check asks whether a schema may follow a subject's versions, and why not.
func (rc *registryClient) Check(ctx context.Context, subject string, b schemaBody) (ok bool, reasons []string, err error) {
	var out struct {
		Compatible bool     `json:"is_compatible"`
		Messages   []string `json:"messages"`
	}
	err = rc.call(ctx, "POST", "/compatibility/subjects/"+segment(subject)+"/versions?verbose=true", b, &out)
	return out.Compatible, out.Messages, err
}

func (rc *registryClient) Register(ctx context.Context, subject string, b schemaBody) (id int, err error) {
	var out struct {
		ID int `json:"id"`
	}
	err = rc.call(ctx, "POST", "/subjects/"+segment(subject)+"/versions", b, &out)
	return out.ID, err
}

func (rc *registryClient) DeleteVersion(ctx context.Context, subject string, version int) error {
	p := "/subjects/" + segment(subject) + "/versions/" + fmt.Sprint(version)
	if err := rc.call(ctx, "DELETE", p, nil, nil); err != nil {
		return err
	}
	return rc.call(ctx, "DELETE", p+"?permanent=true", nil, nil)
}

func (rc *registryClient) DeleteSubject(ctx context.Context, subject string) error {
	p := "/subjects/" + segment(subject)
	if err := rc.call(ctx, "DELETE", p, nil, nil); err != nil {
		return err
	}
	return rc.call(ctx, "DELETE", p+"?permanent=true", nil, nil)
}

// SubjectNames lists the subjects, for choosing one to encode with.
func (rc *registryClient) SubjectNames(ctx context.Context) []string {
	var names []string
	if err := rc.call(ctx, "GET", "/subjects", nil, &names); err != nil {
		return nil
	}
	sort.Strings(names)
	return names
}

// codecFor prepares the decoder for a schema id, with the subject and version that
// hold it (the one named like the topic when there is a choice).
func (rc *registryClient) codecFor(ctx context.Context, id int, topic string) (*codecEntry, error) {
	rc.mu.Lock()
	if e, ok := rc.codecs[id]; ok && (e.err == nil || time.Since(e.at) < 5*time.Second) {
		rc.mu.Unlock()
		return e, e.err
	}
	rc.mu.Unlock()

	e := &codecEntry{at: time.Now()}
	e.codec, e.subject, e.version, e.err = rc.buildCodec(ctx, id, topic)
	rc.mu.Lock()
	if len(rc.codecs) > 512 {
		rc.codecs = map[int]*codecEntry{}
	}
	rc.codecs[id] = e
	rc.mu.Unlock()
	return e, e.err
}

func (rc *registryClient) buildCodec(ctx context.Context, id int, topic string) (*schemaregistry.Codec, string, int, error) {
	var s schemaVersion
	if err := rc.call(ctx, "GET", fmt.Sprintf("/schemas/ids/%d", id), nil, &s); err != nil {
		return nil, "", 0, err
	}
	refs, err := rc.resolve(ctx, s, 0)
	if err != nil {
		return nil, "", 0, err
	}
	codec, err := schemaregistry.NewCodec(s.Type(), s.Schema, refs)
	if err != nil {
		return nil, "", 0, err
	}
	var at []struct {
		Subject string `json:"subject"`
		Version int    `json:"version"`
	}
	subject, version := "", 0
	if rc.call(ctx, "GET", fmt.Sprintf("/schemas/ids/%d/versions", id), nil, &at) == nil && len(at) > 0 {
		subject, version = at[0].Subject, at[0].Version
		for _, a := range at {
			if topic != "" && (a.Subject == topic+"-value" || a.Subject == topic+"-key") {
				subject, version = a.Subject, a.Version
				break
			}
		}
	}
	return codec, subject, version, nil
}

// resolve follows a schema's references to their text, dependencies first.
func (rc *registryClient) resolve(ctx context.Context, s schemaVersion, depth int) ([]schemaregistry.Ref, error) {
	if depth > 16 {
		return nil, errors.New("schema references are nested too deeply")
	}
	var out []schemaregistry.Ref
	for _, r := range s.References {
		dep, err := rc.Version(ctx, r.Subject, r.Version)
		if err != nil {
			return nil, err
		}
		inner, err := rc.resolve(ctx, dep, depth+1)
		if err != nil {
			return nil, err
		}
		out = append(out, inner...)
		out = append(out, schemaregistry.Ref{Name: r.Name, Type: dep.Type(), Text: dep.Schema})
	}
	return out, nil
}

// decoded is a record value read through the registry.
type decoded struct {
	JSON    string
	Type    string
	TypeLbl string
	ID      int
	Subject string
	Version int
}

// decode reads a value written by a registry-aware serializer. ok is false for
// anything else (a plain string, a binary of another kind, an id the registry does not
// know), which is then shown as it always was.
func (rc *registryClient) decode(ctx context.Context, topic string, value []byte) (decoded, bool) {
	id, ok := schemaregistry.WireID(value)
	if !ok {
		return decoded{}, false
	}
	e, err := rc.codecFor(ctx, id, topic)
	if err != nil {
		return decoded{}, false
	}
	j, err := e.codec.Decode(value)
	if err != nil {
		return decoded{}, false
	}
	return decoded{JSON: j, Type: e.codec.Type(), TypeLbl: typeLabel(e.codec.Type()), ID: id, Subject: e.subject, Version: e.version}, true
}

// encode writes JSON text as a record value under the latest version of a subject.
func (rc *registryClient) encode(ctx context.Context, subject, doc string) ([]byte, error) {
	v, err := rc.Version(ctx, subject, 0)
	if err != nil {
		return nil, err
	}
	refs, err := rc.resolve(ctx, v, 0)
	if err != nil {
		return nil, err
	}
	codec, err := schemaregistry.NewCodec(v.Type(), v.Schema, refs)
	if err != nil {
		return nil, err
	}
	out, err := codec.Encode(v.ID, doc)
	if err != nil {
		return nil, badInput("the value does not fit %s version %d: %v", subject, v.Version, err)
	}
	return out, nil
}

// diffLine is one line of a comparison between two versions.
type diffLine struct {
	Op   string // "same", "add" or "del".
	Text string
}

// diffLines compares two texts line by line (the longest common run of lines is kept).
func diffLines(a, b string) []diffLine {
	x, y := strings.Split(a, "\n"), strings.Split(b, "\n")
	// Longest common subsequence over lines; schemas are short, the table is small.
	lcs := make([][]int, len(x)+1)
	for i := range lcs {
		lcs[i] = make([]int, len(y)+1)
	}
	for i := len(x) - 1; i >= 0; i-- {
		for j := len(y) - 1; j >= 0; j-- {
			if x[i] == y[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	var out []diffLine
	i, j := 0, 0
	for i < len(x) && j < len(y) {
		switch {
		case x[i] == y[j]:
			out = append(out, diffLine{"same", x[i]})
			i++
			j++
		case lcs[i+1][j] >= lcs[i][j+1]:
			out = append(out, diffLine{"del", x[i]})
			i++
		default:
			out = append(out, diffLine{"add", y[j]})
			j++
		}
	}
	for ; i < len(x); i++ {
		out = append(out, diffLine{"del", x[i]})
	}
	for ; j < len(y); j++ {
		out = append(out, diffLine{"add", y[j]})
	}
	return out
}
