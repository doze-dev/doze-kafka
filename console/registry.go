package console

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
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
	// While the registry cannot be reached, no id is asked of it until downUntil;
	// each failed probe doubles the wait (from downFirst to downMost), any answer ends it.
	downUntil time.Time
	downFor   time.Duration
	downErr   error
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

func newRegistryClient(base, user, pass string, tlsCfg *tls.Config) *registryClient {
	if base == "" {
		return nil
	}
	hc := &http.Client{Timeout: 10 * time.Second}
	if tlsCfg != nil {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.TLSClientConfig = tlsCfg
		hc.Transport = tr
	}
	return &registryClient{
		base: strings.TrimRight(base, "/"), user: user, pass: pass,
		hc:     hc,
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
	// The console's own reads: doze-kafka's registry answers them and leaves them out of
	// its request log, so the pages' polling does not push out what clients asked
	// (schemaregistry/requests.go). What a person does here (register, check, delete, a
	// level set) is a request like any client's and is kept: the dock shows it. Another
	// registry ignores the header.
	if method == http.MethodGet {
		req.Header.Set("X-Doze-Own", "1")
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
	ID                    int // the latest version's schema id.
	Versions              int // how many versions it has.
}

// TypeName is the language as the home table writes it (Sc-Home): AVRO, PROTOBUF, JSON.
func (r SubjectRow) TypeName() string {
	switch r.Type {
	case schemaregistry.TypeJSON:
		return "JSON"
	case schemaregistry.TypeProtobuf:
		return "PROTOBUF"
	}
	return "AVRO"
}

// TypeShort is the language as the list's pill writes it: AVRO, PROTO, JSON.
func (r SubjectRow) TypeShort() string {
	switch r.Type {
	case schemaregistry.TypeJSON:
		return "JSON"
	case schemaregistry.TypeProtobuf:
		return "PROTO"
	}
	return "AVRO"
}

func (rc *registryClient) Subjects(ctx context.Context) ([]SubjectRow, error) {
	rows, _, err := rc.Catalogue(ctx)
	return rows, err
}

// Catalogue is every subject with its latest version and how many versions it has,
// and the schema id of every version: one request (GET /schemas).
func (rc *registryClient) Catalogue(ctx context.Context) ([]SubjectRow, []int, error) {
	var all []schemaVersion
	if err := rc.call(ctx, "GET", "/schemas", nil, &all); err != nil {
		return nil, nil, err
	}
	at := map[string]int{}
	var out []SubjectRow
	var ids []int
	for _, v := range all {
		ids = append(ids, v.ID)
		i, ok := at[v.Subject]
		if !ok {
			at[v.Subject] = len(out)
			out = append(out, SubjectRow{Name: v.Subject})
			i = len(out) - 1
		}
		out[i].Versions++
		if v.Version > out[i].Latest {
			out[i].Latest, out[i].ID, out[i].Type, out[i].TypeLabel = v.Version, v.ID, v.Type(), v.TypeLabel()
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	sort.Ints(ids)
	return out, ids, nil
}

// Mode is the registry's global mode (READWRITE, READONLY, IMPORT).
func (rc *registryClient) Mode(ctx context.Context) (string, error) {
	var m struct {
		Mode string `json:"mode"`
	}
	if err := rc.call(ctx, "GET", "/mode", nil, &m); err != nil {
		return "", err
	}
	return m.Mode, nil
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

// GlobalCompat is the registry's default compatibility level, which a subject
// without its own level follows.
func (rc *registryClient) GlobalCompat(ctx context.Context) (string, error) {
	var out struct {
		Level string `json:"compatibilityLevel"`
	}
	err := rc.call(ctx, "GET", "/config", nil, &out)
	return out.Level, err
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
//
// An id the registry answered for is kept (its schema never changes); one it does not
// hold, or could not read, is asked again after unknownFor. While the registry cannot be
// reached at all, no id is asked: one probe, then a wait that doubles (SCH-24), so a
// table of a thousand rows is not a thousand requests to a registry that is down.
func (rc *registryClient) codecFor(ctx context.Context, id int, topic string) (*codecEntry, error) {
	rc.mu.Lock()
	if e, ok := rc.codecs[id]; ok && (e.err == nil || time.Since(e.at) < unknownFor) {
		rc.mu.Unlock()
		return e, e.err
	}
	if time.Now().Before(rc.downUntil) {
		err := rc.downErr
		rc.mu.Unlock()
		return &codecEntry{err: err, at: time.Now()}, err
	}
	rc.mu.Unlock()

	e := &codecEntry{at: time.Now()}
	e.codec, e.subject, e.version, e.err = rc.buildCodec(ctx, id, topic)
	rc.mu.Lock()
	defer rc.mu.Unlock()
	var re *regError
	if e.err != nil && !errors.As(e.err, &re) {
		// Not an answer from the registry: it is down or unreachable. Wait before the
		// next probe, and do not remember the id as unknown.
		rc.downFor = min(max(2*rc.downFor, downFirst), downMost)
		rc.downUntil, rc.downErr = time.Now().Add(rc.downFor), e.err
		return e, e.err
	}
	rc.downFor, rc.downUntil, rc.downErr = 0, time.Time{}, nil
	if len(rc.codecs) >= maxCodecs {
		// Full: the entry asked longest ago goes.
		oldest, at := 0, time.Time{}
		for k, v := range rc.codecs {
			if at.IsZero() || v.at.Before(at) {
				oldest, at = k, v.at
			}
		}
		delete(rc.codecs, oldest)
	}
	rc.codecs[id] = e
	return e, e.err
}

const (
	maxCodecs  = 512
	unknownFor = 30 * time.Second
	downFirst  = time.Second
	downMost   = 30 * time.Second
)

func (rc *registryClient) buildCodec(ctx context.Context, id int, topic string) (*schemaregistry.Codec, string, int, error) {
	var s schemaVersion
	if err := rc.call(ctx, "GET", fmt.Sprintf("/schemas/ids/%d", id), nil, &s); err != nil {
		return nil, "", 0, err
	}
	refs, err := rc.resolve(ctx, s)
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

// resolve follows a schema's references to their text, dependencies first. Each
// (subject, version) is fetched and followed once, so a schema that names one many
// times (a diamond) costs what it names and not its paths, and the bound is the
// registry's own: at most maxResolved distinct schemas.
func (rc *registryClient) resolve(ctx context.Context, s schemaVersion) ([]schemaregistry.Ref, error) {
	var out []schemaregistry.Ref
	followed := map[string]bool{}
	seen := map[string]bool{}
	fetched := map[string]schemaVersion{}
	var walk func(s schemaVersion) error
	walk = func(s schemaVersion) error {
		for _, r := range s.References {
			node := fmt.Sprintf("%s\x00%d", r.Subject, r.Version)
			dep, ok := fetched[node]
			if !ok {
				var err error
				if dep, err = rc.Version(ctx, r.Subject, r.Version); err != nil {
					return err
				}
				fetched[node] = dep
			}
			if !followed[node] {
				if len(followed) >= maxResolved {
					return fmt.Errorf("schema references name more than %d schemas", maxResolved)
				}
				followed[node] = true
				if err := walk(dep); err != nil {
					return err
				}
			}
			if key := node + "\x00" + r.Name; !seen[key] {
				seen[key] = true
				out = append(out, schemaregistry.Ref{Name: r.Name, Type: dep.Type(), Text: dep.Schema})
			}
		}
		return nil
	}
	if err := walk(s); err != nil {
		return nil, err
	}
	return out, nil
}

// maxResolved is the registry's bound on the schemas one graph of references names.
const maxResolved = 1000

// decoded is a record value read through the registry.
type decoded struct {
	JSON    string
	Type    string
	TypeLbl string
	ID      int
	Subject string
	Version int
}

// framing says what became of a value the registry was asked to read (TOP-38).
type framing int

const (
	notFramed      framing = iota // the first byte is not the registry's 0x00, or there are fewer than five bytes.
	framedDecoded                 // framed, the id is registered, the body decoded with its schema.
	framedUnknown                 // framed with an id the registry does not have.
	framedUnread                  // framed, but the registry could not be asked (it is down, or refused).
	framedMismatch                // framed with a known id, but the body does not decode with that schema.
)

// decode reads a value written by a registry-aware serializer. ok is false for
// anything else (a plain string, a binary of another kind, an id the registry does not
// know), which is then shown as it always was.
//
//fuzz:not-a-parser looks up the codec for the id and hands the value to schemaregistry.WireID and Codec.Decode, which parse it and are fuzzed there (FuzzDecode)
func (rc *registryClient) decode(ctx context.Context, topic string, value []byte) (decoded, bool) {
	d, state, _ := rc.decodeState(ctx, topic, value)
	return d, state == framedDecoded
}

// decodeState is decode that says why a value was not decoded: it was not framed at all,
// or it was framed with an id the registry does not hold, or the registry could not be
// asked, or the body does not fit the schema the id names. The error is the reason in
// the registry's or the codec's own words.
//
//fuzz:not-a-parser see decode
func (rc *registryClient) decodeState(ctx context.Context, topic string, value []byte) (decoded, framing, error) {
	id, ok := schemaregistry.WireID(value)
	if !ok {
		return decoded{}, notFramed, nil
	}
	e, err := rc.codecFor(ctx, id, topic)
	if err != nil {
		var re *regError
		if errors.As(err, &re) && re.Status == http.StatusNotFound {
			return decoded{ID: id}, framedUnknown, err
		}
		return decoded{ID: id}, framedUnread, err
	}
	j, err := e.codec.Decode(value)
	if err != nil {
		return decoded{ID: id, Subject: e.subject, Version: e.version, Type: e.codec.Type(), TypeLbl: typeLabel(e.codec.Type())}, framedMismatch, err
	}
	return decoded{JSON: j, Type: e.codec.Type(), TypeLbl: typeLabel(e.codec.Type()), ID: id, Subject: e.subject, Version: e.version}, framedDecoded, nil
}

// encode writes JSON text as a record value under the latest version of a subject.
func (rc *registryClient) encode(ctx context.Context, subject, doc string) ([]byte, error) {
	v, err := rc.Version(ctx, subject, 0)
	if err != nil {
		return nil, err
	}
	refs, err := rc.resolve(ctx, v)
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

// regRequests is a page of the registry's request log (GET /_doze/requests), newest
// first: doze-kafka's registry keeps it; another registry has no such route.
type regRequests struct {
	Requests []struct {
		Seq    uint64    `json:"seq"`
		At     time.Time `json:"at"`
		Method string    `json:"method"`
		Path   string    `json:"path"`
		Status int       `json:"status"`
		Took   int64     `json:"tookNanos"`
		User   string    `json:"user"`
		From   string    `json:"from"`
		Agent  string    `json:"agent"`
	} `json:"requests"`
	Next uint64 `json:"next"`
}

// Requests reads the registry's request log: all of it, or the requests about one
// subject; before is the cursor (0 for the newest).
func (rc *registryClient) Requests(ctx context.Context, subject string, before uint64, limit int) (regRequests, error) {
	q := url.Values{"limit": {strconv.Itoa(limit)}}
	if subject != "" {
		q.Set("subject", subject)
	}
	if before != 0 {
		q.Set("before", strconv.FormatUint(before, 10))
	}
	var out regRequests
	err := rc.call(ctx, http.MethodGet, "/_doze/requests?"+q.Encode(), nil, &out)
	return out, err
}
