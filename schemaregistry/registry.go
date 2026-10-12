package schemaregistry

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Storage is where the registry keeps its records: opaque keys and values, written
// atomically. The broker's metadata database provides it, so the registry survives a
// restart without a topic to replay (Confluent keeps its state in a _schemas topic;
// nothing on the wire observes the difference).
type Storage interface {
	LoadAll() (map[string][]byte, error)
	Apply(puts map[string][]byte, deletes []string) error
}

// Compatibility levels, as Confluent names them.
const (
	CompatNone               = "NONE"
	CompatBackward           = "BACKWARD"
	CompatBackwardTransitive = "BACKWARD_TRANSITIVE"
	CompatForward            = "FORWARD"
	CompatForwardTransitive  = "FORWARD_TRANSITIVE"
	CompatFull               = "FULL"
	CompatFullTransitive     = "FULL_TRANSITIVE"
)

// Modes. IMPORT and the per-subject forms beyond these two are not supported.
const (
	ModeReadWrite = "READWRITE"
	ModeReadOnly  = "READONLY"
)

var compatLevels = map[string]bool{
	CompatNone: true, CompatBackward: true, CompatBackwardTransitive: true, CompatForward: true,
	CompatForwardTransitive: true, CompatFull: true, CompatFullTransitive: true,
}

const defaultCompat = CompatBackward

// Records. The key spaces are prefixes; \x00 separates the parts of a key, since a
// subject name may contain anything else.
const (
	keySchema  = "schema\x00"  // schema\x00<id>             -> schemaRec
	keyVersion = "version\x00" // version\x00<subject>\x00<n> -> versionRec
	keyConfig  = "config\x00"  // config\x00<subject>         -> level ("" subject is the global one)
	keyMode    = "mode\x00"    // mode\x00<subject>           -> mode
	keyNextID  = "nextid"
)

type schemaRec struct {
	ID         int         `json:"id"`
	Type       string      `json:"type"`
	Schema     string      `json:"schema"`
	References []Reference `json:"references,omitempty"`
}

type versionRec struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	ID      int    `json:"id"`
	Deleted bool   `json:"deleted,omitempty"`
}

// Registry is the schema store and the rules around it. It is safe for concurrent use.
type Registry struct {
	mu      sync.Mutex
	st      Storage
	schemas map[int]*schemaRec
	byPrint map[string]int // fingerprint -> id.
	subj    map[string][]*versionRec
	compat  map[string]string // subject ("" = global) -> level.
	modes   map[string]string
	nextID  int
}

// Open loads the registry from storage.
func Open(st Storage) (*Registry, error) {
	r := &Registry{st: st, schemas: map[int]*schemaRec{}, byPrint: map[string]int{}, subj: map[string][]*versionRec{},
		compat: map[string]string{}, modes: map[string]string{}, nextID: 1}
	all, err := st.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("schema registry: load: %w", err)
	}
	for k, v := range all {
		switch {
		case strings.HasPrefix(k, keySchema):
			var s schemaRec
			if err := json.Unmarshal(v, &s); err != nil {
				return nil, fmt.Errorf("schema registry: record %q: %w", k, err)
			}
			r.schemas[s.ID] = &s
			r.byPrint[fingerprint(s.Type, s.Schema, s.References)] = s.ID
		case strings.HasPrefix(k, keyVersion):
			var vr versionRec
			if err := json.Unmarshal(v, &vr); err != nil {
				return nil, fmt.Errorf("schema registry: record %q: %w", k, err)
			}
			r.subj[vr.Subject] = append(r.subj[vr.Subject], &vr)
		case strings.HasPrefix(k, keyConfig):
			r.compat[strings.TrimPrefix(k, keyConfig)] = string(v)
		case strings.HasPrefix(k, keyMode):
			r.modes[strings.TrimPrefix(k, keyMode)] = string(v)
		case k == keyNextID:
			if n, err := strconv.Atoi(string(v)); err == nil {
				r.nextID = n
			}
		}
	}
	for _, vs := range r.subj {
		sort.Slice(vs, func(i, j int) bool { return vs[i].Version < vs[j].Version })
	}
	for id := range r.schemas {
		if id >= r.nextID {
			r.nextID = id + 1
		}
	}
	return r, nil
}

func fingerprint(typ, schema string, refs []Reference) string {
	h := sha256.New()
	h.Write([]byte(typ))
	h.Write([]byte{0})
	h.Write([]byte(schema))
	for _, r := range refs {
		fmt.Fprintf(h, "\x00%s\x00%s\x00%d", r.Name, r.Subject, r.Version)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func versionKey(subject string, v int) string {
	return fmt.Sprintf("%s%s\x00%010d", keyVersion, subject, v)
}

func schemaKey(id int) string { return fmt.Sprintf("%s%010d", keySchema, id) }

// batch collects the writes of one operation so they land together.
type batch struct {
	puts    map[string][]byte
	deletes []string
}

func newBatch() *batch { return &batch{puts: map[string][]byte{}} }

func (b *batch) put(key string, v any) {
	var raw []byte
	switch t := v.(type) {
	case string:
		raw = []byte(t)
	default:
		raw, _ = json.Marshal(v)
	}
	b.puts[key] = raw
}

func (b *batch) del(key string) { b.deletes = append(b.deletes, key) }

func (r *Registry) commit(b *batch) *Error {
	if len(b.puts) == 0 && len(b.deletes) == 0 {
		return nil
	}
	if err := r.st.Apply(b.puts, b.deletes); err != nil {
		return newErr(500, codeStore, "Error while committing to the schema store: %v", err)
	}
	return nil
}

// live returns a subject's versions that have not been deleted, oldest first.
func (r *Registry) live(subject string) []*versionRec {
	var out []*versionRec
	for _, v := range r.subj[subject] {
		if !v.Deleted {
			out = append(out, v)
		}
	}
	return out
}

// subjectExists is true when the subject has at least one version that is not deleted.
func (r *Registry) subjectExists(subject string) bool { return len(r.live(subject)) > 0 }

// SchemaInput is a schema as a client submits it.
type SchemaInput struct {
	Schema     string      `json:"schema"`
	SchemaType string      `json:"schemaType,omitempty"`
	References []Reference `json:"references,omitempty"`
}

// SchemaView is a stored schema as the API reports it.
type SchemaView struct {
	Subject    string      `json:"subject,omitempty"`
	Version    int         `json:"version,omitempty"`
	ID         int         `json:"id,omitempty"`
	SchemaType string      `json:"schemaType,omitempty"`
	References []Reference `json:"references,omitempty"`
	Schema     string      `json:"schema"`
}

func (r *Registry) view(vr *versionRec) SchemaView {
	s := r.schemas[vr.ID]
	return SchemaView{Subject: vr.Subject, Version: vr.Version, ID: vr.ID, SchemaType: typeField(s.Type),
		References: s.References, Schema: s.Schema}
}

// typeField is how the API names a schema's type: Avro, the default, is left out.
func typeField(t string) string {
	if t == TypeAvro {
		return ""
	}
	return t
}

// resolve follows references to the schemas they name, dependencies first. Following
// stops at a cycle, which is reported.
//
//fuzz:parser FuzzRegisterReferences
func (r *Registry) resolve(refs []Reference) ([]resolvedRef, *Error) {
	var out []resolvedRef
	seen := map[string]bool{}
	var walk func(refs []Reference, depth int) *Error
	walk = func(refs []Reference, depth int) *Error {
		if depth > 32 {
			return errInvalidSchema("Invalid schema: references are nested too deeply")
		}
		for _, ref := range refs {
			vr := r.find(ref.Subject, ref.Version)
			if vr == nil || vr.Deleted {
				return errInvalidSchema("Invalid schema: reference %q: subject '%s' version %d was not found", ref.Name, ref.Subject, ref.Version)
			}
			s := r.schemas[vr.ID]
			if err := walk(s.References, depth+1); err != nil {
				return err
			}
			key := fmt.Sprintf("%s\x00%d\x00%s", ref.Subject, ref.Version, ref.Name)
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, resolvedRef{Reference: ref, Type: s.Type, Text: s.Schema})
		}
		return nil
	}
	if err := walk(refs, 0); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *Registry) find(subject string, version int) *versionRec {
	for _, v := range r.subj[subject] {
		if v.Version == version {
			return v
		}
	}
	return nil
}

// prepare parses a submitted schema and returns the form the registry stores.
func (r *Registry) prepare(in SchemaInput, normalize bool) (parsedSchema, string, *Error) {
	typ, ok := normalizeType(in.SchemaType)
	if !ok {
		return nil, "", errInvalidSchema("Invalid schema type %s", in.SchemaType)
	}
	if strings.TrimSpace(in.Schema) == "" {
		return nil, "", errInvalidSchema("Invalid schema: schema may not be empty")
	}
	refs, rerr := r.resolve(in.References)
	if rerr != nil {
		return nil, "", rerr
	}
	p, err := parseSchema(typ, in.Schema, refs)
	if err != nil {
		return nil, "", errInvalidSchema("Invalid schema %s, details: %v", describe(in), err)
	}
	return p, p.Canonical(normalize), nil
}

func describe(in SchemaInput) string {
	typ, _ := normalizeType(in.SchemaType)
	return fmt.Sprintf("{schema=%s, schemaType=%s, references=%s}", in.Schema, typ, refsString(in.References))
}

func refsString(refs []Reference) string {
	if len(refs) == 0 {
		return "[]"
	}
	var parts []string
	for _, r := range refs {
		parts = append(parts, fmt.Sprintf("io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference@%x{name=%s, subject=%s, version=%d}", 0, r.Name, r.Subject, r.Version))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// effectiveCompat is the level that applies to a subject: its own, else the global one.
func (r *Registry) effectiveCompat(subject string) string {
	if l, ok := r.compat[subject]; ok && subject != "" {
		return l
	}
	if l, ok := r.compat[""]; ok {
		return l
	}
	return defaultCompat
}

func (r *Registry) effectiveMode(subject string) string {
	if m, ok := r.modes[subject]; ok && subject != "" {
		return m
	}
	if m, ok := r.modes[""]; ok {
		return m
	}
	return ModeReadWrite
}

// compatibleWith tells whether `p` may follow the live versions of a subject at the
// given level, and why not.
func (r *Registry) compatibleWith(subject string, p parsedSchema, level string) []string {
	if level == CompatNone {
		return nil
	}
	var all []string
	versions := r.live(subject)
	if len(versions) == 0 {
		return nil
	}
	transitive := strings.HasSuffix(level, "_TRANSITIVE")
	if !transitive {
		versions = versions[len(versions)-1:]
	}
	base := strings.TrimSuffix(level, "_TRANSITIVE")
	for _, vr := range versions {
		old, err := r.parseStored(vr.ID)
		if err != nil {
			all = append(all, fmt.Sprintf("existing schema version %d cannot be read: %v", vr.Version, err))
			continue
		}
		if msgs := compare(p, old, base); len(msgs) > 0 {
			all = append(all, CompatFinding{Version: vr.Version, Old: r.schemas[vr.ID].Schema, Level: base, Reasons: msgs}.Messages()...)
		}
	}
	return all
}

func (r *Registry) parseStored(id int) (parsedSchema, error) {
	s := r.schemas[id]
	refs, rerr := r.resolve(s.References)
	if rerr != nil {
		// A reference deleted after the fact: compare what is there without it.
		return nil, rerr
	}
	return parseSchema(s.Type, s.Schema, refs)
}

// compare returns the reasons `newer` is not compatible with `older` under a
// non-transitive level name (BACKWARD, FORWARD or FULL).
func compare(newer, older parsedSchema, level string) []string {
	var msgs []string
	if level == CompatBackward || level == CompatFull {
		msgs = append(msgs, canRead(newer, older)...)
	}
	if level == CompatForward || level == CompatFull {
		msgs = append(msgs, canRead(older, newer)...)
	}
	return msgs
}

// canRead reports why a reader built for `reader` cannot read data written for `writer`.
func canRead(reader, writer parsedSchema) []string {
	if reader.Type() != writer.Type() {
		return []string{fmt.Sprintf("Incompatible because of different schema type: %s vs %s", reader.Type(), writer.Type())}
	}
	switch rd := reader.(type) {
	case *avroSchema:
		return avroCanRead(rd, writer.(*avroSchema))
	case *jsonSchema:
		return jsonCanRead(rd, writer.(*jsonSchema))
	case *protoSchema:
		return protoCanRead(rd, writer.(*protoSchema))
	}
	return nil
}

// Register stores a schema under a subject and returns its id. A schema the subject
// already holds is not stored again. Anything else must be compatible with the subject's
// history, at the level in force.
//
//fuzz:parser FuzzRegisterAndCompare, FuzzRegisterReferences, FuzzAPIBody
func (r *Registry) Register(subject string, in SchemaInput, normalize bool) (RegisterResult, *Error) {
	if subject == "" {
		return RegisterResult{}, newErr(422, codeInvalidRequest, "Subject may not be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if strings.TrimSpace(in.Schema) == "" {
		// No schema in the request: the answer is the subject's latest, as Confluent
		// gives it (and "Empty schema" when there is none).
		vs := r.live(subject)
		if len(vs) == 0 {
			return RegisterResult{}, errInvalidSchema("Empty schema")
		}
		v := r.view(vs[len(vs)-1])
		return RegisterResult{ID: v.ID, Latest: &v}, nil
	}
	if r.effectiveMode(subject) == ModeReadOnly {
		return RegisterResult{}, newErr(422, codeOperationNotPermitted, "Subject %s is in read-only mode", subject)
	}
	p, canon, perr := r.prepare(in, normalize)
	if perr != nil {
		return RegisterResult{}, perr
	}
	id, e := r.registerLocked(subject, in, p, canon)
	return RegisterResult{ID: id}, e
}

// RegisterResult is the answer to a registration: the schema's id, or, when the
// request carried no schema, the subject's latest version.
type RegisterResult struct {
	ID     int
	Latest *SchemaView
}

func (r *Registry) registerLocked(subject string, in SchemaInput, p parsedSchema, canon string) (int, *Error) {
	typ := p.Type()
	fp := fingerprint(typ, canon, in.References)
	var replaced []*versionRec // soft-deleted copies of this schema in the subject.
	if id, ok := r.byPrint[fp]; ok {
		for _, v := range r.subj[subject] {
			if v.ID != id {
				continue
			}
			if !v.Deleted {
				return id, nil
			}
			replaced = append(replaced, v)
		}
	}
	if msgs := r.compatibleWith(subject, p, r.effectiveCompat(subject)); len(msgs) > 0 {
		return 0, newErr(409, codeIncompatible, "Schema being registered is incompatible with an earlier schema for subject \"%s\", details: [%s]", subject, strings.Join(msgs, ", "))
	}
	b := newBatch()
	id, known := r.byPrint[fp]
	if !known {
		id = r.nextID
		b.put(schemaKey(id), schemaRec{ID: id, Type: typ, Schema: canon, References: in.References})
		b.put(keyNextID, strconv.Itoa(id+1))
	}
	next := 1
	if vs := r.subj[subject]; len(vs) > 0 {
		next = vs[len(vs)-1].Version + 1
	}
	vr := &versionRec{Subject: subject, Version: next, ID: id}
	b.put(versionKey(subject, next), vr)
	// Registering a schema the subject had deleted does not bring the old version back:
	// that version goes for good and the schema becomes a new, latest one.
	for _, old := range replaced {
		b.del(versionKey(subject, old.Version))
	}
	if err := r.commit(b); err != nil {
		return 0, err
	}
	for _, old := range replaced {
		r.removeVersion(subject, old.Version)
	}
	if !known {
		r.schemas[id] = &schemaRec{ID: id, Type: typ, Schema: canon, References: in.References}
		r.byPrint[fp] = id
		r.nextID = id + 1
	}
	r.subj[subject] = append(r.subj[subject], vr)
	return id, nil
}

// Lookup finds the version of a subject that holds this schema.
func (r *Registry) Lookup(subject string, in SchemaInput, normalize, deleted bool) (SchemaView, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.subj[subject]) == 0 || (!deleted && !r.subjectExists(subject)) {
		return SchemaView{}, errSubjectNotFound(subject)
	}
	p, canon, perr := r.prepare(in, normalize)
	if perr != nil {
		return SchemaView{}, perr
	}
	fp := fingerprint(p.Type(), canon, in.References)
	id, ok := r.byPrint[fp]
	if ok {
		vs := r.subj[subject]
		for i := len(vs) - 1; i >= 0; i-- {
			if vs[i].ID == id && (deleted || !vs[i].Deleted) {
				return r.view(vs[i]), nil
			}
		}
	}
	return SchemaView{}, errSchemaNotFound()
}

// parseVersion reads a version path element: a number, "latest", or -1.
//
//fuzz:parser class=registry-rest FuzzVersionPath
func parseVersion(s string) (int, bool, *Error) {
	if s == "latest" || s == "-1" {
		return 0, true, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > math.MaxInt32 {
		return 0, false, errInvalidVersion(s)
	}
	return n, false, nil
}

// Version returns one version of a subject.
func (r *Registry) Version(subject, version string, deleted bool) (SchemaView, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	vr, err := r.versionLocked(subject, version, deleted)
	if err != nil {
		return SchemaView{}, err
	}
	return r.view(vr), nil
}

func (r *Registry) versionLocked(subject, version string, deleted bool) (*versionRec, *Error) {
	n, latest, err := parseVersion(version)
	if err != nil {
		return nil, err
	}
	if len(r.subj[subject]) == 0 || (!deleted && !r.subjectExists(subject)) {
		return nil, errSubjectNotFound(subject)
	}
	if latest {
		vs := r.live(subject)
		if deleted {
			vs = r.subj[subject]
		}
		return vs[len(vs)-1], nil
	}
	vr := r.find(subject, n)
	if vr == nil || (vr.Deleted && !deleted) {
		return nil, errVersionNotFound(subject, n)
	}
	return vr, nil
}

// Subjects lists subject names, sorted. prefix limits them; deleted adds those whose
// every version is deleted; deletedOnly lists only those.
func (r *Registry) Subjects(prefix string, deleted, deletedOnly bool) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []string{}
	for name, vs := range r.subj {
		if !strings.HasPrefix(name, prefix) || len(vs) == 0 {
			continue
		}
		alive := r.subjectExists(name)
		switch {
		case deletedOnly && !alive:
		case !deletedOnly && (alive || deleted):
		default:
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Versions lists a subject's version numbers.
func (r *Registry) Versions(subject string, deleted bool) ([]int, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.subj[subject]) == 0 || (!deleted && !r.subjectExists(subject)) {
		return nil, errSubjectNotFound(subject)
	}
	out := []int{}
	for _, v := range r.subj[subject] {
		if deleted || !v.Deleted {
			out = append(out, v.Version)
		}
	}
	return out, nil
}

// SchemaByID returns a schema by its global id.
func (r *Registry) SchemaByID(id int) (SchemaView, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.schemas[id]
	if !ok {
		return SchemaView{}, errSchemaIDNotFound(id)
	}
	return SchemaView{ID: 0, SchemaType: typeField(s.Type), References: s.References, Schema: s.Schema}, nil
}

// IDSubjects lists the subject-versions that hold a schema id.
func (r *Registry) IDSubjects(id int, deleted bool) ([]versionRec, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.schemas[id]; !ok {
		return nil, errSchemaNotFound()
	}
	var out []versionRec
	for _, vs := range r.subj {
		for _, v := range vs {
			if v.ID == id && (deleted || !v.Deleted) {
				out = append(out, *v)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Subject != out[j].Subject {
			return out[i].Subject < out[j].Subject
		}
		return out[i].Version < out[j].Version
	})
	return out, nil
}

// AllSchemas lists schemas (latest version of each subject when latestOnly).
func (r *Registry) AllSchemas(prefix string, latestOnly, deleted bool) []SchemaView {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []SchemaView
	for name, vs := range r.subj {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		var keep []*versionRec
		for _, v := range vs {
			if deleted || !v.Deleted {
				keep = append(keep, v)
			}
		}
		if latestOnly && len(keep) > 0 {
			keep = keep[len(keep)-1:]
		}
		for _, v := range keep {
			out = append(out, r.view(v))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Subject != out[j].Subject {
			return out[i].Subject < out[j].Subject
		}
		return out[i].Version < out[j].Version
	})
	return out
}

// ReferencedBy lists the ids of schemas that reference a subject version.
func (r *Registry) ReferencedBy(subject, version string) ([]int, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	vr, err := r.versionLocked(subject, version, true)
	if err != nil {
		return nil, err
	}
	return r.referencing(vr.Subject, vr.Version), nil
}

// referencing returns the ids of schemas, held by some live version, that name the
// subject version. Callers hold r.mu.
func (r *Registry) referencing(subject string, version int) []int {
	out := []int{}
	seen := map[int]bool{}
	for _, vs := range r.subj {
		for _, v := range vs {
			if v.Deleted || seen[v.ID] {
				continue
			}
			for _, ref := range r.schemas[v.ID].References {
				if ref.Subject == subject && ref.Version == version {
					seen[v.ID] = true
					out = append(out, v.ID)
				}
			}
		}
	}
	sort.Ints(out)
	return out
}

// DeleteVersion deletes one version. Soft first; permanent only after that.
func (r *Registry) DeleteVersion(subject, version string, permanent bool) (int, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.effectiveMode(subject) == ModeReadOnly {
		return 0, newErr(422, codeOperationNotPermitted, "Subject %s is in read-only mode", subject)
	}
	vr, err := r.versionLocked(subject, version, true)
	if err != nil {
		return 0, err
	}
	if permanent && !vr.Deleted {
		return 0, newErr(404, codeVersionNotSoftDeleted, "Subject '%s' Version %d was not deleted first before being permanently deleted", subject, vr.Version)
	}
	if !permanent && vr.Deleted {
		return 0, newErr(404, codeVersionSoftDeleted, "Subject '%s' Version %d was soft deleted.Set permanent=true to delete permanently", subject, vr.Version)
	}
	if refs := r.referencing(subject, vr.Version); len(refs) > 0 {
		return 0, newErr(422, codeReferenceExists, "One or more references exist to the schema {magic=1,keytype=SCHEMA,subject=%s,version=%d}.", subject, vr.Version)
	}
	b := newBatch()
	if permanent {
		b.del(versionKey(subject, vr.Version))
	} else {
		cp := *vr
		cp.Deleted = true
		b.put(versionKey(subject, vr.Version), &cp)
	}
	gone := r.orphans(map[*versionRec]bool{vr: permanent})
	for _, id := range gone {
		b.del(schemaKey(id))
	}
	if err := r.commit(b); err != nil {
		return 0, err
	}
	if permanent {
		r.removeVersion(subject, vr.Version)
		r.dropSchemas(gone)
	} else {
		vr.Deleted = true
	}
	return vr.Version, nil
}

// orphans lists schema ids that no version would hold once the given versions (the
// map's true entries) are removed permanently.
func (r *Registry) orphans(removing map[*versionRec]bool) []int {
	still := map[int]bool{}
	cand := map[int]bool{}
	for rec, perm := range removing {
		if perm {
			cand[rec.ID] = true
		}
	}
	if len(cand) == 0 {
		return nil
	}
	for _, vs := range r.subj {
		for _, v := range vs {
			if !removing[v] {
				still[v.ID] = true
			}
		}
	}
	var out []int
	for id := range cand {
		if !still[id] {
			out = append(out, id)
		}
	}
	sort.Ints(out)
	return out
}

func (r *Registry) removeVersion(subject string, version int) {
	vs := r.subj[subject]
	for i, v := range vs {
		if v.Version == version {
			r.subj[subject] = append(vs[:i:i], vs[i+1:]...)
			break
		}
	}
	if len(r.subj[subject]) == 0 {
		delete(r.subj, subject)
	}
}

func (r *Registry) dropSchemas(ids []int) {
	for _, id := range ids {
		if s := r.schemas[id]; s != nil {
			delete(r.byPrint, fingerprint(s.Type, s.Schema, s.References))
			delete(r.schemas, id)
		}
	}
}

// DeleteSubject deletes every version of a subject, returning their numbers.
func (r *Registry) DeleteSubject(subject string, permanent bool) ([]int, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.effectiveMode(subject) == ModeReadOnly {
		return nil, newErr(422, codeOperationNotPermitted, "Subject %s is in read-only mode", subject)
	}
	all := r.subj[subject]
	if len(all) == 0 || (!permanent && !r.subjectExists(subject)) {
		if len(all) > 0 && !permanent {
			return nil, newErr(404, codeSubjectSoftDeleted, "Subject '%s' was soft deleted.Set permanent=true to delete permanently", subject)
		}
		return nil, errSubjectNotFound(subject)
	}
	if permanent && r.subjectExists(subject) {
		return nil, newErr(404, codeSubjectNotSoftDeleted, "Subject '%s' was not deleted first before being permanently deleted", subject)
	}
	var targets []*versionRec
	for _, v := range all {
		if permanent || !v.Deleted {
			targets = append(targets, v)
		}
	}
	for _, v := range targets {
		if refs := r.referencing(subject, v.Version); len(refs) > 0 {
			return nil, newErr(422, codeReferenceExists, "One or more references exist to the schema {magic=1,keytype=SCHEMA,subject=%s,version=%d}.", subject, v.Version)
		}
	}
	b := newBatch()
	removing := map[*versionRec]bool{}
	out := make([]int, 0, len(targets))
	for _, v := range targets {
		out = append(out, v.Version)
		if permanent {
			b.del(versionKey(subject, v.Version))
			removing[v] = true
		} else {
			cp := *v
			cp.Deleted = true
			b.put(versionKey(subject, v.Version), &cp)
		}
	}
	gone := r.orphans(removing)
	for _, id := range gone {
		b.del(schemaKey(id))
	}
	if permanent {
		b.del(keyConfig + subject)
		b.del(keyMode + subject)
	}
	if err := r.commit(b); err != nil {
		return nil, err
	}
	if permanent {
		delete(r.subj, subject)
		delete(r.compat, subject)
		delete(r.modes, subject)
		r.dropSchemas(gone)
	} else {
		for _, v := range targets {
			v.Deleted = true
		}
	}
	return out, nil
}

// CheckCompatible tests a schema against a version of a subject ("latest", a number,
// or "" for the versions the subject's level calls for) and returns why it does not
// fit, with the versions it was compared to. A subject that does not exist accepts
// anything.
func (r *Registry) CheckCompatible(subject, version string, in SchemaInput, normalize bool) ([]CompatFinding, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, _, perr := r.prepare(in, normalize)
	if perr != nil {
		return nil, perr
	}
	level := r.effectiveCompat(subject)
	var against []*versionRec
	if version == "" {
		against = r.live(subject)
		if level != CompatNone && !strings.HasSuffix(level, "_TRANSITIVE") && len(against) > 1 {
			against = against[len(against)-1:]
		}
	} else {
		vr, err := r.versionLocked(subject, version, false)
		if err != nil {
			if err.Code == codeSubjectNotFound {
				return nil, nil
			}
			return nil, err
		}
		against = []*versionRec{vr}
	}
	if level == CompatNone {
		return nil, nil
	}
	base := strings.TrimSuffix(level, "_TRANSITIVE")
	var out []CompatFinding
	for _, vr := range against {
		old, err := r.parseStored(vr.ID)
		if err != nil {
			out = append(out, CompatFinding{Version: vr.Version, Old: r.schemas[vr.ID].Schema, Level: base, Reasons: []string{err.Error()}})
			continue
		}
		if msgs := compare(p, old, base); len(msgs) > 0 {
			out = append(out, CompatFinding{Version: vr.Version, Old: r.schemas[vr.ID].Schema, Level: base, Reasons: msgs})
		}
	}
	return out, nil
}

// CompatFinding is one existing version a schema is not compatible with, and why.
type CompatFinding struct {
	Version int
	Old     string
	Level   string
	Reasons []string
}

// Messages renders findings the way Confluent's verbose compatibility check does.
func (f CompatFinding) Messages() []string {
	out := append([]string(nil), f.Reasons...)
	out = append(out,
		fmt.Sprintf("{oldSchemaVersion: %d}", f.Version),
		fmt.Sprintf("{oldSchema: '%s'}", f.Old),
		fmt.Sprintf("{validateFields: 'false', compatibility: '%s'}", f.Level))
	return out
}

// Compat returns the compatibility level of a subject ("" = global). With
// fallback, a subject without its own level reports the global one.
func (r *Registry) Compat(subject string, fallback bool) (string, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if subject == "" {
		return r.effectiveCompat(""), nil
	}
	if l, ok := r.compat[subject]; ok {
		return l, nil
	}
	if fallback {
		return r.effectiveCompat(subject), nil
	}
	return "", newErr(404, codeSubjectCompatNotSet, "Subject '%s' does not have subject-level compatibility configured", subject)
}

// SetCompat sets the level of a subject ("" = global).
func (r *Registry) SetCompat(subject, level string) *Error {
	level = strings.ToUpper(level)
	if !compatLevels[level] {
		return newErr(422, codeInvalidCompatibility, "Invalid compatibility level. Valid values are none, backward, forward, full, backward_transitive, forward_transitive, and full_transitive")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b := newBatch()
	b.put(keyConfig+subject, level)
	if err := r.commit(b); err != nil {
		return err
	}
	r.compat[subject] = level
	return nil
}

// DeleteCompat removes a subject's own level (the global one is reset to the default)
// and returns what it was.
func (r *Registry) DeleteCompat(subject string) (string, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prev := r.effectiveCompat(subject)
	if subject != "" {
		if _, ok := r.compat[subject]; !ok {
			return "", errSubjectNotFound(subject)
		}
	}
	b := newBatch()
	b.del(keyConfig + subject)
	if err := r.commit(b); err != nil {
		return "", err
	}
	delete(r.compat, subject)
	return prev, nil
}

// Mode returns the mode of a subject ("" = global).
func (r *Registry) Mode(subject string, fallback bool) (string, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if subject == "" {
		return r.effectiveMode(""), nil
	}
	if m, ok := r.modes[subject]; ok {
		return m, nil
	}
	if fallback {
		return r.effectiveMode(subject), nil
	}
	return "", newErr(404, codeSubjectModeNotSet, "Subject '%s' does not have subject-level mode configured", subject)
}

// SetMode sets the mode of a subject ("" = global).
func (r *Registry) SetMode(subject, mode string) *Error {
	mode = strings.ToUpper(mode)
	if mode != ModeReadWrite && mode != ModeReadOnly {
		return newErr(422, codeInvalidMode, "Invalid mode. Valid values are READWRITE and READONLY")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b := newBatch()
	b.put(keyMode+subject, mode)
	if err := r.commit(b); err != nil {
		return err
	}
	r.modes[subject] = mode
	return nil
}

// DeleteMode removes a subject's own mode and returns what it was.
func (r *Registry) DeleteMode(subject string) (string, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prev := r.effectiveMode(subject)
	if _, ok := r.modes[subject]; !ok && subject != "" {
		return "", newErr(404, codeSubjectModeNotSet, "Subject '%s' does not have subject-level mode configured", subject)
	}
	b := newBatch()
	b.del(keyMode + subject)
	if err := r.commit(b); err != nil {
		return "", err
	}
	delete(r.modes, subject)
	return prev, nil
}
