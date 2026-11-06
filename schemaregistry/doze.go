package schemaregistry

import (
	"sort"
	"strings"
	"time"
)

// What the console needs from the registry that Confluent's API does not carry: who
// registered a version and when, when a level or a mode was set, and the whole
// registry in one answer. Served under /_doze/ (api_doze.go), which is not a Confluent
// path (docs/schema-registry.md).

// VersionInfo is a version with what the registry knows about how it came to be.
type VersionInfo struct {
	SchemaView
	Deleted      bool       `json:"deleted"`
	RegisteredAt *time.Time `json:"registeredAt"` // nil: stored before this was kept
	RegisteredBy string     `json:"registeredBy,omitempty"`
	From         string     `json:"from,omitempty"`
	Agent        string     `json:"agent,omitempty"`
}

// VersionInfo answers for one version of a subject, a soft-deleted one included.
func (r *Registry) VersionInfo(subject, version string) (VersionInfo, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	vr, e := r.versionLocked(subject, version, true)
	if e != nil {
		return VersionInfo{}, e
	}
	return VersionInfo{SchemaView: r.view(vr), Deleted: vr.Deleted, RegisteredAt: millis(vr.At),
		RegisteredBy: vr.By, From: vr.From, Agent: vr.Agent}, nil
}

func millis(ms int64) *time.Time {
	if ms == 0 {
		return nil
	}
	t := time.UnixMilli(ms).UTC()
	return &t
}

// Overview is the registry at a glance.
type Overview struct {
	NextID     int            `json:"nextId"`     // what the next new schema is given
	IDsInUse   [][2]int       `json:"idsInUse"`   // the ids that resolve, as ranges [first, last]
	Level      string         `json:"level"`      // the global level
	LevelSetAt *time.Time     `json:"levelSetAt"` // nil: never set here, or before this was kept
	Mode       string         `json:"mode"`       // the global mode
	ModeSetAt  *time.Time     `json:"modeSetAt"`
	Subjects   []SubjectState `json:"subjects"`
}

// SubjectState is one subject in the overview, a soft-deleted one included.
type SubjectState struct {
	Subject         string     `json:"subject"`
	Type            string     `json:"type"` // of the latest version: AVRO, JSON or PROTOBUF
	Versions        int        `json:"versions"`
	DeletedVersions int        `json:"deletedVersions"`
	Deleted         bool       `json:"deleted"` // every version soft-deleted
	LatestVersion   int        `json:"latestVersion"`
	LatestID        int        `json:"latestId"`
	Level           string     `json:"level"`
	LevelIsOwn      bool       `json:"levelIsOwn"`
	LevelSetAt      *time.Time `json:"levelSetAt"`
	Mode            string     `json:"mode"`
	ModeIsOwn       bool       `json:"modeIsOwn"`
	ModeSetAt       *time.Time `json:"modeSetAt"`
	LastRegistered  *time.Time `json:"lastRegistered"`
}

func (o Overview) subject(name string) *SubjectState {
	for i := range o.Subjects {
		if o.Subjects[i].Subject == name {
			return &o.Subjects[i]
		}
	}
	return nil
}

// Overview answers for the whole registry, subjects by name.
func (r *Registry) Overview() Overview {
	r.mu.Lock()
	defer r.mu.Unlock()
	o := Overview{NextID: r.nextID, Level: r.effectiveCompat(""), LevelSetAt: millis(r.compatAt[""]),
		Mode: r.effectiveMode(""), ModeSetAt: millis(r.modeAt[""]), IDsInUse: [][2]int{}, Subjects: []SubjectState{}}
	ids := make([]int, 0, len(r.schemas))
	for id := range r.schemas {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		if n := len(o.IDsInUse); n > 0 && o.IDsInUse[n-1][1] == id-1 {
			o.IDsInUse[n-1][1] = id
		} else {
			o.IDsInUse = append(o.IDsInUse, [2]int{id, id})
		}
	}
	names := make([]string, 0, len(r.subj))
	for name := range r.subj {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		vs := r.subj[name]
		if len(vs) == 0 {
			continue
		}
		s := SubjectState{Subject: name, Level: r.effectiveCompat(name), Mode: r.effectiveMode(name),
			LevelSetAt: millis(r.compatAt[name]), ModeSetAt: millis(r.modeAt[name])}
		_, s.LevelIsOwn = r.compat[name]
		_, s.ModeIsOwn = r.modes[name]
		var last int64
		latest := vs[len(vs)-1]
		for _, v := range vs {
			if v.Deleted {
				s.DeletedVersions++
			} else {
				s.Versions++
				latest = v
			}
			last = max(last, v.At)
		}
		s.Deleted = s.Versions == 0
		s.LatestVersion, s.LatestID = latest.Version, latest.ID
		if sc := r.schemas[latest.ID]; sc != nil {
			s.Type = sc.Type
		}
		s.LastRegistered = millis(last)
		o.Subjects = append(o.Subjects, s)
	}
	return o
}

// levelFor is the level a check uses: the one asked for, or the subject's.
func (r *Registry) levelFor(subject, asked string) string {
	if asked != "" {
		return strings.ToUpper(asked)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.effectiveCompat(subject)
}

// Resolved is a version with what its references bring in: for Avro, the schema with
// the referenced types written out (avroInline); for every type, each schema reached
// through references with its text.
type Resolved struct {
	SchemaType string        `json:"schemaType"`
	Schema     string        `json:"schema"`
	Inlined    bool          `json:"inlined"` // Schema has the referenced types written out
	References []ResolvedRef `json:"references"`
}

// ResolvedRef is one schema a version reaches through its references.
type ResolvedRef struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
}

// Resolved answers for one version of a subject.
func (r *Registry) Resolved(subject, version string) (Resolved, *Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	vr, e := r.versionLocked(subject, version, true)
	if e != nil {
		return Resolved{}, e
	}
	s := r.schemas[vr.ID]
	refs, e := r.resolve(s.References)
	if e != nil {
		return Resolved{}, e
	}
	out := Resolved{SchemaType: s.Type, Schema: s.Schema, References: []ResolvedRef{}}
	for _, rf := range refs {
		out.References = append(out.References, ResolvedRef{Name: rf.Name, Subject: rf.Subject, Version: rf.Version, Schema: rf.Text})
	}
	if s.Type == TypeAvro && len(refs) > 0 {
		if text, err := avroInline(s.Schema, refs); err == nil {
			out.Schema, out.Inlined = text, true
		}
	}
	return out, nil
}
