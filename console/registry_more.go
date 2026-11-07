package console

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

// The registry calls the Schemas pages make beyond reading versions: the version
// metadata, the overview and the check at a level are doze-kafka's own routes
// (under /_doze/, docs/schema-registry.md); the rest are Confluent's. Another
// registry has no /_doze/ routes, and a page that asks one does without what they
// give (VersionInfo answers ok false).

// VersionInfo is a version with when and by whom it was registered. ok is false
// when the registry does not keep it (another registry, or a version stored before
// this was kept answers with no time).
func (rc *registryClient) VersionInfo(ctx context.Context, subject string, version int) (schemaregistry.VersionInfo, bool) {
	var v schemaregistry.VersionInfo
	n := "latest"
	if version > 0 {
		n = strconv.Itoa(version)
	}
	if err := rc.call(ctx, http.MethodGet, "/_doze/subjects/"+segment(subject)+"/versions/"+n, nil, &v); err != nil {
		return v, false
	}
	return v, true
}

// Overview is the registry's state in one read: next free id, ids in use, levels
// and modes with when they were set, every subject with its latest version.
func (rc *registryClient) Overview(ctx context.Context) (schemaregistry.Overview, bool) {
	var o schemaregistry.Overview
	if err := rc.call(ctx, http.MethodGet, "/_doze/overview", nil, &o); err != nil {
		return o, false
	}
	return o, true
}

// subjectState is one subject from an overview, or nil.
func subjectState(o schemaregistry.Overview, subject string) *schemaregistry.SubjectState {
	for i := range o.Subjects {
		if o.Subjects[i].Subject == subject {
			return &o.Subjects[i]
		}
	}
	return nil
}

// checkFinding is one version a schema was compared with and why it may not follow it.
type checkFinding struct {
	Version int                           `json:"version"`
	Level   string                        `json:"level"`
	Reasons []schemaregistry.CompatReason `json:"reasons"`
}

// checkAnswer is the registry's answer to a check at a level, each reason taken apart.
type checkAnswer struct {
	Compatible bool           `json:"compatible"`
	Level      string         `json:"level"`
	Compared   []int          `json:"compared"`
	Findings   []checkFinding `json:"findings"`
}

// CheckAt asks whether a schema may follow a subject's versions at a level (the
// subject's own when level is ""), without changing the level. version is the one
// to compare with ("" for what the level says: the latest, or all of them).
func (rc *registryClient) CheckAt(ctx context.Context, subject string, b schemaBody, level, version string, normalize bool) (checkAnswer, error) {
	q := url.Values{}
	if level != "" {
		q.Set("level", level)
	}
	if version != "" {
		q.Set("version", version)
	}
	if normalize {
		q.Set("normalize", "true")
	}
	p := "/_doze/subjects/" + segment(subject) + "/check"
	if len(q) > 0 {
		p += "?" + q.Encode()
	}
	var out checkAnswer
	err := rc.call(ctx, http.MethodPost, p, b, &out)
	return out, err
}

// Resolved is a version with its references written in (Avro), or listed (JSON
// Schema and Protobuf), as the registry inlines them.
func (rc *registryClient) Resolved(ctx context.Context, subject string, version int) (schemaregistry.Resolved, error) {
	var out schemaregistry.Resolved
	err := rc.call(ctx, http.MethodGet, fmt.Sprintf("/_doze/subjects/%s/versions/%d/resolved", segment(subject), version), nil, &out)
	return out, err
}

// SubjectMode is a subject's mode (READWRITE, READONLY), and whether it is the
// subject's own or the registry's.
func (rc *registryClient) SubjectMode(ctx context.Context, subject string) (mode string, own bool, err error) {
	var m struct {
		Mode string `json:"mode"`
	}
	err = rc.call(ctx, http.MethodGet, "/mode/"+segment(subject), nil, &m)
	if err == nil {
		return m.Mode, true, nil
	}
	var re *regError
	if errors.As(err, &re) && re.Code == 40409 {
		mode, err = rc.Mode(ctx)
		return mode, false, err
	}
	return "", false, err
}

// SetMode sets a subject's mode, or the registry's when subject is "".
func (rc *registryClient) SetMode(ctx context.Context, subject, mode string) error {
	p := "/mode"
	if subject != "" {
		p += "/" + segment(subject)
	}
	return rc.call(ctx, http.MethodPut, p, map[string]string{"mode": mode}, nil)
}

// ClearMode drops a subject's own mode, so it follows the registry's.
func (rc *registryClient) ClearMode(ctx context.Context, subject string) error {
	return rc.call(ctx, http.MethodDelete, "/mode/"+segment(subject), nil, nil)
}

// SetGlobalCompat sets the registry's default level, which every subject without
// its own follows.
func (rc *registryClient) SetGlobalCompat(ctx context.Context, level string) error {
	return rc.call(ctx, http.MethodPut, "/config", map[string]string{"compatibility": level}, nil)
}

// ReferencedBy is the schema ids that reference a version.
func (rc *registryClient) ReferencedBy(ctx context.Context, subject string, version int) ([]int, error) {
	var ids []int
	err := rc.call(ctx, http.MethodGet, fmt.Sprintf("/subjects/%s/versions/%d/referencedby", segment(subject), version), nil, &ids)
	return ids, err
}

// subjectVersion is a subject and version a schema id is registered under.
type subjectVersion struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// IDVersions is every subject and version a schema id is registered under.
func (rc *registryClient) IDVersions(ctx context.Context, id int) ([]subjectVersion, error) {
	var out []subjectVersion
	err := rc.call(ctx, http.MethodGet, fmt.Sprintf("/schemas/ids/%d/versions", id), nil, &out)
	return out, err
}

// SoftDeleteVersion and SoftDeleteSubject are the first delete: the version or
// subject is hidden and its ids stay reserved; it can be deleted for good after.
func (rc *registryClient) SoftDeleteVersion(ctx context.Context, subject string, version int) error {
	return rc.call(ctx, http.MethodDelete, fmt.Sprintf("/subjects/%s/versions/%d", segment(subject), version), nil, nil)
}

func (rc *registryClient) SoftDeleteSubject(ctx context.Context, subject string) error {
	return rc.call(ctx, http.MethodDelete, "/subjects/"+segment(subject), nil, nil)
}

// PermanentDeleteVersion and PermanentDeleteSubject are the second delete, of
// what is already soft-deleted.
func (rc *registryClient) PermanentDeleteVersion(ctx context.Context, subject string, version int) error {
	return rc.call(ctx, http.MethodDelete, fmt.Sprintf("/subjects/%s/versions/%d?permanent=true", segment(subject), version), nil, nil)
}

func (rc *registryClient) PermanentDeleteSubject(ctx context.Context, subject string) error {
	return rc.call(ctx, http.MethodDelete, "/subjects/"+segment(subject)+"?permanent=true", nil, nil)
}

// DeletedSubjects is the subjects whose every version is soft-deleted: listed with
// ?deleted=true and not without.
func (rc *registryClient) DeletedSubjects(ctx context.Context) ([]string, error) {
	var all, live []string
	if err := rc.call(ctx, http.MethodGet, "/subjects?deleted=true", nil, &all); err != nil {
		return nil, err
	}
	if err := rc.call(ctx, http.MethodGet, "/subjects", nil, &live); err != nil {
		return nil, err
	}
	have := map[string]bool{}
	for _, s := range live {
		have[s] = true
	}
	var out []string
	for _, s := range all {
		if !have[s] {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out, nil
}

// AllVersions is a subject's versions, the soft-deleted ones included.
func (rc *registryClient) AllVersions(ctx context.Context, subject string) ([]int, error) {
	var vs []int
	err := rc.call(ctx, http.MethodGet, "/subjects/"+segment(subject)+"/versions?deleted=true", nil, &vs)
	return vs, err
}
