package console

import (
	"context"
	"slices"
	"testing"
)

// The registry calls the Schemas pages make beyond reading versions, against the
// real registry.
func TestRegistryClientCallsForTheSchemaPages(t *testing.T) {
	c, _, _ := countingRegistry(t)
	ctx := context.Background()
	rc := c.reg
	id1, err := rc.Register(ctx, "orders-value", schemaBody{Schema: userV1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rc.Register(ctx, "orders-value", schemaBody{Schema: userV2}); err != nil {
		t.Fatal(err)
	}

	// When and how a version was registered.
	vi, ok := rc.VersionInfo(ctx, "orders-value", 1)
	if !ok || vi.ID != id1 || vi.Version != 1 || vi.RegisteredAt == nil {
		t.Errorf("version info: %+v %v", vi, ok)
	}
	if _, ok := rc.VersionInfo(ctx, "nobody", 0); ok {
		t.Error("version info of a subject that is not there")
	}

	// The overview: the next free id and the subject.
	o, ok := rc.Overview(ctx)
	if !ok || o.NextID <= id1 || subjectState(o, "orders-value") == nil || subjectState(o, "orders-value").Versions != 2 {
		t.Errorf("overview: %+v %v", o, ok)
	}
	if subjectState(o, "nobody") != nil {
		t.Error("an overview names a subject that is not there")
	}

	// A check at another level, with the reasons taken apart; the level is not changed.
	breaking := `{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"must","type":"string"}]}`
	a, err := rc.CheckAt(ctx, "orders-value", schemaBody{Schema: breaking}, "BACKWARD", "", false)
	if err != nil || a.Compatible || len(a.Findings) == 0 || len(a.Findings[0].Reasons) == 0 || a.Findings[0].Reasons[0].Type == "" {
		t.Errorf("check at BACKWARD: %+v %v", a, err)
	}
	if a, err := rc.CheckAt(ctx, "orders-value", schemaBody{Schema: breaking}, "NONE", "1", true); err != nil || !a.Compatible {
		t.Errorf("check at NONE: %+v %v", a, err)
	}
	if lvl, own, _ := rc.Compat(ctx, "orders-value"); own || lvl != "BACKWARD" {
		t.Errorf("a check changed the level: %s %v", lvl, own)
	}

	// Modes: the registry's until the subject has its own, and back.
	if m, own, err := rc.SubjectMode(ctx, "orders-value"); err != nil || own || m != "READWRITE" {
		t.Errorf("mode before: %s %v %v", m, own, err)
	}
	if err := rc.SetMode(ctx, "orders-value", "READONLY"); err != nil {
		t.Fatal(err)
	}
	if m, own, _ := rc.SubjectMode(ctx, "orders-value"); !own || m != "READONLY" {
		t.Errorf("mode set: %s %v", m, own)
	}
	if err := rc.ClearMode(ctx, "orders-value"); err != nil {
		t.Fatal(err)
	}
	if err := rc.SetGlobalCompat(ctx, "FULL"); err != nil {
		t.Fatal(err)
	}
	if g, _ := rc.GlobalCompat(ctx); g != "FULL" {
		t.Errorf("global level %s", g)
	}
	if err := rc.SetGlobalCompat(ctx, "BACKWARD"); err != nil {
		t.Fatal(err)
	}

	// References and the subjects an id is under.
	if _, err := rc.Register(ctx, "orders-copy", schemaBody{Schema: userV1}); err != nil {
		t.Fatal(err)
	}
	at, err := rc.IDVersions(ctx, id1)
	if err != nil || len(at) != 2 {
		t.Errorf("id %d is under %v %v", id1, at, err)
	}
	if ids, err := rc.ReferencedBy(ctx, "orders-value", 1); err != nil || len(ids) != 0 {
		t.Errorf("referenced by: %v %v", ids, err)
	}
	if r, err := rc.Resolved(ctx, "orders-value", 1); err != nil || r.Schema == "" {
		t.Errorf("resolved: %+v %v", r, err)
	}

	// The two deletes, each on its own: soft hides and keeps, permanent removes.
	if err := rc.SoftDeleteVersion(ctx, "orders-value", 1); err != nil {
		t.Fatal(err)
	}
	if vs, _ := rc.AllVersions(ctx, "orders-value"); !slices.Equal(vs, []int{1, 2}) {
		t.Errorf("a soft-deleted version is kept: %v", vs)
	}
	if vs, _ := rc.Versions(ctx, "orders-value"); !slices.Equal(vs, []int{2}) {
		t.Errorf("and not listed: %v", vs)
	}
	if err := rc.PermanentDeleteVersion(ctx, "orders-value", 1); err != nil {
		t.Fatal(err)
	}
	if vs, _ := rc.AllVersions(ctx, "orders-value"); !slices.Equal(vs, []int{2}) {
		t.Errorf("a permanent delete removes it: %v", vs)
	}
	if err := rc.SoftDeleteSubject(ctx, "orders-copy"); err != nil {
		t.Fatal(err)
	}
	if d, err := rc.DeletedSubjects(ctx); err != nil || !slices.Equal(d, []string{"orders-copy"}) {
		t.Errorf("deleted subjects: %v %v", d, err)
	}
	if err := rc.PermanentDeleteSubject(ctx, "orders-copy"); err != nil {
		t.Fatal(err)
	}
	if d, _ := rc.DeletedSubjects(ctx); len(d) != 0 {
		t.Errorf("after the permanent delete: %v", d)
	}
}
