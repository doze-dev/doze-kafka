package console

import "testing"

type fakeInspector map[string]GroupSnapshot

func (f fakeInspector) InspectGroup(id string) (GroupSnapshot, bool) { g, ok := f[id]; return g, ok }
func (f fakeInspector) InspectGroups() []GroupSnapshot               { return nil }

func TestInspectGroupSaysWhenItCannotSee(t *testing.T) {
	c := &Console{}
	if got := c.inspectGroup("g"); got.State != InspectUnavailable || c.hasGroupInspector() {
		t.Errorf("no inspector: %+v, want unavailable", got)
	}
	c.inspector = fakeInspector{"g": {ID: "g", State: "Stable"}}
	if got := c.inspectGroup("g"); got.State != InspectOK || got.Group.State != "Stable" {
		t.Errorf("held group: %+v", got)
	}
	if got := c.inspectGroup("x"); got.State != InspectNone {
		t.Errorf("unknown group: %+v, want none (not unavailable)", got)
	}
}
