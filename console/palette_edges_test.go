package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"
)

func paletteOf(t *testing.T, c *Console) map[string]paletteEntry {
	t.Helper()
	rec := req(t, c, "GET", "/_console/api/palette", nil)
	if rec.Code != 200 {
		t.Fatalf("palette: %d", rec.Code)
	}
	var out []paletteEntry
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	by := map[string]paletteEntry{}
	for _, e := range out {
		by[e.S+"/"+e.N] = e
	}
	return by
}

func TestPaletteDescribesTopicsGroupsAndSchemas(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	for name, parts := range map[string]int32{"one": 1, "three": 3} {
		if err := c.be.CreateTopic(ctx, name, parts, nil); err != nil {
			t.Fatal(err)
		}
	}
	bulkProduce(t, cl, "one", 0, 1, vals("m"), time.Time{})
	bulkProduce(t, cl, "three", 0, 14, vals("m"), time.Time{})
	bulkProduce(t, cl, "three", 1, 1400, vals("m"), time.Time{})
	commitFor(t, addr, "workers", "three", map[int32]int64{0: 4})
	register(t, c, "three-value", userV1)
	register(t, c, "three-value", userV2)
	register(t, c, "one-value", userV1)

	p := paletteOf(t, c)
	if d := p["topics/one"].D; d != "1 partition · 1 message" {
		t.Errorf("one: %q", d)
	}
	if d := p["topics/three"].D; d != "3 partitions · 1,414 messages" {
		t.Errorf("three: %q", d)
	}
	if e := p["topics/three"]; e.K != "topic" || e.U != "/_console/topics/three" {
		t.Errorf("%+v", e)
	}
	if e := p["groups/workers"]; e.D != "Empty · 0 members · lag 10" || e.K != "group" || e.U != "/_console/groups/workers" {
		t.Errorf("group: %+v", e)
	}
	if d := p["schemas/three-value"].D; d != "Avro · v1-2 · BACKWARD" {
		t.Errorf("two versions: %q", d)
	}
	if d := p["schemas/one-value"].D; d != "Avro · v1 · BACKWARD" {
		t.Errorf("one version: %q", d)
	}
	for _, a := range []string{"topics/Create topic", "actions/Follow a message by trace id", "schemas/Register schema"} {
		if e, ok := p[a]; !ok || e.K != "action" {
			t.Errorf("no %s", a)
		}
	}
	if _, ok := p["actions/Show errors"]; ok {
		t.Error("Show errors is offered without a recorder")
	}
}

// Past 25 groups or subjects the palette stops asking each one for detail.
func TestPaletteSkipsDetailForManyGroupsAndSubjects(t *testing.T) {
	c, addr, _ := newRegistryConsole(t, func(o *Options) { o.Traffic = &fakeTraffic{} })
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "t", 1, nil); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{26} {
		for i := 0; i < n; i++ {
			commitFor(t, addr, fmt.Sprintf("g%02d-%d", i, n), "t", map[int32]int64{0: 0})
			register(t, c, fmt.Sprintf("s%02d-%d-value", i, n), userV1)
		}
	}
	// With 26 groups and 26 subjects the palette is past the limit.
	p := paletteOf(t, c)
	if e := p["groups/g00-26"]; e.D != "Empty · 0 members" {
		t.Errorf("a group on a broker with many: %q", e.D)
	}
	if e := p["schemas/s00-26-value"]; e.D != "Avro · v1" {
		t.Errorf("a subject on a registry with many: %q", e.D)
	}
	if _, ok := p["actions/Show errors"]; !ok {
		t.Error("Show errors is not offered with a recorder")
	}
}

func TestPaletteDetailAtExactlyTwentyFive(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "t", 1, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		commitFor(t, addr, fmt.Sprintf("g%02d", i), "t", map[int32]int64{0: 0})
		register(t, c, fmt.Sprintf("s%02d-value", i), userV1)
	}
	p := paletteOf(t, c)
	if e := p["groups/g00"]; e.D != "Empty · 0 members · lag 0" {
		t.Errorf("25 groups: %q", e.D)
	}
	if e := p["schemas/s00-value"]; e.D != "Avro · v1 · BACKWARD" {
		t.Errorf("25 subjects: %q", e.D)
	}
}

func TestShellDataFacts(t *testing.T) {
	c, addr := newTestConsoleAddr(t, func(o *Options) { o.Facts.KafkaMajor = 4; o.Facts.Workspace = "harbour" })
	topicWith(t, c, "a", 1, 0)
	topicWith(t, c, "b", 1, 0)
	commitFor(t, addr, "g", "a", map[int32]int64{0: 0})
	d := map[string]any{"View": "topic"}
	c.shellData(httptest.NewRequest("GET", "/_console/topics", nil), d)
	counts := d["Counts"].(map[string]string)
	if counts["topics"] != "2" || counts["groups"] != "1" {
		t.Errorf("counts %v", counts)
	}
	if _, ok := counts["schemas"]; ok {
		t.Errorf("a count for schemas without a registry: %v", counts)
	}
	if d["KafkaCompat"] != "Kafka 4.0 compat" || d["Workspace"] != "harbour" || d["HasRegistry"] != false || d["HasTraffic"] != false || d["Home"] != "topics" {
		t.Errorf("%v", d)
	}
	plain := newTestConsole(t)
	d = map[string]any{}
	plain.shellData(httptest.NewRequest("GET", "/_console/topics", nil), d)
	if _, ok := d["KafkaCompat"]; ok {
		t.Error("a compat label although no Kafka major was given")
	}
}
