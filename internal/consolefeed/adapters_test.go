package consolefeed

import (
	"reflect"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/console"
	"github.com/doze-dev/doze-kafka/internal/broker"
	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/traffic"
)

// fill gives every field of v a distinct non-zero value, so a field an adapter forgets to
// copy shows as a zero on the other side.
func fill(v reflect.Value, n *int) {
	*n++
	switch v.Kind() {
	case reflect.String:
		v.SetString("s" + string(rune('a'+*n%26)) + string(rune('a'+(*n/26)%26)))
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int32, reflect.Int64:
		v.SetInt(int64(*n)) // a time.Duration is an Int64, and distinct numbers keep two fields apart.
	case reflect.Struct:
		if v.Type() == reflect.TypeOf(time.Time{}) {
			v.Set(reflect.ValueOf(time.Unix(1_700_000_000+int64(*n), 0)))
			return
		}
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).CanSet() {
				fill(v.Field(i), n)
			}
		}
	case reflect.Slice:
		s := reflect.MakeSlice(v.Type(), 2, 2)
		for i := 0; i < 2; i++ {
			fill(s.Index(i), n)
		}
		v.Set(s)
	}
}

// same reports where two values differ, matching struct fields by name: a field the
// source has and the target lacks (or holds as zero) is a field the adapter drops.
func same(t *testing.T, path string, a, b reflect.Value) {
	t.Helper()
	switch a.Kind() {
	case reflect.Struct:
		if a.Type() == reflect.TypeOf(time.Time{}) {
			if !a.Interface().(time.Time).Equal(b.Interface().(time.Time)) {
				t.Errorf("%s: %v became %v", path, a.Interface(), b.Interface())
			}
			return
		}
		for i := 0; i < a.NumField(); i++ {
			name := a.Type().Field(i).Name
			bf := b.FieldByName(name)
			if !bf.IsValid() {
				t.Errorf("%s.%s has no counterpart", path, name)
				continue
			}
			same(t, path+"."+name, a.Field(i), bf)
		}
	case reflect.Slice:
		if a.Len() != b.Len() {
			t.Errorf("%s: %d elements became %d", path, a.Len(), b.Len())
			return
		}
		for i := 0; i < a.Len(); i++ {
			same(t, path+"[]", a.Index(i), b.Index(i))
		}
	case reflect.String:
		if a.String() != b.String() {
			t.Errorf("%s: %q became %q", path, a.String(), b.String())
		}
	case reflect.Bool:
		if a.Bool() != b.Bool() {
			t.Errorf("%s: %v became %v", path, a.Bool(), b.Bool())
		}
	default:
		if a.Int() != b.Int() {
			t.Errorf("%s: %d became %d", path, a.Int(), b.Int())
		}
	}
}

type fakeGroups struct {
	snap   broker.GroupSnapshot
	expiry broker.OffsetExpiry
}

func (f fakeGroups) InspectGroup(id string) (broker.GroupSnapshot, bool) {
	return f.snap, id == f.snap.ID
}
func (f fakeGroups) InspectGroups() []broker.GroupSnapshot {
	return []broker.GroupSnapshot{f.snap, f.snap}
}
func (f fakeGroups) OffsetExpiry(string) broker.OffsetExpiry { return f.expiry }

func TestTheConsoleSeesWhatTheGroupCoordinatorsHold(t *testing.T) {
	if Groups(nil) != nil {
		t.Fatal("no broker to ask must be a nil inspector, so the page can say it cannot see")
	}
	var f fakeGroups
	n := 0
	fill(reflect.ValueOf(&f.snap).Elem(), &n)
	fill(reflect.ValueOf(&f.expiry).Elem(), &n)
	f.snap.ID = "g1"

	in := Groups(f)
	got, ok := in.InspectGroup("g1")
	if !ok {
		t.Fatal("a group the broker holds is not found")
	}
	same(t, "GroupSnapshot", reflect.ValueOf(f.snap), reflect.ValueOf(got))
	if _, ok := in.InspectGroup("nope"); ok {
		t.Error("a group the broker does not hold is found")
	}
	if all := in.InspectGroups(); len(all) != 2 {
		t.Errorf("%d groups, want 2", len(all))
	}

	ex, ok := in.(console.OffsetExpirer)
	if !ok {
		t.Fatal("an inspector over a broker that can say when offsets expire must say it")
	}
	e, ok := ex.OffsetExpiry("g1")
	if !ok {
		t.Fatal("the broker said nothing about expiry")
	}
	same(t, "OffsetExpiry", reflect.ValueOf(f.expiry), reflect.ValueOf(e))

	// An inspector whose broker has no expiry method says nothing about expiry.
	plain := Groups(onlyInspect{f})
	if _, ok := plain.(console.OffsetExpirer).OffsetExpiry("g1"); ok {
		t.Error("an inspector with no expiry answers one")
	}
}

// onlyInspect hides the OffsetExpiry method of fakeGroups.
type onlyInspect struct{ g fakeGroups }

func (o onlyInspect) InspectGroup(id string) (broker.GroupSnapshot, bool) {
	return o.g.InspectGroup(id)
}
func (o onlyInspect) InspectGroups() []broker.GroupSnapshot { return o.g.InspectGroups() }

type fakeLogs struct{ info broker.CleanerInfo }

func (f fakeLogs) CleanerInfo(topic string, p int32) (broker.CleanerInfo, bool) {
	return f.info, topic == "t" && p == 3
}

func TestTheConsoleSeesWhatTheLogCleanerHolds(t *testing.T) {
	if Logs(nil) != nil {
		t.Fatal("no broker to ask must be a nil source")
	}
	var f fakeLogs
	n := 0
	fill(reflect.ValueOf(&f.info).Elem(), &n)
	src := Logs(f)
	got, ok := src.Cleaner("t", 3)
	if !ok {
		t.Fatal("a partition the broker holds is not found")
	}
	same(t, "CleanerInfo", reflect.ValueOf(f.info), reflect.ValueOf(got))
	if _, ok := src.Cleaner("t", 4); ok {
		t.Error("a partition the broker does not hold is found")
	}
}

func TestTheTrafficQueryCarriesItsCounts(t *testing.T) {
	rec := traffic.New(100, nil)
	rec.ConnOpened(1, "127.0.0.1:50000")
	for i := 0; i < 3; i++ {
		rec.Request(protocol.Observation{Conn: 1, Remote: "127.0.0.1:50000", ClientID: "billing", API: kmsg.Metadata, Version: 12,
			Start: time.Now().Add(-time.Second), Duration: time.Millisecond, Correlation: int32(i),
			Summary: protocol.Summary{Topics: []string{"orders"}, Kind: "metadata"}})
	}
	q, ok := Traffic(rec).(interface {
		Query(console.TrafficFilter, int) console.TrafficResult
	})
	if !ok {
		t.Fatal("the traffic source has no Query")
	}
	res := q.Query(console.TrafficFilter{}, 10)
	if res.Total != 3 || len(res.Rows) != 3 || res.Requests < 3 {
		t.Errorf("total %d, rows %d, requests %d: want 3 of each", res.Total, len(res.Rows), res.Requests)
	}
	if res.Newest.IsZero() || res.Oldest.IsZero() || res.Newest.Before(res.Oldest) {
		t.Errorf("newest %v, oldest %v", res.Newest, res.Oldest)
	}
}
