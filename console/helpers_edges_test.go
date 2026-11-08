package console

import (
	"encoding/binary"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The schema editor's line numbers: one for every line of the schema, and at least
// eleven so an empty editor still has its height.
func TestTheEditorGutterNumbersEveryLineAndPadsToEleven(t *testing.T) {
	f := templateFuncs("/p", func(string, string) string { return "" })
	gutter := f["gutter"].(func(string) string)
	lines := f["lines"].(func(string) int)

	numbers := func(n int) string {
		var out []string
		for i := 1; i <= n; i++ {
			out = append(out, strconv.Itoa(i))
		}
		return strings.Join(out, "\n")
	}
	if got := gutter(""); got != numbers(11) {
		t.Errorf("an empty editor: %q, want 1 to 11", got)
	}
	if got := gutter("a\nb\nc\n"); got != numbers(11) {
		t.Errorf("a short schema keeps the eleven-line height: %q", got)
	}
	twelve := strings.TrimSuffix(strings.Repeat("x\n", 12), "\n")
	if got := gutter(twelve); got != numbers(12) {
		t.Errorf("twelve lines: %q, want 1 to 12", got)
	}
	if got := gutter(twelve + "\n\n"); got != numbers(12) {
		t.Errorf("trailing newlines are not lines: %q", got)
	}
	if got := gutter(strings.Repeat("x\n", 40)); got != numbers(40) {
		t.Errorf("forty lines: %q", got)
	}

	if n := lines("a\nb\nc"); n != 3 {
		t.Errorf("three lines counted as %d", n)
	}
	if n := lines("one"); n != 1 {
		t.Errorf("one line counted as %d", n)
	}
	if n := lines("a\nb\n"); n != 2 {
		t.Errorf("a trailing newline starts no line: %d", n)
	}
}

// The topic page says a record-named subject is listed only when it has one.
func TestOnlyASubjectLabelledRecordCountsAsARecordSubject(t *testing.T) {
	f := templateFuncs("/p", func(string, string) string { return "" })
	has := f["hasRecordSubject"].(func([]topicSchema) bool)
	if !has([]topicSchema{{Label: "key"}, {Label: "record"}}) {
		t.Error("a record-named subject among others was not found")
	}
	if has([]topicSchema{{Label: "key"}, {Label: "value"}}) {
		t.Error("key and value subjects were taken for a record-named one")
	}
	if has(nil) {
		t.Error("no subjects were taken for a record-named one")
	}
}

// How the board's facts row words the cleanup policy.
func TestThePolicyTextNamesThePolicyAndWhetherItCompacts(t *testing.T) {
	for _, tc := range []struct {
		f    topicFactsView
		want string
	}{
		{topicFactsView{}, "delete, not compacted"},
		{topicFactsView{Policy: "delete"}, "delete, not compacted"},
		{topicFactsView{Policy: "compact", Compact: true}, "compact, compacted"},
		{topicFactsView{Policy: "compact,delete", Compact: true}, "compact,delete, compacted"},
		{topicFactsView{Compact: true}, "delete, compacted"},
	} {
		if got := tc.f.PolicyText(); got != tc.want {
			t.Errorf("%+v: %q, want %q", tc.f, got, tc.want)
		}
	}
}

// The create form lays settings two to a row; an even count leaves no empty row.
func TestSettingsAreLaidOutTwoToARowWithoutAnEmptyRow(t *testing.T) {
	mk := func(n int) []settingView {
		out := make([]settingView, n)
		for i := range out {
			out[i].Key = strconv.Itoa(i)
		}
		return out
	}
	for n, want := range map[int][]int{0: nil, 1: {1}, 2: {2}, 3: {2, 1}, 4: {2, 2}, 5: {2, 2, 1}} {
		rows := settingRows(mk(n))
		var got []int
		for _, r := range rows {
			got = append(got, len(r))
		}
		if len(got) != len(want) {
			t.Errorf("%d settings: rows of %v, want %v", n, got, want)
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("%d settings: rows of %v, want %v", n, got, want)
				break
			}
		}
	}
}

// controlBatch is a record batch header of 12+length bytes with the control bit set:
// base offset, length, attributes (bit 5) and the last offset delta where the broker
// writes them.
func controlBatch(base int64, delta int32, length int, control bool) []byte {
	raw := make([]byte, 12+length)
	binary.BigEndian.PutUint64(raw[0:8], uint64(base))
	binary.BigEndian.PutUint32(raw[8:12], uint32(length))
	if control {
		binary.BigEndian.PutUint16(raw[21:23], 0x20)
	}
	binary.BigEndian.PutUint32(raw[23:27], uint32(delta))
	return raw
}

// The control batches read out of a fetch's bytes: a batch that fills the buffer to
// its last byte is read, one that is cut off ends the scan, and a batch covers
// base to base plus its last offset delta.
func TestControlSpansReadABatchThatFillsTheBufferAndStopAtACutOffOne(t *testing.T) {
	// 61 bytes: the batch header alone, the smallest batch there is.
	spans := controlSpans(controlBatch(7, 2, 49, true))
	if len(spans) != 1 || spans[0] != (offsetSpan{First: 7, Last: 9}) {
		t.Errorf("a header-sized control batch at 7 with delta 2: %+v, want [7, 9]", spans)
	}
	if spans := controlSpans(controlBatch(7, 0, 49, false)); len(spans) != 0 {
		t.Errorf("a data batch is not a control batch: %+v", spans)
	}

	// A whole control batch, then a second that the response cut off.
	raw := append(controlBatch(3, 0, 49, true), controlBatch(4, 0, 200, true)[:80]...)
	if spans := controlSpans(raw); len(spans) != 1 || spans[0].First != 3 {
		t.Errorf("only the whole batch counts: %+v", spans)
	}
	// And a lone one that was cut off.
	if spans := controlSpans(controlBatch(4, 0, 100, true)[:70]); len(spans) != 0 {
		t.Errorf("a cut-off batch was read: %+v", spans)
	}
	// Two whole batches back to back are both read.
	raw = append(controlBatch(1, 0, 49, true), controlBatch(5, 1, 60, true)...)
	spans = controlSpans(raw)
	if len(spans) != 2 || spans[0] != (offsetSpan{First: 1, Last: 1}) || spans[1] != (offsetSpan{First: 5, Last: 6}) {
		t.Errorf("two batches: %+v", spans)
	}
}

// The offsets of a window that hold a marker: those of a control batch inside the
// window that have no record, and not the offset the window ends at.
func TestMarkersAreOnlyTheRecordlessOffsetsOfAControlBatchInsideTheWindow(t *testing.T) {
	got := markersIn(0, 5, map[int64]bool{}, []offsetSpan{{First: 3, Last: 7}})
	if len(got) != 2 || !got[3] || !got[4] {
		t.Errorf("window [0,5) over a batch at 3..7: %v, want 3 and 4 (5 is where the window ends)", got)
	}
	got = markersIn(4, 10, map[int64]bool{5: true}, []offsetSpan{{First: 2, Last: 6}})
	if len(got) != 2 || !got[4] || !got[6] || got[5] {
		t.Errorf("window [4,10) over a batch at 2..6 with a record at 5: %v, want 4 and 6", got)
	}
	if got := markersIn(0, 3, map[int64]bool{}, []offsetSpan{{First: 5, Last: 6}}); len(got) != 0 {
		t.Errorf("a batch past the window: %v", got)
	}
}

// The merged view's lanes: how far along the time axis each record sits, one lane per
// partition, and four axis moments from the oldest record.
func TestMergedLanesPlaceRecordsOnATimeAxisFromTheOldest(t *testing.T) {
	now := time.Now()
	t0 := now.Add(-10 * time.Second)
	bounds := []PartitionBound{{Partition: 0, Latest: 4}, {Partition: 1, Latest: 6}}
	asc := []msgView{
		{Partition: 0, Timestamp: t0, TickHeight: 7},
		{Partition: 1, Timestamp: now.Add(-5 * time.Second), TickHeight: 9},
		{Partition: 2, Timestamp: now.Add(-2 * time.Second), TickHeight: 11}, // a partition with no lane here: left out.
	}
	lanes, axis := mergedLanes(2, bounds, asc)
	if len(lanes) != 2 || lanes[0].Count != 4 || lanes[1].Count != 6 {
		t.Fatalf("lanes %+v", lanes)
	}
	if len(lanes[0].Ticks) != 1 || lanes[0].Ticks[0].Pct != 0 || lanes[0].Ticks[0].H != 7 {
		t.Errorf("the oldest record sits at the start: %+v", lanes[0].Ticks)
	}
	if len(lanes[1].Ticks) != 1 || lanes[1].Ticks[0].H != 9 {
		t.Fatalf("lane 1: %+v", lanes[1].Ticks)
	}
	if pct := lanes[1].Ticks[0].Pct; pct < 49.9 || pct > 50 {
		t.Errorf("a record halfway between the oldest and now sits at %v%%, want about 50", pct)
	}
	if len(axis) != 3 || !axis[0].Equal(t0) {
		t.Fatalf("axis %v, want 3 moments from %v", axis, t0)
	}
	if d := axis[1].Sub(t0); d < 3*time.Second || d > 4*time.Second {
		t.Errorf("a third of the way is %v after the oldest, want about 3.3s", d)
	}
	if d := axis[2].Sub(t0); d < 6*time.Second || d > 7*time.Second {
		t.Errorf("two thirds of the way is %v after the oldest, want about 6.7s", d)
	}

	// No records: the lanes are there and the axis is not.
	lanes, axis = mergedLanes(2, bounds, nil)
	if len(lanes) != 2 || axis != nil {
		t.Errorf("no records: %d lanes, axis %v", len(lanes), axis)
	}
	// Records from the future (clock skew) still land on the lane, at the start.
	lanes, _ = mergedLanes(1, bounds, []msgView{{Partition: 0, Timestamp: now.Add(time.Hour)}})
	if len(lanes[0].Ticks) != 1 || lanes[0].Ticks[0].Pct != 0 {
		t.Errorf("a record from the future: %+v", lanes[0].Ticks)
	}
}

// Which registry decoded a value is named from the facts, else from the client the
// console was given.
func TestTheRegistryThatDecodedAValueIsNamedByTheFactsOrTheClient(t *testing.T) {
	other := &registryClient{base: "http://client.example:9000"}

	c := &Console{facts: BrokerFacts{RegistryURL: "https://public.example:8081", RegistryBuiltIn: true}, reg: other}
	if got := c.registryWhere(); got != "the built-in registry at public.example:8081" {
		t.Errorf("facts name the registry: %q", got)
	}
	c = &Console{facts: BrokerFacts{RegistryURL: "http://public.example:8081"}, reg: other}
	if got := c.registryWhere(); got != "the registry at public.example:8081" {
		t.Errorf("an external registry: %q", got)
	}
	c = &Console{facts: BrokerFacts{RegistryBuiltIn: true}, reg: other}
	if got := c.registryWhere(); got != "the built-in registry at client.example:9000" {
		t.Errorf("no public address: the client's, %q", got)
	}
	c = &Console{reg: other}
	if got := c.registryWhere(); got != "the registry at client.example:9000" {
		t.Errorf("no public address, not built in: %q", got)
	}
	c = &Console{}
	if got := c.registryWhere(); got != "the registry at " {
		t.Errorf("no registry at all: %q", got)
	}
}

// A string literal at the very start of a .proto text is coloured as a string.
func TestAStringLiteralAtTheStartOfProtoTextIsColouredAsAString(t *testing.T) {
	if got, want := string(protoTint(`"v1" 7`)), `<span class="s">&#34;v1&#34;</span> <span class="n">7</span>`; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}
