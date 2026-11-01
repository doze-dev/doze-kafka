package broker

import (
	"bytes"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// How often the sweep runs: a hundredth of the retention, no less than 50 ms and no
// more than ten minutes, which is also what a retention of none gets (documented in
// docs/limitations.md and docs/consumer-groups.md); an interval given outright wins.
func TestOffsetExpiryIntervalIsAHundredthOfTheRetentionWithinItsBounds(t *testing.T) {
	for _, tc := range []struct {
		retention, every, want time.Duration
	}{
		{retention: 0, want: 10 * time.Minute},
		{retention: -time.Second, want: 10 * time.Minute},
		{retention: time.Second, want: 50 * time.Millisecond},     // a hundredth is 10 ms: raised to 50.
		{retention: 5 * time.Second, want: 50 * time.Millisecond}, // exactly 50 ms.
		{retention: time.Hour, want: 36 * time.Second},            // a hundredth.
		{retention: 7 * 24 * time.Hour, want: 10 * time.Minute},   // a hundredth would be 100 minutes: capped.
		{retention: time.Hour, every: 3 * time.Second, want: 3 * time.Second},
		{retention: 0, every: 3 * time.Second, want: 3 * time.Second},
	} {
		e := &offsetExpiry{retention: tc.retention, every: tc.every}
		if got := e.interval(); got != tc.want {
			t.Errorf("retention %v, every %v: interval %v, want %v", tc.retention, tc.every, got, tc.want)
		}
	}
}

// A classic group that was empty when the broker stopped is back, with its protocol
// type, after a restart (recorded: groups-listed-after-restart).
func TestOffsetRetentionRestoresAnEmptiedClassicGroupWithItsProtocolType(t *testing.T) {
	dir := t.TempDir()
	start := time.Now()
	r := newExpiryRig(t, dir, start, "a")
	member, _ := r.join(t, "g", "a")
	r.commit(t, "g", "a", 5)
	r.leave(t, "g", member)
	r.exp.flush()
	r.b.Stop()
	r.m.Close()

	r2 := newExpiryRig(t, dir, start.Add(time.Second), "a")
	var found *adminGroupListing
	for _, g := range r2.b.listAllGroups() {
		if g.id == "g" {
			g := g
			found = &g
		}
	}
	if found == nil || found.state != "Empty" || found.protocolType != "consumer" || found.groupType != "classic" {
		t.Fatalf("after the restart the emptied group is %+v, want Empty, classic, protocol type consumer", found)
	}
}

// What a live group has not subscribed to is kept until its retention is up, counted
// from the commit, and no longer.
func TestOffsetRetentionOfAnUnsubscribedTopicRunsFromTheCommit(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a", "b")
	r.join(t, "g", "a")
	r.commit(t, "g", "b", 2)

	r.advance(testRetention - time.Second)
	r.exp.sweep()
	if !r.has("g", "b") {
		t.Fatal("an offset on a topic the group does not read expired before its retention")
	}
	r.advance(2 * time.Second)
	r.exp.sweep()
	if r.has("g", "b") {
		t.Fatal("an offset on a topic the group does not read outlived its retention")
	}
}

// A group with no members and no offsets is gone at the first sweep that finds it so,
// even when nothing was recorded of it (recorded: g-none, g-c-none); one with members
// is not, whatever it holds.
func TestOffsetRetentionDropsAnEmptyGroupWithNoOffsetsAndKeepsALiveOne(t *testing.T) {
	r := newExpiryRig(t, t.TempDir(), time.Now(), "a")
	r.join(t, "live", "a")
	r.b.groups.restoreEmpty("nobody", "consumer")
	r.b.cg848.restoreEmpty("nobody-c", 4, 4, "uniform")
	listed := func() map[string]bool {
		out := map[string]bool{}
		for _, g := range r.b.listAllGroups() {
			out[g.id] = true
		}
		return out
	}
	if got := listed(); !got["live"] || !got["nobody"] || !got["nobody-c"] {
		t.Fatalf("setup: listed %v", got)
	}
	r.exp.sweep()
	if got := listed(); !got["live"] || got["nobody"] || got["nobody-c"] {
		t.Errorf("after a sweep the groups are %v, want only live", got)
	}
}

// The groups of data written before groups with members were recorded are taken to
// have members, for the session of a Java client's default, and the log says so.
func TestOffsetRetentionAdoptsGroupsWithOffsetsForAnAdoptedSession(t *testing.T) {
	open := func(dir string, logOut io.Writer) (*offsetExpiry, func()) {
		m, err := meta.Open(filepath.Join(dir, "meta.db"))
		if err != nil {
			t.Fatal(err)
		}
		logs, err := log.OpenManager(filepath.Join(dir, "data"), log.ManagerOptions{SegmentBytes: 1 << 20, IndexIntervalBytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		cfg := config.Default()
		b, err := New(cfg, slog.New(slog.NewTextHandler(logOut, nil)), logs, m)
		if err != nil {
			t.Fatal(err)
		}
		return b.expiry, func() { b.Stop(); logs.Close(); m.Close() }
	}

	// A store with no group at all: nothing is adopted, and nothing is said.
	var quiet bytes.Buffer
	_, closeEmpty := open(t.TempDir(), &quiet)
	closeEmpty()
	if strings.Contains(quiet.String(), "taken to have had members") {
		t.Errorf("a store with no groups logged an adoption: %s", quiet.String())
	}

	// Then a store with offsets and no record of members (an earlier version's).
	dir := t.TempDir()
	m, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTopic("a", 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := m.CommitOffset("old", "a", 0, 3, ""); err != nil {
		t.Fatal(err)
	}
	m.Close()

	var said bytes.Buffer
	e, closeOld := open(dir, &said)
	defer closeOld()
	if !strings.Contains(said.String(), "taken to have had members") || !strings.Contains(said.String(), "groups=1") {
		t.Errorf("the adoption of one group was not logged: %s", said.String())
	}
	e.mu.Lock()
	st := e.groups["old"]
	e.mu.Unlock()
	if !st.Live || !st.Classic || !st.presumed || st.Session != 45*time.Second {
		t.Errorf("the adopted group is %+v, want a live classic one, presumed, with a 45 s session", st)
	}
}
