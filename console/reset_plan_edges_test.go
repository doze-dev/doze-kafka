package console

import (
	"context"
	"strings"
	"testing"
)

// A commit on a partition the topic does not have is not kept, so a plan never meets an
// offset beyond the topic's partitions (the plan's own guard against one is for that case).
func TestAGroupHoldsNoOffsetOnAPartitionTheTopicLacks(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "shrunk", 2, 4)
	commitFor(t, addr, "gs", "shrunk", map[int32]int64{0: 3, 2: 5})
	// (This broker does not keep a commit on a partition the topic lacks, so the group holds one offset.)
	if got := groupOffsetsText(t, c, "gs"); got != "shrunk/0=3" {
		t.Errorf("the group holds %q", got)
	}
	rows, err := c.be.PlanReset(context.Background(), "gs", ResetSpec{To: "earliest"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Partition != 0 {
		t.Errorf("rows: %+v", rows)
	}
}

// The console names a topic's value schema the way serializers do, and says so on the page.
func TestSubjectNameRuleNamesTheValueSubject(t *testing.T) {
	if got := string(subjectNameRule()); !strings.Contains(got, "&lt;topic&gt;-value") {
		t.Errorf("%s", got)
	}
}
