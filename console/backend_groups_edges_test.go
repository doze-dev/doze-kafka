package console

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// groupFixture: topics gt (2 partitions, 5 and 3 records) and gu (1 partition, 4
// records), and a group g with commits on all of them .
func groupFixture(t *testing.T) (*Console, string, *kgo.Client) {
	t.Helper()
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	for name, parts := range map[string]int32{"gt": 2, "gu": 1} {
		if err := c.be.CreateTopic(ctx, name, parts, nil); err != nil {
			t.Fatal(err)
		}
	}
	bulkProduce(t, cl, "gt", 0, 5, vals("a"), time.Time{})
	bulkProduce(t, cl, "gt", 1, 3, vals("b"), time.Time{})
	bulkProduce(t, cl, "gu", 0, 4, vals("c"), time.Time{})
	commitFor(t, addr, "g", "gt", map[int32]int64{0: 2, 1: 1})
	commitFor(t, addr, "g", "gu", map[int32]int64{0: 1})
	return c, addr, cl
}

func groupOffsetsText(t *testing.T, c *Console, group string) string {
	t.Helper()
	os, err := c.be.GroupOffsets(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	var parts []string
	for _, o := range os {
		parts = append(parts, fmt.Sprintf("%s/%d=%d", o.Topic, o.Partition, o.Committed))
	}
	return strings.Join(parts, " ")
}

func TestGroupOffsetsAreSortedByTopicThenPartition(t *testing.T) {
	c, _, _ := groupFixture(t)
	if got, want := groupOffsetsText(t, c, "g"), "gt/0=2 gt/1=1 gu/0=1"; got != want {
		t.Errorf("offsets %q, want %q", got, want)
	}
}

func TestResetGroupOffsets(t *testing.T) {
	c, _, cl := groupFixture(t)
	ctx := context.Background()
	deleteBefore(t, cl, "gt", 0, 1)

	// One topic, to the earliest retained offset; the other topic is untouched, and an
	// offset on a partition that does not exist is left alone.
	if n, err := c.be.ResetGroupOffsets(ctx, "g", "earliest", "gt"); err != nil || n != 2 {
		t.Fatalf("reset gt to earliest: %d %v", n, err)
	}
	if got, want := groupOffsetsText(t, c, "g"), "gt/0=1 gt/1=0 gu/0=1"; got != want {
		t.Errorf("after earliest on gt: %q, want %q", got, want)
	}
	// Every topic, to the end.
	if n, err := c.be.ResetGroupOffsets(ctx, "g", "latest", ""); err != nil || n != 3 {
		t.Fatalf("reset all to latest: %d %v", n, err)
	}
	if got, want := groupOffsetsText(t, c, "g"), "gt/0=5 gt/1=3 gu/0=4"; got != want {
		t.Errorf("after latest: %q, want %q", got, want)
	}
	if _, err := c.be.ResetGroupOffsets(ctx, "g", "earliest", "nowhere"); err == nil || !strings.Contains(err.Error(), `no committed offsets on topic "nowhere"`) {
		t.Errorf("a topic the group has nothing on: %v", err)
	}
	if _, err := c.be.ResetGroupOffsets(ctx, "g", "middle", ""); err == nil {
		t.Error("reset to a position that is neither earliest nor latest")
	}
	if _, err := c.be.ResetGroupOffsets(ctx, "ghost", "earliest", ""); err == nil {
		t.Error("reset of a group that does not exist")
	}
}

func TestSetGroupOffsetBounds(t *testing.T) {
	c, _, cl := groupFixture(t)
	ctx := context.Background()
	deleteBefore(t, cl, "gt", 0, 2)
	set := func(topic string, part int32, to string, off int64) (int64, error) {
		return c.be.SetGroupOffset(ctx, "g", topic, part, to, off)
	}
	if got, err := set("gt", 0, "", 2); err != nil || got != 2 {
		t.Errorf("the first retained offset: %d %v", got, err)
	}
	if _, err := set("gt", 0, "", 1); err == nil || !strings.Contains(err.Error(), "outside partition 0") {
		t.Errorf("an offset before the log start: %v", err)
	}
	if got, err := set("gt", 0, "", 5); err != nil || got != 5 {
		t.Errorf("the end of the log: %d %v", got, err)
	}
	if _, err := set("gt", 0, "", 6); err == nil {
		t.Error("an offset past the end")
	}
	if _, err := set("gt", 2, "", 0); err == nil || !strings.Contains(err.Error(), "has no partition 2") {
		t.Errorf("the partition one past the last: %v", err)
	}
	if _, err := set("gt", -1, "", 0); err == nil {
		t.Error("a negative partition")
	}
	if got, err := set("gt", 0, "earliest", 0); err != nil || got != 2 {
		t.Errorf("earliest: %d %v", got, err)
	}
	if got, err := set("gt", 1, "latest", 0); err != nil || got != 3 {
		t.Errorf("latest: %d %v", got, err)
	}
	if _, err := set("gt", 0, "sideways", 0); err == nil {
		t.Error("an unknown position")
	}
	if got, want := groupOffsetsText(t, c, "g"), "gt/0=2 gt/1=3 gu/0=1"; got != want {
		t.Errorf("committed %q, want %q", got, want)
	}
}

// What commitOffsets reports when the broker takes some, all or none.
func TestCommitOffsetsReportsWhatTheBrokerTook(t *testing.T) {
	c, _, _ := groupFixture(t)
	ctx := context.Background()
	n, err := c.be.commitOffsets(ctx, "h", []OffsetTarget{{"gt", 0, 1}, {"gu", 0, 2}})
	if err != nil || n != 2 {
		t.Errorf("both taken: %d %v", n, err)
	}
	if got, want := groupOffsetsText(t, c, "h"), "gt/0=1 gu/0=2"; got != want {
		t.Errorf("committed %q, want %q", got, want)
	}
	n, err = c.be.commitOffsets(ctx, "h", []OffsetTarget{{"gt", 0, 3}, {"nosuchtopic", 0, 2}})
	if err == nil || n != 1 || !strings.Contains(err.Error(), "1 of 2 offsets were written") {
		t.Errorf("one refused: %d %v", n, err)
	}
	n, err = c.be.commitOffsets(ctx, "h", []OffsetTarget{{"nosuchtopic", 0, 2}})
	if err == nil || n != 0 || strings.Contains(err.Error(), "were written") {
		t.Errorf("all refused: %d %v", n, err)
	}
}

func TestTopicConsumersLagAndForeignPartitions(t *testing.T) {
	c, _, _ := groupFixture(t)
	got, err := c.be.TopicConsumers(context.Background(), "gu")
	if err != nil || len(got) != 1 {
		t.Fatalf("%+v %v", got, err)
	}
	// 4 records, committed 1 on partition 0; the commit on a partition gu lacks counts for nothing.
	if got[0].Group != "g" || got[0].Lag != 3 || got[0].Members != 0 {
		t.Errorf("%+v", got[0])
	}
	got, err = c.be.TopicConsumers(context.Background(), "gt")
	if err != nil || len(got) != 1 || got[0].Lag != (5-2)+(3-1) {
		t.Errorf("gt: %+v %v", got, err)
	}
}

// ListGroups: sorted by ID, and each protocol's describe is asked only if a group of
// that protocol exists.
func TestListGroupsOrderAndWhichDescribesAreAsked(t *testing.T) {
	c, sn := newSniffedConsole(t)
	ctx := context.Background()
	if gs, err := c.be.ListGroups(ctx); err != nil || len(gs) != 0 {
		t.Fatalf("%v %v", gs, err)
	}
	if n := len(sn.sent(15)) + len(sn.sent(69)); n != 0 {
		t.Errorf("%d describes for no groups", n)
	}
	if err := c.be.CreateTopic(ctx, "t", 1, nil); err != nil {
		t.Fatal(err)
	}
	// Commit through the console's own connection, so the groups exist.
	for _, g := range []string{"b", "c", "a"} {
		if _, err := c.be.commitOffsets(ctx, g, []OffsetTarget{{"t", 0, 0}}); err != nil {
			t.Fatal(err)
		}
	}
	gs, err := c.be.ListGroups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, g := range gs {
		ids = append(ids, g.ID+":"+g.Protocol)
	}
	if strings.Join(ids, ",") != "a:classic,b:classic,c:classic" {
		t.Errorf("groups %v", ids)
	}
	if n := len(sn.sent(15)); n != 1 {
		t.Errorf("%d DescribeGroups requests for classic groups, want 1", n)
	}
	if n := len(sn.sent(69)); n != 0 {
		t.Errorf("%d ConsumerGroupDescribe requests with no consumer-protocol group", n)
	}
}

// A group page's members, whichever protocol the group speaks.
func TestDescribeGroupMembersOfBothProtocols(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "d", 2, nil); err != nil {
		t.Fatal(err)
	}
	var betaKey any = "opt_in_kafka_next_gen_balancer_beta"
	join := func(group, client string, extra ...kgo.Opt) {
		cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(addr), kgo.ConsumerGroup(group), kgo.ConsumeTopics("d"), kgo.ClientID(client)}, extra...)...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cl.Close)
		go func() {
			cctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			cl.PollFetches(cctx)
		}()
	}
	join("classic", "classic-client")
	join("next", "next-client",
		kgo.WithContext(context.WithValue(context.Background(), betaKey, true)),
		kgo.GroupProtocol("consumer"), kgo.Balancers(kgo.StickyBalancer()))

	wait := func(id string) *GroupDetail {
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			d, err := c.be.DescribeGroup(ctx, id)
			if err == nil && len(d.Members) == 1 && len(d.Members[0].Assigned["d"]) == 2 {
				return d
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("group %s never showed its member with both partitions", id)
		return nil
	}
	d := wait("classic")
	if d.Protocol != "classic" || d.Members[0].ClientID != "classic-client" || strings.HasPrefix(d.Members[0].Host, "/") || d.Members[0].Host == "" {
		t.Errorf("classic: %+v", d)
	}
	d = wait("next")
	if d.Protocol != "848" || d.Assignor == "" || d.Members[0].ClientID != "next-client" || strings.HasPrefix(d.Members[0].Host, "/") {
		t.Errorf("848: %+v", d)
	}
	if _, err := c.be.DescribeGroup(ctx, "nobody"); err == nil {
		t.Error("a group that does not exist")
	}
}
