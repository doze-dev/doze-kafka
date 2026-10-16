package broker_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Real clients of the two protocols in one group. What the broker is to do with a
// cooperative classic member and with a static one that restarts is recorded
// (conformance/recordings/mixed-protocol-cooperative.json, mixed-protocol-static.json);
// these hold the thing a user of it cares about: no partition is ever two members'.

// ownership is who the clients themselves believe owns each partition of one topic, kept
// from their assigned, revoked and lost callbacks. A member is told it owns a partition
// only after the member that had it has been told it does not.
type ownership struct {
	mu     sync.Mutex
	owner  map[int32]string
	broken []string
}

func newOwnership() *ownership { return &ownership{owner: map[int32]string{}} }

// callbacks are the options that report member name's partitions to o.
func (o *ownership) callbacks(name string) []kgo.Opt {
	gone := func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
		o.mu.Lock()
		defer o.mu.Unlock()
		for _, ps := range lost {
			for _, p := range ps {
				if o.owner[p] == name {
					delete(o.owner, p)
				}
			}
		}
	}
	return []kgo.Opt{
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, got map[string][]int32) {
			o.mu.Lock()
			defer o.mu.Unlock()
			for _, ps := range got {
				for _, p := range ps {
					if cur, held := o.owner[p]; held && cur != name {
						o.broken = append(o.broken, fmt.Sprintf("partition %d given to %s while %s still has it", p, name, cur))
					}
					o.owner[p] = name
				}
			}
		}),
		kgo.OnPartitionsRevoked(gone),
		kgo.OnPartitionsLost(gone),
	}
}

func (o *ownership) held() map[string]int {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := map[string]int{}
	for _, name := range o.owner {
		out[name]++
	}
	return out
}

func (o *ownership) violations() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.broken...)
}

// member starts a consumer of topic in group and polls it until the test ends or stop is
// called.
func member(t *testing.T, ctx context.Context, addr, group, topic string, opts ...kgo.Opt) (stop func()) {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(addr), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
		kgo.DialTimeout(2 * time.Second), kgo.HeartbeatInterval(200 * time.Millisecond),
	}, opts...)...)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	pollCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for pollCtx.Err() == nil {
			cl.PollFetches(pollCtx)
		}
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			<-done
			cl.Close()
		})
	}
	t.Cleanup(stop)
	return stop
}

func describeConsumerGroup(t *testing.T, ctx context.Context, cl *kgo.Client, group string) kmsg.ConsumerGroupDescribeResponseGroup {
	t.Helper()
	req := kmsg.NewPtrConsumerGroupDescribeRequest()
	req.Groups = []string{group}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Groups) != 1 {
		t.Fatalf("ConsumerGroupDescribe: %v %+v", err, resp)
	}
	return resp.Groups[0]
}

// eventually waits for what the clients hold to be want.
func (o *ownership) eventually(t *testing.T, what string, want map[string]int) {
	t.Helper()
	deadline := time.Now().Add(40 * time.Second)
	for {
		got := o.held()
		same := len(got) == len(want)
		for name, n := range want {
			same = same && got[name] == n
		}
		if same {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: the clients hold %v, want %v (violations: %v)", what, got, want, o.violations())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A cooperative-sticky classic consumer and a consumer-protocol one in one group: the
// classic one goes on reading through the rebalance and gives up half, and at no moment
// do the two clients both believe a partition is theirs.
func TestMixedGroup_CooperativeClassicMemberAndConsumerProtocolMemberNeverShareAPartition(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	defer admin.Close()
	mustCreateTopic(t, ctx, admin, "t", 4)

	own := newOwnership()
	member(t, ctx, addr, "g", "t", append(own.callbacks("classic"), kgo.Balancers(kgo.CooperativeStickyBalancer()))...)
	own.eventually(t, "the cooperative member alone", map[string]int{"classic": 4})

	stopNew := member(t, ctx, addr, "g", "t", append(own.callbacks("consumer"), kip848Opts()...)...)
	own.eventually(t, "after the consumer-protocol member joined", map[string]int{"classic": 2, "consumer": 2})
	if v := own.violations(); len(v) != 0 {
		t.Fatalf("a partition had two owners: %v", v)
	}
	g := describeConsumerGroup(t, ctx, admin, "g")
	seen := map[int32]string{}
	for _, m := range g.Members {
		for _, tp := range m.Assignment.TopicPartitions {
			for _, p := range tp.Partitions {
				if other, twice := seen[p]; twice {
					t.Errorf("the broker has partition %d assigned to %s and to %s", p, other, m.MemberID)
				}
				seen[p] = m.MemberID
			}
		}
	}
	if len(g.Members) != 2 || len(seen) != 4 {
		t.Errorf("the group has %d members holding %d partitions, want 2 and 4: %+v", len(g.Members), len(seen), g.Members)
	}

	// The consumer-protocol member leaves: the group is classic again and the
	// cooperative member has all four back, never having shared one.
	stopNew()
	own.eventually(t, "after the consumer-protocol member left", map[string]int{"classic": 4})
	if v := own.violations(); len(v) != 0 {
		t.Fatalf("a partition had two owners: %v", v)
	}
}

// A static classic consumer (group.instance.id) of a group with a consumer-protocol
// member restarts: it closes without leaving, as a static member does, and starts again.
// The instance has one seat throughout. It used to be seated a second time, its old seat
// keeping its partition until a 45-second session ran out.
func TestMixedGroup_StaticClassicMemberRestartingHasOneSeat(t *testing.T) {
	addr := startBroker(t, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin := newClient(t, addr)
	defer admin.Close()
	mustCreateTopic(t, ctx, admin, "t", 2)

	own := newOwnership()
	member(t, ctx, addr, "g", "t", append(own.callbacks("consumer"), kip848Opts()...)...)
	own.eventually(t, "the consumer-protocol member alone", map[string]int{"consumer": 2})

	static := append(own.callbacks("static"), kgo.InstanceID("i1"))
	stopFirst := member(t, ctx, addr, "g", "t", static...)
	own.eventually(t, "after the static classic member joined", map[string]int{"consumer": 1, "static": 1})
	seats := func() (members, instances int) {
		g := describeConsumerGroup(t, ctx, admin, "g")
		for _, m := range g.Members {
			if m.InstanceID != nil && *m.InstanceID == "i1" {
				instances++
			}
		}
		return len(g.Members), instances
	}
	if members, instances := seats(); members != 2 || instances != 1 {
		t.Fatalf("before the restart the group has %d members, %d of them the instance", members, instances)
	}

	stopFirst() // the process goes away; a static member does not leave.
	member(t, ctx, addr, "g", "t", static...)
	deadline := time.Now().Add(20 * time.Second)
	for {
		members, instances := seats()
		if members == 2 && instances == 1 && own.held()["static"] == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the restart the group has %d members, %d of them the instance; the clients hold %v", members, instances, own.held())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if v := own.violations(); len(v) != 0 {
		t.Fatalf("a partition had two owners: %v", v)
	}
}
