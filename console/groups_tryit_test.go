package console

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Gr-02: the Lag tab ends in "Try it", the fields OffsetCommit takes. It works for an
// empty group and says why it does not for one with members; and the neighbours well
// names the members and the schema of what the group reads.
func TestGroupLagTabHasTryItAndItsNeighboursNameMembersAndSubject(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	topicWith(t, c, "orders", 1, 6)
	register(t, c, "orders-value", userV1)
	commitFor(t, addr, "idle", "orders", map[int32]int64{0: 2})

	body := req(t, c, "GET", "/_console/groups/idle", nil).Body.String()
	form := between(body, `id="try-offset"`, "</form>")
	for _, want := range []string{"Try it", "the same fields OffsetCommit and the admin API take", `name="topic"`, `value="orders"`, `name="partition"`, `name="offset"`, `value="2"`, "Set offset"} {
		if !strings.Contains(form, want) {
			t.Errorf("the Try it form does not show %q:\n%s", want, form)
		}
	}
	if strings.Contains(form, " disabled>") {
		t.Errorf("an empty group's Try it is disabled:\n%s", form)
	}
	// The schema of what the group reads is a neighbour.
	nb := between(body, `id="neighbours"`, `<nav class="tabs"`)
	for _, want := range []string{"reads", ">orders</span>", "c-sc", ">orders-value</span>"} {
		if !strings.Contains(nb, want) {
			t.Errorf("the neighbours do not say %q:\n%s", want, nb)
		}
	}

	// It sets the offset it says it sets.
	rec := req(t, c, "POST", "/_console/groups/idle/offset", url.Values{"topic": {"orders"}, "partition": {"0"}, "offset": {"4"}, "tab": {"lag"}})
	if rec.Code != 200 {
		t.Fatalf("Set offset: %d %.200s", rec.Code, rec.Body)
	}
	if offs, _ := c.be.GroupOffsets(context.Background(), "idle"); len(offs) != 1 || offs[0].Committed != 4 {
		t.Fatalf("the committed offset is %+v, want 4", offs)
	}

	// With a member connected the form is disabled and says why, and the member is named.
	commitFor(t, addr, "busy", "orders", map[int32]int64{0: 1})
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID("billing-worker"), kgo.ConsumerGroup("busy"), kgo.ConsumeTopics("orders"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	cl.PollFetches(ctx)
	cancel()
	var busy string
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		busy = req(t, c, "GET", "/_console/groups/busy", nil).Body.String()
		if strings.Contains(busy, "member is connected") {
			break
		}
	}
	form = between(busy, `id="try-offset"`, "</form>")
	if !strings.Contains(form, "disabled while 1 member is connected: stop the consumers first") || !strings.Contains(form, `<button class="btn" type="submit" disabled>`) {
		t.Errorf("a group with a member does not disable Try it and say why:\n%s", form)
	}
	nb = between(busy, `id="neighbours"`, `<nav class="tabs"`)
	if !strings.Contains(nb, "members") || !strings.Contains(nb, ">billing-worker</span>") {
		t.Errorf("the neighbours do not name the member:\n%s", nb)
	}
}
