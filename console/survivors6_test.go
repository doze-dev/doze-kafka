package console

import (
	"context"
	"testing"
	"time"
)

// A purge whose request gets no answer is an error, not a purge that did nothing.
func TestPurgeThatNeverAnswersIsAnError(t *testing.T) {
	c, sn := newSniffedConsole(t)
	if err := c.be.CreateTopic(context.Background(), "unanswered", 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.be.Produce(context.Background(), "unanswered", 0, nil, []byte("v"), nil, "1"); err != nil {
		t.Fatal(err)
	}
	sn.blackhole(21) // DeleteRecords
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	if err := c.be.PurgeTopic(ctx, "unanswered"); err == nil {
		t.Error("a purge nobody answered reports success")
	}
	bs, err := c.be.PartitionBounds(context.Background(), "unanswered", 1)
	if err != nil || bs[0].Count() != 1 {
		t.Errorf("the topic was emptied without an answer: %+v %v", bs, err)
	}
}
