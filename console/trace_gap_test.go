package console

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Records planned but gone (retention since the plan): the scan waits one empty poll
// to be sure, then stops with what it found; it does not call that a timeout.
func TestScanRangesStopsAtAGapAfterOneEmptyPoll(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "gap", 1, nil); err != nil {
		t.Fatal(err)
	}
	recs := make([]*kgo.Record, 6)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: "gap", Value: []byte("v"), Headers: tp(traceA)}
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	hits, timedOut, unread, err := c.scanRanges(ctx, &traceJob{trace: traceA}, []partRange{{tpKey{"gap", 0}, 0, 10}})
	el := time.Since(start)
	if err != nil || timedOut || len(unread) != 0 || len(hits) != 6 {
		t.Errorf("hits %d, timedOut %v, unread %v, err %v", len(hits), timedOut, unread, err)
	}
	if el < 1400*time.Millisecond || el > 6*time.Second {
		t.Errorf("the scan gave up after %v; it should wait out one empty poll (1.5 s) and no more", el)
	}
}
