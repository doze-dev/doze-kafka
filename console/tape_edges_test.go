package console

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type tapeFixture struct {
	c    *Console
	addr string
	cl   *kgo.Client
	id   string
}

func (f tapeFixture) tape(t *testing.T, topic string, partition, count int32, from, hl, jump int64, committed bool) map[string]any {
	t.Helper()
	d, err := f.c.tapeData(context.Background(), topic, f.id, partition, count, from, hl, jump, committed)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func offsetsOfRows(vs []msgView) string {
	var out []string
	for _, v := range vs {
		out = append(out, fmt.Sprint(v.Offset))
	}
	return strings.Join(out, ",")
}

// The window the tape shows: where it starts, which way it can be paged, and what it
// says when asked for an offset the log does not hold.
func TestTapeWindowsAndPaging(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "tw", 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.be.CreateTopic(ctx, "empty", 1, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "tw", 0, 130, vals("m"), time.Time{})
	f := tapeFixture{c: c, addr: addr, cl: cl, id: "id1"}

	d := f.tape(t, "tw", 0, 1, -1, -1, -1, false)
	msgs, rows := d["Messages"].([]msgView), d["Rows"].([]msgView)
	if len(msgs) != 60 || msgs[0].Offset != 70 || msgs[59].Offset != 129 || rows[0].Offset != 129 || rows[59].Offset != 70 {
		t.Fatalf("tail window: %s | %s", offsetsOfRows(msgs[:2]), offsetsOfRows(rows[:2]))
	}
	for i, m := range msgs {
		if m.Hot != (i >= 57) {
			t.Errorf("message %d hot=%v: the newest three are hot at the tail", i, m.Hot)
		}
	}
	if d["AtTail"] != true || d["HasOlder"] != true || d["HasNewer"] != false || d["OlderFrom"] != int64(10) || d["NewerFrom"] != int64(130) ||
		d["WindowFrom"] != int64(70) || d["WindowLast"] != int64(129) || d["Count"] != int64(130) || d["Earliest"] != int64(0) || d["Latest"] != int64(130) {
		t.Errorf("tail: %v", d)
	}
	if d["PartSelect"] != false || d["HasTxn"] != false || d["Hash"] != "id1|0-130," {
		t.Errorf("tail: PartSelect %v HasTxn %v Hash %v", d["PartSelect"], d["HasTxn"], d["Hash"])
	}

	// The first offset that is still the tail window, and the one before it.
	if d = f.tape(t, "tw", 0, 1, 70, -1, -1, false); d["AtTail"] != true || d["WindowFrom"] != int64(70) {
		t.Errorf("from=70: AtTail %v", d["AtTail"])
	}
	d = f.tape(t, "tw", 0, 1, 69, -1, -1, false)
	if d["AtTail"] != false || d["HasNewer"] != true || d["WindowFrom"] != int64(69) || d["WindowLast"] != int64(128) || d["NewerFrom"] != int64(129) || d["OlderFrom"] != int64(9) || d["HasOlder"] != true {
		t.Errorf("from=69: %v", d)
	}
	if ms := d["Messages"].([]msgView); len(ms) != 60 || ms[0].Offset != 69 || ms[0].Hot || ms[59].Hot {
		t.Errorf("a paged window is never hot: %v", offsetsOfRows(ms[:1]))
	}
	d = f.tape(t, "tw", 0, 1, 0, 33, -1, false)
	if d["HasOlder"] != false || d["OlderFrom"] != int64(0) || d["WindowLast"] != int64(59) || d["NewerFrom"] != int64(60) || d["HL"] != int64(33) {
		t.Errorf("from=0: %v", d)
	}

	// Asking for an offset.
	d = f.tape(t, "tw", 0, 1, -1, -1, 50, false)
	if d["WindowFrom"] != int64(30) || d["HL"] != int64(50) || d["JumpNote"] != "" || d["AtTail"] != false {
		t.Errorf("jump to 50: from %v hl %v note %q", d["WindowFrom"], d["HL"], d["JumpNote"])
	}
	d = f.tape(t, "tw", 0, 1, -1, -1, 5, false)
	if d["WindowFrom"] != int64(0) || d["HL"] != int64(5) {
		t.Errorf("jump to 5: from %v hl %v", d["WindowFrom"], d["HL"])
	}
	// The newest record is in the tail window, which stays the tail.
	d = f.tape(t, "tw", 0, 1, -1, -1, 129, false)
	if d["JumpNote"] != "" || d["HL"] != int64(129) || d["AtTail"] != true || d["WindowFrom"] != int64(70) {
		t.Errorf("jump to the newest: %v %v %v %v", d["JumpNote"], d["HL"], d["AtTail"], d["WindowFrom"])
	}
	d = f.tape(t, "tw", 0, 1, 20, 7, 130, false)
	if d["JumpNote"] != "Offset 130 has not been written yet (the newest is 129) — showing the latest messages." || d["AtTail"] != true || d["HL"] != int64(-1) {
		t.Errorf("jump to the end: %v %v %v", d["JumpNote"], d["AtTail"], d["HL"])
	}
	if d = f.tape(t, "tw", 0, 1, -1, -1, jumpMalformed, false); d["JumpNote"] != "That is not an offset. Type a whole number, 0 or more." || d["AtTail"] != true {
		t.Errorf("a malformed jump: %v", d["JumpNote"])
	}
	d = f.tape(t, "empty", 0, 1, -1, -1, 3, false)
	if d["JumpNote"] != "This partition has no messages to jump to." || len(d["Messages"].([]msgView)) != 0 || d["HasOlder"] != false || d["WindowLast"] != int64(0) {
		t.Errorf("an empty partition: %v", d)
	}

	// A log that starts above zero.
	deleteBefore(t, cl, "tw", 0, 40)
	d = f.tape(t, "tw", 0, 1, -1, -1, 10, false)
	if want := "Offset 10 is before the start of the log (the oldest is 40) — showing from there."; d["JumpNote"] != want || d["WindowFrom"] != int64(40) {
		t.Errorf("a jump before the log start: %q from %v", d["JumpNote"], d["WindowFrom"])
	}
	d = f.tape(t, "tw", 0, 1, 40, -1, -1, false)
	if d["HasOlder"] != false || d["OlderFrom"] != int64(40) || d["Count"] != int64(90) || d["Earliest"] != int64(40) {
		t.Errorf("from the log start: %v", d)
	}
	d = f.tape(t, "tw", 0, 1, 41, -1, -1, false)
	if d["HasOlder"] != true || d["OlderFrom"] != int64(40) {
		t.Errorf("from just past the log start: %v %v", d["HasOlder"], d["OlderFrom"])
	}
	// Jumping near the log start does not reach below it.
	if d = f.tape(t, "tw", 0, 1, -1, -1, 45, false); d["WindowFrom"] != int64(40) || d["HL"] != int64(45) {
		t.Errorf("jump to 45 of a log starting at 40: from %v", d["WindowFrom"])
	}
}

func TestTapeOfAManyPartitionTopicOffersASelect(t *testing.T) {
	c, _ := newTestConsoleAddr(t)
	ctx := context.Background()
	for n, want := range map[int32]bool{24: false, 25: true} {
		name := fmt.Sprintf("many%d", n)
		if err := c.be.CreateTopic(ctx, name, n, nil); err != nil {
			t.Fatal(err)
		}
		d, err := c.tapeData(ctx, name, "i", 0, n, -1, -1, -1, false)
		if err != nil || d["PartSelect"] != want {
			t.Errorf("%d partitions: PartSelect %v (%v)", n, d["PartSelect"], err)
		}
	}
}

// Transactions on the tape: aborted and open records are marked, markers are rows that
// are not records, and the committed view leaves them out.
func TestTapeMarksTransactions(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "tx", 1, nil); err != nil {
		t.Fatal(err)
	}
	p := txnProducer(t, addr, "tape")
	step := func(value string, end kgo.TransactionEndTry, finish bool) {
		if err := p.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		txnWrite(t, ctx, p, "tx", value)
		if finish {
			if err := p.EndTransaction(ctx, end); err != nil {
				t.Fatal(err)
			}
		}
	}
	step("kept", kgo.TryCommit, true)  // offsets 0 and 1 (marker)
	step("gone", kgo.TryAbort, true)   // 2 and 3 (marker)
	step("open", kgo.TryCommit, false) // 4
	f := tapeFixture{c: c, addr: addr, id: "i"}

	d := f.tape(t, "tx", 0, 1, -1, -1, -1, false)
	msgs, rows := d["Messages"].([]msgView), d["Rows"].([]msgView)
	var got []string
	for _, m := range msgs {
		got = append(got, fmt.Sprintf("%d:%s", m.Offset, m.Txn))
	}
	if strings.Join(got, " ") != "0: 2:aborted 4:open" {
		t.Errorf("records %v", got)
	}
	if offsetsOfRows(rows) != "4,3,2,1,0" || !rows[1].Marker || !rows[3].Marker || rows[0].Marker || rows[2].Marker {
		t.Errorf("rows %s", offsetsOfRows(rows))
	}
	if d["HasTxn"] != true || d["CommittedOnly"] != false {
		t.Errorf("HasTxn %v", d["HasTxn"])
	}
	d = f.tape(t, "tx", 0, 1, -1, -1, -1, true)
	if offsetsOfRows(d["Rows"].([]msgView)) != "0" || offsetsOfRows(d["Messages"].([]msgView)) != "0" || d["CommittedOnly"] != true {
		t.Errorf("committed only: %s", offsetsOfRows(d["Rows"].([]msgView)))
	}
}

// A record whose transaction aborted is marked aborted wherever the window falls.
func TestTapeMarksAnAbortedRecordWhoseMarkerIsOutsideTheWindow(t *testing.T) {
	t.Skip("BUG, reported not fixed (production code is out of scope for this task): txnStatus reads the committed view only " +
		"when the window holds a marker or reaches the open part of the log, so an aborted record whose abort marker is " +
		"the first offset past the window is shown as an ordinary record. Remove this line when it is fixed.")
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "edge", 1, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "edge", 0, 59, vals("m"), time.Time{}) // offsets 0..58
	p := txnProducer(t, addr, "edge")
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	txnWrite(t, ctx, p, "edge", "aborted-last")                 // offset 59
	if err := p.EndTransaction(ctx, kgo.TryAbort); err != nil { // marker at 60
		t.Fatal(err)
	}
	bulkProduce(t, cl, "edge", 0, 5, vals("n"), time.Time{}) // 61..65
	f := tapeFixture{c: c, addr: addr, id: "i"}
	d := f.tape(t, "edge", 0, 1, 0, -1, -1, false) // [0,60): the marker at 60 is not in it.
	for _, m := range d["Messages"].([]msgView) {
		if m.Offset == 59 && m.Txn != "aborted" {
			t.Errorf("the record at 59 belongs to an aborted transaction and is shown as %q", m.Txn)
		}
	}
}
