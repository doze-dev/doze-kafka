package traffic

import (
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// A fetch from v13 names its topics by id alone. The partitions the inspector lists
// for it carry the topic's name all the same: from the broker's answer when there is
// one, from the recorder's own lookup when there is not (they were blank).
func TestAFetchByTopicIDListsItsPartitionsByName(t *testing.T) {
	id := [16]byte{7}
	r := New(10, func(got [16]byte) string {
		if got == id {
			return "orders"
		}
		return ""
	})
	q := kmsg.NewPtrFetchRequest()
	q.SetVersion(13)
	tp := kmsg.NewFetchRequestTopic()
	tp.TopicID = id
	tp.Partitions = append(tp.Partitions, kmsg.NewFetchRequestTopicPartition())
	q.Topics = append(q.Topics, tp)
	r.Request(obs(1, "app", kmsg.Fetch, 0, func(o *protocol.Observation) { o.Req, o.TopicIDs = q, [][16]byte{id} }))
	got := r.Events(Filter{}, 0)
	if len(got) != 1 || len(got[0].Facts.Partitions) != 1 || got[0].Facts.Partitions[0].Topic != "orders" {
		t.Fatalf("partitions = %+v", got[0].Facts.Partitions)
	}
}
