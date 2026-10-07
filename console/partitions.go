package console

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// AddPartitions grows a topic to count partitions in total. Kafka never removes
// partitions, so count must be above the current number; the broker says so if not.
func (b *backend) AddPartitions(ctx context.Context, name string, count int32) error {
	req := kmsg.NewPtrCreatePartitionsRequest()
	t := kmsg.NewCreatePartitionsRequestTopic()
	t.Topic, t.Count = name, count
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return err
	}
	for _, r := range resp.Topics {
		if err := refusal(r.ErrorCode, r.ErrorMessage); err != nil {
			return err
		}
	}
	return nil
}

// topicPartitions adds partitions from the form on the topic's page. The form asks
// how many to ADD, not the new total: the page shows the current count beside it, and
// "add 2" cannot be mistaken for a shrink the way a stale total can.
func (c *Console) topicPartitions(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	add, err := strconv.Atoi(strings.TrimSpace(r.FormValue("add")))
	if err != nil || add < 1 {
		c.fail(w, r, badInput("%s", "enter how many partitions to add (1 or more)"))
		return
	}
	current, err := c.be.PartitionCount(r.Context(), name)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if err := c.be.AddPartitions(r.Context(), name, current+int32(add)); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.topicURL(name), fmt.Sprintf("Topic now has %d partitions", current+int32(add)))
}
