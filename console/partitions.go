package console

import (
	"context"
	"fmt"
	"math"
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

// topicPartitions grows a topic from the form on its Partitions tab (Tp-17). The form asks
// the new TOTAL, as CreatePartitions does (TOP-30); a total at or below the present one is
// sent as it is and the broker's refusal (INVALID_PARTITIONS) is shown in its own words.
// "add" is still read, for a caller that says how many to add.
func (c *Console) topicPartitions(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if raw := strings.TrimSpace(r.FormValue("total")); raw != "" {
		total, err := strconv.Atoi(raw)
		if err != nil || total < 1 || total > math.MaxInt32 {
			c.fail(w, r, badInput("%s", "enter the number of partitions the topic should have in total (1 or more)"))
			return
		}
		if err := c.be.AddPartitions(r.Context(), name, int32(total)); err != nil {
			c.fail(w, r, err)
			return
		}
		c.redirect(w, r, c.topicURL(name)+"?tab=partitions", fmt.Sprintf("Topic now has %d partitions", total))
		return
	}
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
	c.redirect(w, r, c.topicURL(name)+"?tab=partitions", fmt.Sprintf("Topic now has %d partitions", current+int32(add)))
}
