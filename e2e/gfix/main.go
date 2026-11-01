// Command gfix makes the group states a browser test needs that the console cannot make
// itself, with one Kafka request each.
//
//	gfix commit host:port group topic partition offset
//
// commits that offset for the group with no member (the group is then Empty), however far
// past the log end it is: the broker accepts it, as Kafka does.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/doze-dev/doze-kafka/e2e/connopt"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func main() {
	if len(os.Args) != 7 || os.Args[1] != "commit" {
		fmt.Fprintln(os.Stderr, "usage: gfix commit host:port group topic partition offset")
		os.Exit(2)
	}
	part, err1 := strconv.ParseInt(os.Args[5], 10, 32)
	off, err2 := strconv.ParseInt(os.Args[6], 10, 64)
	if err1 != nil || err2 != nil {
		fmt.Fprintln(os.Stderr, "partition and offset are numbers")
		os.Exit(2)
	}
	cl, err := kgo.NewClient(append(connopt.Opts(), kgo.SeedBrokers(os.Args[2]))...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group, req.Generation = os.Args[3], -1
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = os.Args[4]
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Partition, rp.Offset = int32(part), off
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || resp.Topics[0].Partitions[0].ErrorCode != 0 {
		fmt.Fprintln(os.Stderr, "commit failed:", err, resp)
		os.Exit(1)
	}
	fmt.Println("committed")
}
