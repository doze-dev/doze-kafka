package server_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/doze-dev/doze-kafka/server"
)

// A broker for a test: a throwaway data directory, a free loopback port, and any
// Kafka client pointed at Addr.
func ExampleNew() {
	dir, _ := os.MkdirTemp("", "doze-kafka-example")
	defer os.RemoveAll(dir)

	srv, err := server.New(server.Options{DataDir: dir})
	if err != nil {
		panic(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	<-srv.Ready() // the listener is bound; Addr is valid.
	defer func() { stop(); <-done }()

	cl, err := kgo.NewClient(kgo.SeedBrokers(srv.Addr().String()), kgo.AllowAutoTopicCreation(),
		kgo.ConsumeTopics("greetings"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cl.ProduceSync(pctx, &kgo.Record{Topic: "greetings", Value: []byte("hello")}).FirstErr(); err != nil {
		panic(err)
	}
	cl.PollFetches(pctx).EachRecord(func(r *kgo.Record) { fmt.Println(string(r.Value)) })
	// Output: hello
}
