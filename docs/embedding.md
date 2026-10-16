# Embedding doze-kafka

doze-kafka is a Go library as well as a binary. Tests that need a real Kafka can
start one in-process: no Docker, no ports to pick, no cleanup beyond cancelling a
context.

```go
import (
	"context"
	"testing"

	"github.com/doze-dev/doze-kafka/server"
)

func TestWithKafka(t *testing.T) {
	srv, err := server.New(server.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	<-srv.Ready()                        // the listener is bound
	brokers := srv.Addr().String()       // 127.0.0.1:<a free port>
	t.Cleanup(func() { stop(); <-done }) // flushes logs, closes storage

	t.Log("point any Kafka client at", brokers)
}
```

Point any Kafka client at `brokers`. `ExampleNew` in `server/example_test.go` is a
complete, runnable version that produces and consumes a record. The code above
is compiled and run by `TestEmbeddingDocCompiles`, so it cannot rot.

## What the defaults give you

- **Loopback, free port.** With no `ListenAddr` the broker binds `127.0.0.1:0`, so
  parallel tests never fight over 9092 and nothing is reachable from the network.
  `Addr()` reports the address actually bound, and Metadata advertises that same
  port, so a client that connects stays connected.
- **Self-contained data.** The metadata database lives at `<DataDir>/meta.db`.
  `t.TempDir()` therefore leaves nothing behind.
- **Topics appear on first use** (`AutoCreateTopics`, one partition), as with the
  binary, for a client that asks for it in its Metadata request. The Java client,
  Sarama and librdkafka do by default; franz-go does only with
  `kgo.AllowAutoTopicCreation()`, and without it a produce to a new topic fails with
  `UNKNOWN_TOPIC_OR_PARTITION`.

## Lifecycle

| Call | What it does |
|---|---|
| `New(opts)` | Validates, opens storage. Does not listen. If it returns an error it has closed whatever it had opened: the same data directory can be used at once. |
| `Bind(ctx)` | Opens the listener and returns its address. Optional: `Serve` binds if you did not. If it fails, the server still holds its storage: call `Close`. |
| `AlsoListen(ctx, addr)` | Opens a second address serving the same broker. Before `Serve` only; afterwards it returns an error. Clients are still told the main listener's port. Closed when `Serve` returns, or by `Close`. |
| `Serve(ctx)` | Accepts until `ctx` is cancelled (or `Close` is called), drains connections, flushes the logs and closes storage. Returns the first error from that, including a failed final fsync. However it returns, error or not, it has closed every listener (the `AlsoListen` ones too), storage and the lock on the data directory. A server is served once: a second `Serve` returns an error. |
| `Ready()` | A channel closed when the outcome of starting is known: once the listener is bound, or once the server has ended without ever binding (`Serve` returned first, or `Close` was called). Waiting on it never hangs. Afterwards `Addr() != nil` means ready, and `Addr() == nil` means it ended without becoming ready. |
| `Addr()` | The bound address. Nil before `Bind`/`Serve`, and for good on a server that ended without binding. |
| `Close()` | Releases everything: any listener `Bind` or `AlsoListen` opened, storage and the lock on the data directory. On a server that is serving it also stops it, as cancelling the context does, and returns once `Serve` has drained and closed storage. Idempotent, safe from any goroutine and after `Serve`; a waiter on `Ready` is released. |
| `ServeListener(ctx, ln)` | `Serve` on a listener you provide, such as a unix socket. Kafka framing does not care what carries it. Given a TCP listener, clients are told its port. The listener becomes the server's and is closed when the call returns, except when the call is refused (already serving, or closed): then it was not touched. |

Waiting for a broker that may fail to start:

```go
import (
	"context"
	"net"
	"testing"

	"github.com/doze-dev/doze-kafka/server"
)

// start serves a broker and returns its address, or why it never came up.
func start(ctx context.Context, opts server.Options) (addr string, done <-chan error, err error) {
	srv, err := server.New(opts)
	if err != nil {
		return "", nil, err
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx) }()
	<-srv.Ready()
	if srv.Addr() == nil {
		return "", nil, <-served // it ended without binding: this is why
	}
	return srv.Addr().String(), served, nil
}

func TestABrokerThatCannotStartSaysWhy(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	if _, _, err := start(context.Background(), server.Options{DataDir: t.TempDir(), ListenAddr: taken.Addr().String()}); err == nil {
		t.Fatal("a broker on a port in use started")
	}

	ctx, stop := context.WithCancel(context.Background())
	addr, done, err := start(ctx, server.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("serving on", addr)
	stop()
	<-done
}
```

Every exit is held by a test: `TestAFailureAtAnyStepOfConstructionReleasesEverything`
fails each step of `New`, `Bind` and `Serve` in turn and then counts goroutines and
open file descriptors and opens the same data directory again.
| `Config()` | The effective configuration as a plain `server.Config`. |
| `SchemaRegistry()` | The built-in Confluent-compatible schema registry as an `http.Handler`, for you to serve on a listener of your own. Its schemas are kept in the broker's metadata database; when the broker requires SASL, the handler requires the same users as HTTP basic authentication. Valid until the server is closed. |

## Options

| Option | Meaning |
|---|---|
| `DataDir` | Required: `New` returns an error without it. Partition logs. One broker per directory; a second `New` on the same one fails. |
| `MetaDB` | Defaults to `<DataDir>/meta.db`. |
| `Version` | Kafka major to emulate, 1–4. 0 = default (4). |
| `ListenAddr` | Default `127.0.0.1:0`. |
| `AdvertisedHost`, `AdvertisedPort` | What clients are told to dial. Set when the broker sits behind a proxy. The host is a name or an IP address, without a port. |
| `AutoCreateTopics`, `DefaultPartitions` | Topic behaviour. |
| `RetentionBytes`, `RetentionMs` | Delete-only retention. 0 = unbounded. |
| `SegmentBytes`, `IndexIntervalBytes` | Storage tuning. Set `SegmentBytes` small to test retention or rolling. |
| `FsyncInterval` | Periodic fsync of active segments. **Zero means the default (500ms), not "off"**: an options struct's zero value is the default. Use a negative value to disable. `acks=all` always fsyncs. |
| `ConsumerSessionTimeout` | How long a silent group member lives. Set low to test failure handling. |
| `SASLMechanisms`, `Credentials`, `TLSConfig` | Authentication and TLS; independent and composable. `Config().TLS` reports whether the listener is TLS. |
| `Logger` | nil discards. Request lines are Debug. |

The CLI differs from this in one place: `--fsync-interval 0` disables periodic
fsync, where the zero `Options` value keeps the default. Both are deliberate.

## Behind a proxy or on a unix socket

The doze module does this: it passes a unix-socket listener to `ServeListener`
and sets `AdvertisedHost`/`AdvertisedPort` to the address clients reach the proxy
on, so Metadata sends them there rather than to the socket.

## What is not here

The embed API is the broker, and the schema registry as a handler you serve yourself
(`SchemaRegistry()`, above). The web console is a separate package
(`github.com/doze-dev/doze-kafka/console`) that talks to a broker as any Kafka
client would; embedding it is optional. A console started that way shows topics,
messages and groups, and no Traffic: what clients asked is kept inside the broker,
in types this API does not export, and a broker built with `server.New` keeps none
of it and offers no client-telemetry APIs (`TestEmbeddedBrokerDoesNotAdvertiseTelemetry`).
The doze module, which lives in this repository and so can reach the broker's
internals, builds its broker through `internal/embed.NewWithConsole` when it starts a
console, and that one has Traffic and client metrics as the `doze-kafka` command's does.

## The surface is pinned

`task api` writes the exported surface of `server/` to `testdata/api/server.txt`
and fails when the code and the file differ, or when the commit that changed the
file has no `CHANGELOG.md` line containing "server API". `Server.Traffic()` and
`Server.ClientMetrics()` are not on it: they returned internal types that only
this module's own commands could use. `NewFromConfig` is not on it either: it
took `internal/config.Config`, which an embedder cannot name, so it was never callable
from outside. `server.New(server.Options{...})` is the one constructor (Options carries
`SegmentBytes`, `IndexIntervalBytes`, `ConsumerSessionTimeout` and `FsyncInterval`);
`Config()` returns the public `server.Config`. This module's commands build from the
internal config through `internal/embed`.
