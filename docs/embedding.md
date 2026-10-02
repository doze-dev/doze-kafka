# Embedding doze-kafka

doze-kafka is a Go library as well as a binary. Tests that need a real Kafka can
start one in-process: no Docker, no ports to pick, no cleanup beyond cancelling a
context.

```go
import "github.com/doze-dev/doze-kafka/server"

srv, err := server.New(server.Options{DataDir: t.TempDir()})
if err != nil {
    t.Fatal(err)
}
ctx, stop := context.WithCancel(context.Background())
done := make(chan error, 1)
go func() { done <- srv.Serve(ctx) }()
<-srv.Ready()                      // the listener is bound
brokers := srv.Addr().String()     // 127.0.0.1:<a free port>
t.Cleanup(func() { stop(); <-done }) // flushes logs, closes storage
```

Point any Kafka client at `brokers`. `ExampleNew` in `server/example_test.go` is a
complete, runnable version that produces and consumes a record.

## What the defaults give you

- **Loopback, free port.** With no `ListenAddr` the broker binds `127.0.0.1:0`, so
  parallel tests never fight over 9092 and nothing is reachable from the network.
  `Addr()` reports the address actually bound, and Metadata advertises that same
  port, so a client that connects stays connected.
- **Self-contained data.** The metadata database lives at `<DataDir>/meta.db`.
  `t.TempDir()` therefore leaves nothing behind.
- **Topics appear on first use** (`AutoCreateTopics`, one partition), as with the
  binary.

## Lifecycle

| Call | What it does |
|---|---|
| `New(opts)` | Validates, opens storage. Does not listen. |
| `Bind(ctx)` | Opens the listener and returns its address. Optional: `Serve` binds if you did not. |
| `Serve(ctx)` | Accepts until `ctx` is cancelled, drains connections, flushes the logs and closes storage. Returns the first error from that, including a failed final fsync. |
| `Ready()` | A channel closed once the listener is bound (or `Serve` has returned), so waiting on it never hangs. Check `Addr() != nil`. |
| `Addr()` | The bound address, or nil before `Bind`/`Serve`. |
| `Close()` | Releases the storage of a server that was built but never served. Idempotent, and safe after `Serve`. To stop a running server, cancel its context. |
| `ServeListener(ctx, ln)` | `Serve` on a listener you provide, such as a unix socket. Kafka framing does not care what carries it. |
| `Config()` | The effective configuration as a plain `server.Config`. |

## Options

| Option | Meaning |
|---|---|
| `DataDir` | Required. Partition logs. |
| `MetaDB` | Defaults to `<DataDir>/meta.db`. |
| `Version` | Kafka major to emulate, 1–4. 0 = default (4). |
| `ListenAddr` | Default `127.0.0.1:0`. |
| `AdvertisedHost`, `AdvertisedPort` | What clients are told to dial. Set when the broker sits behind a proxy. |
| `AutoCreateTopics`, `DefaultPartitions` | Topic behaviour. |
| `RetentionBytes`, `RetentionMs` | Delete-only retention. 0 = unbounded. |
| `SegmentBytes`, `IndexIntervalBytes` | Storage tuning. Set `SegmentBytes` small to test retention or rolling. |
| `FsyncInterval` | Periodic fsync of active segments. **Zero means the default (500ms), not "off"**: an options struct's zero value is the default. Use a negative value to disable. `acks=all` always fsyncs. |
| `ConsumerSessionTimeout` | How long a silent group member lives. Set low to test failure handling. |
| `SASLMechanisms`, `Credentials`, `TLSConfig` | Authentication and TLS; independent and composable. |
| `Logger` | nil discards. Request lines are Debug. |

The CLI differs from this in one place: `--fsync-interval 0` disables periodic
fsync, where the zero `Options` value keeps the default. Both are deliberate.

## Behind a proxy or on a unix socket

The doze module does this: it passes a unix-socket listener to `ServeListener`
and sets `AdvertisedHost`/`AdvertisedPort` to the address clients reach the proxy
on, so Metadata sends them there rather than to the socket.

## What is not here

The embed API is the broker. The web console is a separate package
(`github.com/doze-dev/doze-kafka/console`) that talks to a broker as any Kafka
client would; embedding it is optional.
