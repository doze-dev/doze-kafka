package kafka

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/doze-dev/doze-kafka/console"
	"github.com/doze-dev/doze-kafka/internal/consolefeed"
	"github.com/doze-dev/doze-kafka/internal/embed"
	"github.com/doze-dev/doze-kafka/internal/serverhook"
	"github.com/doze-dev/doze-kafka/server"
)

// ServeFromArgs runs the embedded broker for a `kafka-plugin __serve …`
// invocation: it opens the doze-kafka server on a unix socket (which the doze
// proxy splices client connections onto) and serves until SIGTERM.
func ServeFromArgs(argv []string) error {
	fs := flag.NewFlagSet("__serve", flag.ContinueOnError)
	socket := fs.String("socket", "", "unix socket to listen on")
	datadir := fs.String("datadir", "", "data directory")
	version := fs.String("version", "4", "Kafka protocol profile (1-4)")
	advertise := fs.String("advertise", "", "advertised host:port for Metadata")
	consoleAddr := fs.String("console-addr", "", "host:port for the web console ('' = off)")
	consoleSocket := fs.String("console-socket", "", "unix socket for the web console (doze forwards its port here)")
	registrySocket := fs.String("registry-socket", "", "unix socket for the schema registry ('' = off)")
	registryURL := fs.String("registry-url", "", "where clients, and the console, reach the schema registry")
	autoCreate := fs.String("auto-create", "", "auto-create topics (true/false)")
	defaultParts := fs.Int("default-partitions", 0, "partitions for auto-created topics")
	retentionMs := fs.Int64("retention-ms", 0, "retention in ms")
	retentionBytes := fs.Int64("retention-bytes", 0, "retention in bytes")
	if err := fs.Parse(argv[1:]); err != nil { // argv[0] == "__serve"
		return err
	}
	if *socket == "" || *datadir == "" {
		return fmt.Errorf("kafka __serve: --socket and --datadir are required")
	}

	ver, err := strconv.Atoi(*version)
	if err != nil {
		ver = 4
	}
	opts := server.Options{
		Version:           ver,
		DataDir:           *datadir,
		DefaultPartitions: *defaultParts,
		RetentionMs:       *retentionMs,
		RetentionBytes:    *retentionBytes,
	}
	if *advertise != "" {
		host, port, err := net.SplitHostPort(*advertise)
		if err == nil {
			opts.AdvertisedHost = host
			if p, perr := strconv.Atoi(port); perr == nil {
				opts.AdvertisedPort = p
			}
		}
	}
	if *autoCreate != "" {
		b := *autoCreate == "true"
		opts.AutoCreateTopics = &b
	}

	// A broker that will have a console keeps what the console's Traffic page reads
	// (the record of requests, and what clients report about themselves under
	// KIP-714, which it then offers them the APIs for), as the doze-kafka command's
	// broker does. One with no console keeps neither and offers neither: nobody
	// would read them. (The module used to build every broker the second way and
	// then start a console over it, which therefore had no Traffic.)
	newServer := server.New
	withConsole := *consoleAddr != "" || *consoleSocket != ""
	if withConsole {
		newServer = embed.NewWithConsole
	}
	srv, err := newServer(opts)
	if err != nil {
		return err
	}

	_ = os.Remove(*socket)
	ln, err := net.Listen("unix", *socket)
	if err != nil {
		srv.Close() // storage and the lock on the data directory.
		return fmt.Errorf("listen on %s: %w", *socket, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The web console, on its own HTTP port (Kafka's wire protocol can't share
	// one). It dials the broker's unix socket directly — through the doze proxy
	// its idle client would count as a live connection and pin the instance
	// awake forever. Console failure never takes the broker down.
	// The schema registry, on the socket doze forwards registry.<name> to. It is
	// started before the console, which reads it at registryURL.
	if *registrySocket != "" {
		if err := serveHTTPOn(ctx, *registrySocket, func() (http.Handler, error) { return srv.SchemaRegistry() }); err != nil {
			fmt.Fprintf(os.Stderr, "kafka schema registry: %v\n", err)
			*registryURL = ""
		}
	}
	if withConsole {
		opts := console.Options{
			Addr: "unix://" + *socket, DisplayAddr: *advertise,
			Traffic:       consolefeed.Traffic(serverhook.Traffic(srv)),
			ClientMetrics: consolefeed.ClientMetrics(serverhook.ClientMetrics(srv)),
		}
		if *registryURL != "" {
			opts.RegistryURL = *registryURL
			opts.Facts.RegistryURL, opts.Facts.RegistryBuiltIn = *registryURL, true
		}
		con, err := console.New(opts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "kafka console: %v\n", err)
		} else {
			httpSrv := &http.Server{Addr: *consoleAddr, Handler: con}
			go func() {
				var err error
				if *consoleSocket != "" {
					var ln net.Listener
					if ln, err = listenUnix(*consoleSocket); err == nil {
						err = httpSrv.Serve(ln)
					}
				} else {
					err = httpSrv.ListenAndServe()
				}
				if err != nil && err != http.ErrServerClosed {
					fmt.Fprintf(os.Stderr, "kafka console: %v\n", err)
				}
			}()
			go func() {
				<-ctx.Done()
				httpSrv.Close()
				con.Close()
			}()
		}
	}

	// ServeListener opens storage before accepting, so an accepting socket
	// means ready — which is exactly the Ready{socket} gate the driver sets.
	return srv.ServeListener(ctx, ln)
}

// listenUnix listens on a unix socket, replacing a stale one from a crash.
func listenUnix(path string) (net.Listener, error) {
	_ = os.Remove(path)
	return net.Listen("unix", path)
}

// serveHTTPOn serves the handler build makes on a unix socket until ctx ends.
// The socket is bound before it returns, so a failure is an error here.
func serveHTTPOn(ctx context.Context, path string, build func() (http.Handler, error)) error {
	h, err := build()
	if err != nil {
		return err
	}
	ln, err := listenUnix(path)
	if err != nil {
		return err
	}
	httpSrv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = httpSrv.Serve(ln) }()
	go func() {
		<-ctx.Done()
		_ = httpSrv.Close()
	}()
	return nil
}
