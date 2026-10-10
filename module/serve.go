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

	"github.com/doze-dev/doze-kafka/console"
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

	srv, err := server.New(opts)
	if err != nil {
		return err
	}

	_ = os.Remove(*socket)
	ln, err := net.Listen("unix", *socket)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *socket, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The web console, on its own HTTP port (Kafka's wire protocol can't share
	// one). It dials the broker's unix socket directly — through the doze proxy
	// its idle client would count as a live connection and pin the instance
	// awake forever. Console failure never takes the broker down.
	if *consoleAddr != "" {
		con, err := console.New(console.Options{Addr: "unix://" + *socket, DisplayAddr: *advertise})
		if err != nil {
			fmt.Fprintf(os.Stderr, "kafka console: %v\n", err)
		} else {
			httpSrv := &http.Server{Addr: *consoleAddr, Handler: con}
			go func() {
				if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
