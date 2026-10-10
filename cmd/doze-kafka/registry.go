package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/server"
	names "github.com/doze-dev/doze-names"
)

// registryPort is Confluent's own.
const registryPort = 8081

// registryAddrFor is where the registry binds in name mode: the name's own address,
// beside the broker.
func registryAddrFor(ip string) string { return net.JoinHostPort(ip, strconv.Itoa(registryPort)) }

// startRegistry binds the registry's HTTP listener and serves the built-in schema
// registry on it. As with the console, the bind is done before this returns so a
// port that is taken is an error, not a "listening" that was a lie. It returns a
// func that stops it and the address it bound.
func startRegistry(ctx context.Context, cfg config.Config, srv *server.Server, logger *slog.Logger) (func(), string, error) {
	handler, err := srv.SchemaRegistry()
	if err != nil {
		return nil, "", err
	}
	ln, err := zoneListen(cfg.SchemaRegistryAddr)
	if err != nil {
		return nil, "", fmt.Errorf("schema registry: listen on %s: %w (choose another with --schema-registry-addr, or turn it off with --schema-registry=false)", cfg.SchemaRegistryAddr, err)
	}
	httpSrv := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Warn("schema registry stopped", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		httpSrv.Close()
	}()
	return func() { httpSrv.Close() }, ln.Addr().String(), nil
}

// routeRegistry publishes the registry under its own name, schema.<instance>.doze, so
// http://schema.<instance>.doze reaches it through the shared front door the same way
// the console is reached. It returns the URL to print: port-less when a front door is
// up, port-ful otherwise, "" when this instance has no name.
func (z *zone) routeRegistry(logger *slog.Logger, instance, registryAddr string) string {
	if z == nil || z.own == nil {
		return ""
	}
	host, port, err := net.SplitHostPort(registryAddr)
	if err != nil {
		return ""
	}
	if host == "" || net.ParseIP(host).IsUnspecified() {
		host = "127.0.0.1"
	}
	lease, err := z.reg.Claim(names.Qualified("schema", instance))
	if err != nil {
		logger.Debug("zone: could not claim the schema registry name", "err", err)
		return ""
	}
	z.extra = append(z.extra, lease)
	if err := lease.Route(net.JoinHostPort(host, port)); err != nil {
		logger.Debug("zone: could not publish the schema registry route", "err", err)
		return ""
	}
	name := lease.Name.Host
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		u := z.reg.URLFor(name)
		if u == "http://"+name || time.Now().After(deadline) {
			return u
		}
		time.Sleep(20 * time.Millisecond)
	}
}
