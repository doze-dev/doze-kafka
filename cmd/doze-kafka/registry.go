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
//
// When the broker has a certificate the registry is served over TLS with it
// (config.RegistryTLS): a client that sets security.protocol=SSL for the broker
// sets schema.registry.url=https://... beside it, and with SASL the registry's
// requests carry the same passwords.
func startRegistry(ctx context.Context, cfg config.Config, srv *server.Server, logger *slog.Logger) (func(), string, error) {
	handler, err := srv.SchemaRegistry()
	if err != nil {
		return nil, "", err
	}
	ln, err := zoneListen(cfg.SchemaRegistryAddr)
	if err != nil {
		return nil, "", fmt.Errorf("schema registry: listen on %s: %w (choose another with --schema-registry-addr, or turn it off with --schema-registry=false)", cfg.SchemaRegistryAddr, err)
	}
	httpSrv := &http.Server{
		Handler: handler, ReadHeaderTimeout: 10 * time.Second,
		// A failed handshake (a plain-HTTP client on the TLS port, a client that does
		// not trust the certificate) is the client's business: at debug, in the
		// process's own log format, rather than on the standard logger.
		ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelDebug),
	}
	serve := func() error { return httpSrv.Serve(ln) }
	if cfg.RegistryTLS() {
		tlsCfg, err := cfg.TLSConfig()
		if err != nil {
			ln.Close()
			return nil, "", err
		}
		httpSrv.TLSConfig = tlsCfg
		serve = func() error { return httpSrv.ServeTLS(ln, "", "") }
	}
	go func() {
		if err := serve(); err != nil && err != http.ErrServerClosed {
			logger.Warn("schema registry stopped", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		httpSrv.Close()
	}()
	return func() { httpSrv.Close() }, ln.Addr().String(), nil
}

// registryScheme is how a client reaches the registry: https when it is served
// with the broker's certificate.
func registryScheme(cfg config.Config) string {
	if cfg.RegistryTLS() {
		return "https"
	}
	return "http"
}

// registryTLSURL is where clients reach a registry served over TLS at bound.
//
// It is not behind the zone's front door, which speaks plain HTTP to what it
// fronts and would be refused. Beside a named broker it is on the broker's own
// host (the name, or the address where the name does not resolve here) on its own
// port: the host the broker's certificate has to name anyway. Anywhere else it
// is the address it was told to bind.
func registryTLSURL(besideTheName bool, brokerHost, bound string) string {
	if _, port, err := net.SplitHostPort(bound); err == nil && besideTheName && brokerHost != "" {
		return "https://" + net.JoinHostPort(brokerHost, port)
	}
	return urlFor("https", bound)
}

// routeRegistry publishes the registry under its own name, schema.<instance>.doze, so
// http://schema.<instance>.doze reaches it through the shared front door the same way
// the console is reached. It returns the URL to print: port-less when a front door is
// up, port-ful otherwise, the address it bound when the name does not answer, and ""
// when this instance has no name.
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
			// The registry binds beside the broker, on the instance's address, and
			// this name has an address of its own: without a front door the port-ful
			// URL names an address nothing listens on, which is what asking catches.
			return z.byNameOrAddress(logger, "schema registry", u, net.JoinHostPort(host, port))
		}
		time.Sleep(20 * time.Millisecond)
	}
}
