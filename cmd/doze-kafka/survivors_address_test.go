package main

import (
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

func TestInstanceAddressUnderListen(t *testing.T) {
	cfg := config.Default()
	cfg.ListenAddr = "127.0.0.1:9092"
	cfg.ConsoleAddr = "127.0.0.1:8080"
	for _, c := range []struct {
		name          string
		edit          func(*config.Config)
		bootstrap, ui string
	}{
		{"console on", func(*config.Config) {}, "127.0.0.1:9092", "http://127.0.0.1:8080"},
		{"console off", func(c *config.Config) { c.Console = false }, "127.0.0.1:9092", ""},
		{"no console address", func(c *config.Config) { c.ConsoleAddr = "" }, "127.0.0.1:9092", ""},
		{"an advertised host", func(c *config.Config) { c.AdvertisedHost = "box.local" }, "box.local:9092", "http://127.0.0.1:8080"},
		{"every interface", func(c *config.Config) { c.ListenAddr = ":9092" }, "localhost:9092", "http://127.0.0.1:8080"},
		{"not an address", func(c *config.Config) { c.ListenAddr = "nope" }, "nope", ""},
	} {
		cc := cfg
		cc.Console = true
		c.edit(&cc)
		if b, ui := instanceAddress(cc); b != c.bootstrap || ui != c.ui {
			t.Errorf("%s: %q, %q; want %q, %q", c.name, b, ui, c.bootstrap, c.ui)
		}
	}
}

// Where the registry is said to be, with the registry on or off, under an address or a
// name, in plain text or TLS. (A name nothing is running under is where it will be.)
func TestRegistryURLByConfiguration(t *testing.T) {
	cfg := config.Default()
	cfg.Name = "shop"
	for _, c := range []struct {
		name string
		edit func(*config.Config)
		want string
	}{
		{"off", func(c *config.Config) { c.SchemaRegistry = false; c.ListenAddr = "127.0.0.1:9092" }, ""},
		{"under an address, none given", func(c *config.Config) { c.ListenAddr = "127.0.0.1:9092"; c.SchemaRegistryAddr = "" }, ""},
		{"under an address", func(c *config.Config) { c.ListenAddr = "127.0.0.1:9092"; c.SchemaRegistryAddr = "127.0.0.1:8081" }, "http://127.0.0.1:8081"},
		{"under an address, in TLS", func(c *config.Config) {
			c.ListenAddr, c.SchemaRegistryAddr = "127.0.0.1:9092", "127.0.0.1:8081"
			c.TLSCertFile, c.TLSKeyFile = "c.pem", "k.pem"
		}, "https://127.0.0.1:8081"},
		{"by name, not running", func(*config.Config) {}, "http://schema.shop.doze"},
		{"by name, in TLS, not running", func(c *config.Config) { c.TLSCertFile, c.TLSKeyFile = "c.pem", "k.pem" }, "https://kafka.shop.doze:8081"},
		{"by name, in TLS, at an address", func(c *config.Config) {
			c.TLSCertFile, c.TLSKeyFile, c.SchemaRegistryAddr = "c.pem", "k.pem", "10.0.0.5:9443"
		}, "https://10.0.0.5:9443"},
	} {
		cc := cfg
		c.edit(&cc)
		if got := registryURL(cc); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}
