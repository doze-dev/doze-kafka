package main

// `doze-kafka env` — the shell block that points a Kafka client at this instance.
//
// The values depend on the instance (its name, or the address it was told to
// listen on, and whether SASL is on), so a README can only show one combination.
// This prints the right one for the configuration in front of you, and `eval`
// makes it one step. It answers through instanceAddress, the same function the
// server's own banner uses, so the two cannot disagree.

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/doze-dev/doze-kafka/internal/config"
)

func runEnv(args []string, stdout, stderr io.Writer) int {
	st, err := loadEnvConfig(args)
	switch {
	case err == errHelp:
		d := config.Default()
		fs, _ := newFlagSet(&d)
		printHelp(stdout, fs)
		return 0
	case err != nil:
		return usageError(stderr, err)
	}
	envTo(stdout, st.cfg)
	return 0
}

// envTo prints what `env` prints for cfg, so a test can read it without a process.
func envTo(w io.Writer, cfg config.Config) {
	bootstrap, console := instanceAddress(cfg)
	vars := map[string]string{
		"KAFKA_BOOTSTRAP_SERVERS": bootstrap,
		"KAFKA_BROKERS":           bootstrap,
	}
	if cfg.AuthEnabled() {
		vars["KAFKA_SASL_MECHANISM"] = cfg.SASLMechanisms[0]
	}
	if u := registryURL(cfg); u != "" {
		vars["SCHEMA_REGISTRY_URL"] = u
	}
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "export %s=%s\n", k, vars[k])
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "# Both names are in common use; set whichever your client reads.")
	if console != "" {
		fmt.Fprintf(w, "# Console: %s\n", console)
	}
	if cfg.AuthEnabled() {
		fmt.Fprintf(w, "# SASL is on (%s): credentials are the --sasl-user pairs, not printed here.\n", strings.Join(cfg.SASLMechanisms, ", "))
	}
}

// envBound is where a running broker ended up: its Kafka listener, and the
// console's and the registry's (empty when not served).
type envBound struct{ listen, console, registry string }

// envHint is the `doze-kafka env` command the start-up summary prints: one that,
// run in this directory, prints this broker's addresses and not some other's.
//
// It is built from where the broker is actually listening, never from a file a
// running broker leaves behind: a file outlives a crash, two brokers in one
// directory would share it, and a broker with its data elsewhere would be found
// by no one. Every setting env's output depends on is on the command line, with
// --config "" so a doze-kafka.toml that says otherwise is not read. When the
// bare `doze-kafka env` prints exactly the same thing here (by name, with nothing
// set), the bare command is printed instead.
func envHint(st startup, b envBound) string {
	cfg := st.cfg
	var args []string
	if cfg.ByName() {
		args = append(args, "--name", cfg.InstanceName())
		if cfg.AdvertisedHost != "" {
			args = append(args, "--advertised-host", cfg.AdvertisedHost)
		}
		if cfg.ConsoleAddr != "" && b.console != "" {
			args = append(args, "--console-addr", b.console)
		}
		if cfg.SchemaRegistryAddr != "" && b.registry != "" {
			args = append(args, "--schema-registry-addr", b.registry)
		}
	} else {
		args = append(args, "--listen", b.listen)
		// The advertised host only when the listener alone would not give it.
		if probe, err := loadEnvConfig([]string{"--listen", b.listen, "--config", ""}); err != nil || probe.cfg.AdvertisedHost != cfg.AdvertisedHost {
			args = append(args, "--advertised-host", cfg.AdvertisedHost)
		}
		if b.console != "" {
			args = append(args, "--console-addr", b.console)
		}
		if b.registry != "" {
			args = append(args, "--schema-registry-addr", b.registry)
		}
	}
	if !cfg.Console {
		args = append(args, "--console=false")
	}
	if !cfg.SchemaRegistry {
		args = append(args, "--schema-registry=false")
	} else if cfg.TLSEnabled() {
		// The registry's scheme follows the certificate; env loads it, as the broker did.
		args = append(args, "--tls-cert", absPath(cfg.TLSCertFile), "--tls-key", absPath(cfg.TLSKeyFile))
		if !cfg.SchemaRegistryTLS {
			args = append(args, "--schema-registry-tls=false")
		}
	}
	if cfg.AuthEnabled() {
		args = append(args, "--sasl-mechanisms", strings.Join(cfg.SASLMechanisms, ","))
	}
	args = append(args, "--config", "")

	if envOutput(nil) == envOutput(args) && envOutput(args) != "" {
		return "doze-kafka env"
	}
	quoted := make([]string, len(args))
	for i, a := range args {
		quoted[i] = shellQuote(a)
	}
	return "doze-kafka env " + strings.Join(quoted, " ")
}

// envOutput is what `doze-kafka env args` prints here, or "" if it would fail.
func envOutput(args []string) string {
	st, err := loadEnvConfig(args)
	if err != nil {
		return ""
	}
	var b bytes.Buffer
	envTo(&b, st.cfg)
	return b.String()
}

// shellQuote quotes s for a POSIX shell, inside the double quotes of
// eval "$(...)": single quotes, and only when needed.
func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	if strings.IndexFunc(s, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("-_./:=,@[]+", r))
	}) < 0 {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
