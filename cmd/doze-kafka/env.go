package main

// `doze-kafka env` — the shell block that points a Kafka client at this instance.
//
// The values depend on the instance (its name, or the address it was told to
// listen on, and whether SASL is on), so a README can only show one combination.
// This prints the right one for the configuration in front of you, and `eval`
// makes it one step. It answers through instanceAddress, the same function the
// server's own banner uses, so the two cannot disagree.

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/doze-dev/doze-kafka/internal/config"
)

func runEnv(args []string, stdout, stderr io.Writer) int {
	st, err := loadConfig(args)
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
