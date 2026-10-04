//go:build !windows

package main

// `doze-kafka doctor` — what is in place, what is not, and what to run.
//
// The zone's failure modes are quiet ones. A name that does not resolve looks like
// a broker that is down; a name that resolves to an address nothing listens on looks
// the same; another process holding the name looks like your own instance being
// ignored. Each has a different remedy, and none is visible from the error a Kafka
// client reports. So this prints the state rather than guessing at it, and needs no
// privilege to do so: names.Check is deliberately unprivileged.

import (
	"fmt"
	"io"
	"sort"

	"github.com/doze-dev/doze-kafka/internal/config"
	names "github.com/doze-dev/doze-names"
)

func runDoctor(args []string, stdout, stderr io.Writer) int {
	// The ordinary config flags: doctor reports on the instance this invocation
	// WOULD start, so it reads the same configuration that invocation would.
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
	return doctorTo(stdout, st.cfg, names.Check(), names.Open(names.Home(), "doze-kafka"))
}

// registry is the slice of *names.Registry doctor reads, named so a test can supply
// one without a filesystem.
type registry interface {
	Snapshot() map[string]names.Entry
}

func doctorTo(w io.Writer, cfg config.Config, status names.Status, reg registry) int {
	fmt.Fprintf(w, "doze-kafka %s\n\n", version)

	// --listen and the name are exclusive, so report whichever this configuration
	// actually uses; "answers on kafka.<name>.doze" under --listen would describe a
	// name nothing claims.
	fmt.Fprintln(w, "this instance")
	fmt.Fprintf(w, "  name        %s\n", cfg.InstanceName())
	if cfg.ByName() {
		fmt.Fprintf(w, "  answers on  %s:%d\n", qualifiedHost(cfg.InstanceName()), kafkaPort)
	} else {
		fmt.Fprintf(w, "  answers on  %s (--listen; no .doze name is claimed)\n", cfg.ListenAddr)
	}
	fmt.Fprintf(w, "  kafka       %d\n", cfg.Version)
	fmt.Fprintf(w, "  data        %s\n", cfg.DataDir)

	fmt.Fprintf(w, "\n.doze on this machine (%s)\n", status.Platform)
	for _, step := range status.Steps {
		mark := "✗"
		if step.Done {
			mark = "✓"
		}
		fmt.Fprintf(w, "  %s %-22s %s\n", mark, step.Name, step.Detail)
	}
	if len(status.Steps) == 0 {
		fmt.Fprintln(w, "  (nothing reported — this platform has no setup to do)")
	}

	// Who holds which name matters: "my name stopped working" is almost always
	// another process holding it, and the registry knows which.
	if reg != nil {
		snap := reg.Snapshot()
		fmt.Fprintf(w, "\nnames registered (%d)\n", len(snap))
		if len(snap) == 0 {
			fmt.Fprintln(w, "  (none — no doze binary is running)")
		}
		hosts := make([]string, 0, len(snap))
		for h := range snap {
			hosts = append(hosts, h)
		}
		sort.Strings(hosts)
		for _, host := range hosts {
			e := snap[host]
			fmt.Fprintf(w, "  %-28s %-14s pid %d", host, e.Owner, e.PID)
			if e.Target != "" {
				fmt.Fprintf(w, "  -> %s", e.Target)
			}
			fmt.Fprintln(w)
		}
	}

	fmt.Fprintln(w)
	if status.OK() {
		fmt.Fprintln(w, "✓ .doze resolves. Names will work.")
		return 0
	}
	fmt.Fprintln(w, "✗ .doze does not resolve yet.")
	fmt.Fprintln(w, "    doze-kafka dns-setup          set it up (one sudo, once per machine)")
	fmt.Fprintln(w, "    doze-kafka dns-setup --print  print the script instead, to run yourself")
	fmt.Fprintln(w, "    doze-kafka --listen host:port serve on an address instead of a name")
	// Not an error exit: being told what is missing is the command working.
	return 0
}
