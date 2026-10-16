package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
	names "github.com/doze-dev/doze-names"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Two instances on one machine is the whole reason names are per-instance: each gets
// its own loopback address, so both bind 9092 and neither notices the other.
func TestTwoInstancesGetTheirOwnNamesAndAddresses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harbour := joinZone(ctx, quietLogger(), "harbour")
	defer harbour.close()
	atlas := joinZone(ctx, quietLogger(), "atlas")
	defer atlas.close()

	if harbour.own == nil || atlas.own == nil {
		t.Fatalf("both instances must get their own name: harbour=%v atlas=%v", harbour.own, atlas.own)
	}
	if got, want := harbour.host(), "kafka.harbour.doze"; got != want {
		t.Errorf("harbour answers on %q, want %q", got, want)
	}
	if got, want := atlas.host(), "kafka.atlas.doze"; got != want {
		t.Errorf("atlas answers on %q, want %q", got, want)
	}
	if harbour.ip() == atlas.ip() {
		t.Errorf("both instances landed on %s: they cannot both bind a port there", harbour.ip())
	}
}

// kafka.doze is never claimed. It was a machine-wide shorthand for whichever instance
// started first, which made an address under it mean a different instance depending on
// boot order. One way to reach an instance, not two.
func TestTheMachineWideApexIsNeverClaimed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	z := joinZone(ctx, quietLogger(), "harbour")
	defer z.close()

	for host := range names.Open(names.Home(), "doze-kafka").Snapshot() {
		if host == "kafka.doze" {
			t.Errorf("%s was claimed; an instance answers only to kafka.<name>.doze", host)
		}
	}
}

// The experience matches doze-aws's: the same instance name gives the same suffix.
func TestTheNameIsTheServiceThenTheInstance(t *testing.T) {
	if got, want := qualifiedHost("harbour"), "kafka.harbour.doze"; got != want {
		t.Errorf("qualifiedHost = %q, want %q", got, want)
	}
}

// Losing your name to another live instance has a different answer from having no
// names at all (run under another name, or take an address), so the two must not share
// one message.
func TestAHeldNameSaysWhoHasItAndWhatToDo(t *testing.T) {
	z := &zone{holder: &heldBy{host: "kafka.harbour.doze", pid: 4242, owner: "doze-kafka"}}
	_, err := openListener(config.Default(), z, quietLogger(), nil)
	if err == nil {
		t.Fatal("a held name with no --listen must not start")
	}
	for _, want := range []string{"kafka.harbour.doze", "4242", "--name", "--listen"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the held-name error is missing %q:\n%s", want, err)
		}
	}

	// With no name at all it is a DNS problem, and --name would not help.
	_, err = openListener(config.Default(), &zone{}, quietLogger(), nil)
	if err == nil {
		t.Fatal("no name and no --listen must not start")
	}
	if strings.Contains(err.Error(), "--name") {
		t.Errorf("suggesting --name when no name resolves at all sends the reader the wrong way:\n%s", err)
	}
	if !strings.Contains(err.Error(), "doctor") || !errors.Is(err, errNothingToListenOn) {
		t.Errorf("the no-names error should point at doctor and stay matchable:\n%s", err)
	}
}

// --listen and the name are exclusive: with an address given, no zone is joined and
// openListener serves the address without touching it. Nothing is written to the
// registry, because a stray entry could make a sibling directory's instance look taken.
func TestListenServesTheAddressAndClaimsNoName(t *testing.T) {
	reg := names.Open(names.Home(), "doze-kafka")
	before := len(reg.Snapshot())

	cfg := config.Default()
	cfg.ListenAddr = "127.0.0.1:0"
	b, err := openListener(cfg, nil, quietLogger(), nil)
	if err != nil {
		t.Fatalf("--listen must serve with no zone at all: %v", err)
	}
	defer b.ln.Close()

	if b.what != "address" {
		t.Errorf("binding is %q, want %q", b.what, "address")
	}
	if !strings.HasPrefix(b.bootstrap, "127.0.0.1:") || strings.Contains(b.bootstrap, ".doze") {
		t.Errorf("bootstrap = %q, want the bound address", b.bootstrap)
	}
	if after := len(reg.Snapshot()); after != before {
		t.Errorf("registry grew from %d to %d entries under --listen: %v", before, after, reg.Snapshot())
	}
}

// What the banner and Metadata may advertise: a name that does not resolve here is
// worse than an address.
func TestAnUnresolvableNameIsAdvertisedAsItsAddress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	z := joinZone(ctx, quietLogger(), "unresolvable")
	defer z.close()
	if z.own == nil {
		skipOrFail(t, "no name claimed")
	}
	// The lookups below are stand-ins, so the question after them (does the name
	// answer) is stood in for too: this registry is the test's own, and the machine's
	// resolver has never heard of the name.
	z.reach = func(string, int) bool { return true }
	nothing := func(context.Context, string) ([]string, error) { return nil, errors.New("no such host") }
	b, err := openListener(config.Default(), z, quietLogger(), nothing)
	if err != nil {
		// A Mac whose network daemon is not running, or (under this package's
		// throwaway DOZE_HOME) one whose daemon serves another home.
		skipOrFail(t, "cannot listen on %s for the name: %v", z.ip(), err)
	}
	defer b.ln.Close()
	if b.host != z.ip() || strings.Contains(b.bootstrap, ".doze") {
		t.Errorf("advertised host %q, bootstrap %q: a name that does not resolve must not be what clients are sent to", b.host, b.bootstrap)
	}

	// And the opposite: where it resolves to this address, the name is kept.
	b.ln.Close()
	here := func(context.Context, string) ([]string, error) { return []string{z.ip()}, nil }
	b, err = openListener(config.Default(), z, quietLogger(), here)
	if err != nil {
		t.Fatal(err)
	}
	defer b.ln.Close()
	if b.host != z.host() {
		t.Errorf("advertised host %q for a name that resolves here, want %q", b.host, z.host())
	}

	// It resolves here, and nothing answers at it: still the address.
	b.ln.Close()
	z.reach = func(string, int) bool { return false }
	b, err = openListener(config.Default(), z, quietLogger(), here)
	if err != nil {
		t.Fatal(err)
	}
	defer b.ln.Close()
	if b.host != z.ip() || strings.Contains(b.bootstrap, ".doze") {
		t.Errorf("advertised host %q, bootstrap %q for a name nothing answers at", b.host, b.bootstrap)
	}
	z.reach = func(string, int) bool { return true }

	// A name that resolves, but to someone else's address (a container inheriting its
	// host's DNS) is not this instance's name.
	b.ln.Close()
	elsewhere := func(context.Context, string) ([]string, error) { return []string{"127.0.0.42"}, nil }
	b, err = openListener(config.Default(), z, quietLogger(), elsewhere)
	if err != nil {
		t.Fatal(err)
	}
	defer b.ln.Close()
	if b.host == z.host() {
		t.Errorf("advertised %q, which resolves to an address this instance is not on", b.host)
	}
}

// resolvesHere needs both an answer and the right one; dropping either half produces a
// plausible check that is wrong only inside a container.
func TestResolvesHereNeedsBothAnAnswerAndTheRightOne(t *testing.T) {
	const host, mine = "kafka.harbour.doze", "127.0.0.17"
	for _, c := range []struct {
		name   string
		lookup lookupFunc
		want   bool
	}{
		{"names this address", func(context.Context, string) ([]string, error) { return []string{mine}, nil }, true},
		{"names it among others", func(context.Context, string) ([]string, error) { return []string{"127.0.0.42", mine}, nil }, true},
		{"resolves somewhere else", func(context.Context, string) ([]string, error) { return []string{"127.0.0.42"}, nil }, false},
		{"does not resolve", func(context.Context, string) ([]string, error) { return nil, errors.New("no such host") }, false},
		{"answers with nothing", func(context.Context, string) ([]string, error) { return nil, nil }, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := resolvesHere(c.lookup, host, mine); got != c.want {
				t.Errorf("resolvesHere = %v, want %v", got, c.want)
			}
		})
	}
}

// A taken port on the name's address is an error that names the cause: the remedy is
// the opposite of "run dns-setup".
func TestATakenAddressSaysSoRatherThanBlamingDNS(t *testing.T) {
	if runtime.GOOS == "darwin" {
		// Not a missing setup, so not a failure under DOZE_NAMES_REQUIRED either: on
		// macOS a name's address is never bound (doze-names translates it to a
		// private port), so no other process can be holding it, and this clash
		// cannot be arranged.
		t.Skip("on macOS a name's address is translated, never bound: nothing else can hold its port")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	z := joinZone(ctx, quietLogger(), "portclash")
	defer z.close()
	if z.own == nil {
		skipOrFail(t, "no name claimed")
	}
	blocker, err := net.Listen("tcp", net.JoinHostPort(z.ip(), "9092"))
	if err != nil {
		skipOrFail(t, "cannot bind %s:9092 (in use?): %v", z.ip(), err)
	}
	defer blocker.Close()

	_, err = openListener(config.Default(), z, quietLogger(), nil)
	if err == nil {
		t.Fatal("binding an address something else holds must fail")
	}
	if strings.Contains(err.Error(), "dns-setup") || !strings.Contains(err.Error(), "--name") {
		t.Errorf("the error blames DNS for a port clash:\n%s", err)
	}
}

// In name mode the broker's own Metadata must send clients back to where it is: the
// address the name resolves to (this test's registry is private, so the name does not
// resolve through the machine's resolver, and the address is what is told), on the port
// it bound. A blank or "localhost" host here would send every client to a different
// machine than the one that answered the bootstrap.
func TestNameModeAdvertisesWhereTheBrokerIs(t *testing.T) {
	// run() below sets the machine up when it is not (as root without asking), which
	// is not a test's business: look first, and start nothing on a machine that is
	// not ready.
	needZone(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	z := joinZone(ctx, quietLogger(), "metadata")
	if z.own == nil {
		z.close()
		skipOrFail(t, "no name claimed")
	}
	ip, host := z.ip(), z.own.Name.Host
	// Through the lease, as the server listens: on macOS the address is not one a
	// process can bind.
	l, err := z.own.Listen(kafkaPort)
	if err == nil {
		l.Close()
	}
	z.close()
	if err != nil {
		skipOrFail(t, "cannot listen on %s:9092 for %s (in use?): %v", ip, host, err)
	}

	dir := t.TempDir()
	t.Chdir(dir)
	st, err := loadConfig([]string{"--name", "metadata", "--data-dir", filepath.Join(dir, "data"), "--console=false", "--config", ""})
	if err != nil {
		t.Fatal(err)
	}
	// ensureNames asks the MACHINE; this test is about what the broker advertises.
	out := &safeBuf{}
	runCtx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(runCtx, st, newLogger(st.cfg, out), out) }()
	defer func() { stop(); <-done }()

	waitFor(t, "the summary", func() bool {
		select {
		case err := <-done:
			skipOrFail(t, "did not start in name mode here: %v", err)
		default:
		}
		return strings.Contains(out.String(), "doze-kafka is up")
	})

	cl, err := kgo.NewClient(kgo.SeedBrokers(net.JoinHostPort(ip, "9092")))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	resp, err := kmsg.NewPtrMetadataRequest().RequestWith(runCtx, cl)
	if err != nil {
		t.Fatal(err)
	}
	// The broker tells clients the name where the name resolves here (a machine set
	// up for .doze, as a CI runner is once something has run setup) and the address
	// where it does not. Under DOZE_NAMES_REQUIRED the machine is one where it must.
	want := ip
	if resolvesHere(net.DefaultResolver.LookupHost, host, ip) && names.Reachable(host, kafkaPort) {
		want = host
	} else if namesRequired() {
		t.Errorf("%s does not resolve to %s and answer on this machine, and %s says it must", host, ip, envNamesRequired)
	}
	if len(resp.Brokers) != 1 || resp.Brokers[0].Host != want || resp.Brokers[0].Port != 9092 {
		t.Errorf("Metadata brokers = %+v, want one at %s:9092", resp.Brokers, want)
	}
}

// DOZE_ZONE=off is every doze binary's switch: with no --listen to fall back on,
// the instance cannot start, and says what to do instead rather than touching the zone.
func TestZoneOffWithoutListenHasNothingToListenOn(t *testing.T) {
	t.Setenv(names.EnvZone, "off")
	_, err := prepareZone(context.Background(), config.Default(), quietLogger())
	if !errors.Is(err, errNothingToListenOn) {
		t.Errorf("err = %v, want errNothingToListenOn", err)
	}
	cfg := config.Default()
	cfg.ListenAddr = "127.0.0.1:0"
	if z, err := prepareZone(context.Background(), cfg, quietLogger()); z != nil || err != nil {
		t.Errorf("--listen touched the zone: %v %v", z, err)
	}
}
