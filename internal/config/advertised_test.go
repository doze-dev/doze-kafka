package config

import "testing"

// A client is told to reconnect to an address that reaches the listener. The
// default advertised host was "localhost" whatever the listener was; on a machine
// where that resolves to ::1 first, a client of the 127.0.0.1 listener was sent to a
// port nothing listened on, and kcat gave up at the first refusal.
func TestResolveAdvertisedHostFollowsTheListener(t *testing.T) {
	for _, tc := range []struct{ listen, set, want string }{
		{"127.0.0.1:9092", "", "127.0.0.1"},
		{"127.0.0.1:0", "", "127.0.0.1"},
		{"[::1]:9092", "", "::1"},
		{"localhost:9092", "", "127.0.0.1"},
		{"192.168.1.5:9092", "", "192.168.1.5"},
		{":9092", "", "localhost"},
		{"0.0.0.0:9092", "", "localhost"},
		{"[::]:9092", "", "localhost"},
		// What was set is never changed.
		{"127.0.0.1:9092", "localhost", "localhost"},
		{":9092", "host.docker.internal", "host.docker.internal"},
		{"192.168.1.5:9092", "kafka.lan", "kafka.lan"},
	} {
		c := Default()
		c.ListenAddr, c.AdvertisedHost = tc.listen, tc.set
		c.ResolveAdvertisedHost()
		if c.AdvertisedHost != tc.want {
			t.Errorf("listen %s, advertised-host %q: resolved to %q, want %q", tc.listen, tc.set, c.AdvertisedHost, tc.want)
		}
		if err := c.Validate(); err != nil {
			t.Errorf("listen %s: the resolved configuration does not validate: %v", tc.listen, err)
		}
	}
	// Resolving twice is the same as once.
	c := Default()
	c.ResolveAdvertisedHost()
	first := c.AdvertisedHost
	c.ListenAddr = ":9092"
	c.ResolveAdvertisedHost()
	if c.AdvertisedHost != first {
		t.Errorf("a resolved host changed when resolved again: %q -> %q", first, c.AdvertisedHost)
	}
	// "Derive it" is a valid thing to ask for; a malformed host is not.
	bad := Default()
	bad.AdvertisedHost = "host:9092"
	if err := bad.Validate(); err == nil {
		t.Error("an advertised host with a port in it validated")
	}
}
