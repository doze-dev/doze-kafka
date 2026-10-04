//go:build !windows

package main

import "testing"

// The default action of dns-setup changes the machine and asks for sudo, so
// anything it does not understand is an error, not a reason to fall through to
// it. `dns-setup extra --check` used to run the installer.
func TestDNSSetupRefusesWhatItDoesNotUnderstand(t *testing.T) {
	for _, tc := range []struct {
		args  []string
		mode  string
		print bool
		bad   bool
	}{
		{nil, "install", false, false},
		{[]string{"--check"}, "check", false, false},
		{[]string{"--uninstall"}, "uninstall", false, false},
		{[]string{"--print"}, "install", true, false},
		{[]string{"--uninstall", "--print"}, "uninstall", true, false},
		{[]string{"extra", "--check"}, "", false, true},
		{[]string{"check"}, "", false, true},
		{[]string{"--check", "extra"}, "", false, true},
		{[]string{"--chek"}, "", false, true},
		{[]string{"--check", "--uninstall"}, "", false, true},
		{[]string{"--check", "--print"}, "", false, true},
	} {
		mode, print, err := parseDNSSetup(tc.args)
		if (err != nil) != tc.bad || mode != tc.mode || print != tc.print {
			t.Errorf("parseDNSSetup(%v) = %q, print=%v, err=%v; want %q, print=%v, error=%v", tc.args, mode, print, err, tc.mode, tc.print, tc.bad)
		}
	}
}
