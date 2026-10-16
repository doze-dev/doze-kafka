package main

import (
	"reflect"
	"strings"
	"testing"
)

// `config print` is "the effective configuration, defaults included": a setting
// left at its default is printed with that default, and the printed file, started
// from, is the same configuration. console-prefix and acl-default were left out at
// their defaults, and meta-db always.
func TestConfigPrintIsTheWholeEffectiveConfigurationAndRoundTrips(t *testing.T) {
	cases := [][]string{
		{"--listen", "127.0.0.1:0"},
		{"--listen", "127.0.0.1:9092", "--console-prefix", "/kafka", "--acl-default", "deny", "--acl-super-user", "User:admin"},
		{"--listen", "127.0.0.1:0", "--sasl-mechanisms", "PLAIN", "--sasl-user", "alice:s3cret", "--meta-db", "/var/lib/meta.db"},
		{"--name", "harbour", "--console=false", "--schema-registry=false"},
	}
	for _, args := range cases {
		t.Chdir(t.TempDir())
		code, printed, errOut := run3(append([]string{"config", "print", "--show-secrets", "--config", ""}, args...)...)
		if code != 0 {
			t.Fatalf("config print %v: %d %s", args, code, errOut)
		}
		for _, key := range []string{"console-prefix = ", "acl-default = ", "meta-db"} {
			if !strings.Contains(printed, key) {
				t.Errorf("config print %v leaves out %q:\n%s", args, key, printed)
			}
		}
		path := writeTOML(t, t.TempDir(), printed)
		want, err := loadConfig(append([]string{"--config", ""}, args...))
		if err != nil {
			t.Fatal(err)
		}
		got, err := loadConfig([]string{"--config", path})
		if err != nil {
			t.Fatalf("starting from what config print %v wrote: %v\n%s", args, err, printed)
		}
		if !reflect.DeepEqual(got.cfg, want.cfg) {
			t.Errorf("config print %v does not round-trip:\n got %+v\nwant %+v", args, got.cfg, want.cfg)
		}
		_, again, _ := run3("config", "print", "--show-secrets", "--config", path)
		if again != printed {
			t.Errorf("printing the printed file gives something else:\n%s\nthen\n%s", printed, again)
		}
	}
}
