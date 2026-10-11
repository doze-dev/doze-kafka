package config

import "testing"

func TestConsolePrefixValidation(t *testing.T) {
	for p, ok := range map[string]bool{
		"": true, "/": true, "/_console": true, "/kafka/ui": true, "/a-b_c.d": true,
		"console": false, "/a b": false, "/a?x=1": false, "/a#f": false, "/a/../b": false,
		"//evil": false, "/healthz": false, "/a%2Fb": false,
	} {
		c := Default()
		c.ConsolePrefix = p
		err := c.Validate()
		if ok && err != nil {
			t.Errorf("prefix %q refused: %v", p, err)
		}
		if !ok && err == nil {
			t.Errorf("prefix %q accepted", p)
		}
	}
}
