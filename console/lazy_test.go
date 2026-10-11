package console

import "testing"

// Idle heap is a budget (testdata/lightness.json): the templates are parsed on
// the first request, not when the console is built.
func TestTemplatesAreParsedOnFirstUse(t *testing.T) {
	c, err := New(Options{Addr: "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.tmpl != nil {
		t.Fatal("templates were parsed by New; they must wait for the first request")
	}
	if len(c.templates().Templates()) == 0 {
		t.Fatal("templates did not parse on first use")
	}
	if c.tmpl == nil {
		t.Fatal("first use did not keep the parsed set")
	}
}
