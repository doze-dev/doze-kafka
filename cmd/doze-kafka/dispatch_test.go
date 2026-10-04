package main

import (
	"strings"
	"testing"
)

// Nothing except a deliberate start may end up booting a broker: a command typed
// wrongly is an error, and the new commands answer without binding anything.
func TestCommandsAnswerAndATypoIsAnError(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
		out  string
	}{
		{[]string{"help"}, 0, "doctor"},
		{[]string{"help"}, 0, "env"},
		{[]string{"version"}, 0, "doze-kafka"},
		{[]string{"env", "--help"}, 0, "Usage:"},
		{[]string{"doctor", "--help"}, 0, "Usage:"},
		{[]string{"doctr"}, 2, ""},
		{[]string{"serve"}, 2, ""},
	} {
		code, stdout, stderr := run3(tc.args...)
		if code != tc.code || !strings.Contains(stdout+stderr, tc.out) {
			t.Errorf("%v: exit %d (want %d), output %q%q, want it to contain %q", tc.args, code, tc.code, stdout, stderr, tc.out)
		}
	}
}
