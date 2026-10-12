package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A config file can hold passwords, and a file that cannot be read is very often one
// with a password in the wrong place: unquoted, on a line of its own, where the user
// name goes. So an error for the file names the line and the key and repeats nothing
// that was written: not the value, not the start of it, not its first letter, and under
// [credentials] not the key either.
func TestConfigFileErrorsNameTheLineAndTheKeyAndNeverWhatWasWritten(t *testing.T) {
	const p = "PLANTEDsecret-Zq7-93f1"
	const notRepeated = "; what was written is not repeated, because this file can hold passwords"
	const notTOML = "this is not valid TOML (a string is written in quotes)"
	const creds = `under [credentials], where an entry is user = "password"`
	for _, tc := range []struct {
		name, content, want string
	}{
		// Outside [credentials]: the value is never repeated, the key is named.
		{"an unquoted value under [tls]", "[tls]\nkey=" + p,
			`read FILE: toml: line 2 (key "tls.key"): ` + notTOML + notRepeated},
		{"a string that is not closed", "[tls]\ncert = \"a\"\nkey = \"unterminated " + p,
			`read FILE: toml: line 3 (key "tls.key"): ` + notTOML + notRepeated},
		{"a number run into a word", "listen = 12" + p,
			`read FILE: toml: line 1: ` + notTOML + notRepeated},
		{"a line that is only a word", "name = \"x\"\n" + p,
			`read FILE: toml: line 2: ` + notTOML + notRepeated},
		{"a table name that is not closed", "[" + p,
			`read FILE: toml: line 2: ` + notTOML + notRepeated},
		{"a string where a number goes", "segment-bytes = \"" + p + "\"",
			`read FILE: toml: line 1 (key "segment-bytes"): the value is a string where a number is expected` + notRepeated},
		{"a string where a list goes", "sasl-mechanisms = \"" + p + "\"",
			`read FILE: toml: line 1 (key "sasl-mechanisms"): the value is a string where a list is expected` + notRepeated},
		{"a string where true or false goes", "console = \"" + p + "\"",
			`read FILE: toml: line 1 (key "console"): the value is a string where true or false is expected` + notRepeated},
		{"a list where a string goes", "[tls]\nkey = [\"" + p + "\"]",
			`read FILE: toml: line 2 (key "tls.key"): the value is a list where a string is expected` + notRepeated},
		{"a number where a string goes", "[credentials]\nalice = \"" + p + "\"\n[tls]\ncert = 5",
			`read FILE: toml: line 4 (key "tls.cert"): the value is a number where a string is expected` + notRepeated},
		{"a string where a table goes", "tls = \"" + p + "\"",
			`read FILE: toml: line 1 (key "tls"): the value is a string where a table is expected` + notRepeated},
		{"a key set twice", "log-level = \"info\"\nlog-level = \"" + p + "\"",
			`read FILE: toml: line 2 (key "log-level"): the key is set twice` + notRepeated},
		{"a number too large for its key", "advertised-port = 99999999999999999999999",
			`read FILE: toml: line 1 (key "advertised-port"): the number is too large for this key` + notRepeated},
		{"a duration that is not one", "fsync-interval = \"" + p + "\"",
			`FILE: fsync-interval: not a duration (one is written "500ms" or "45s")` + notRepeated},
		{"a session timeout that is not a duration", "consumer-session-timeout = \"" + p + "\"",
			`FILE: consumer-session-timeout: not a duration (one is written "500ms" or "45s")` + notRepeated},
		{"an offsets retention that is not a duration", "offsets-retention = \"" + p + "\"",
			`FILE: offsets-retention: not a duration (one is written "500ms" or "45s")` + notRepeated},

		// Under [credentials]: the key is not repeated either. It is where the user name
		// goes, and a password on a line of its own is read as one.
		{"a password that is not quoted", "[credentials]\nalice = " + p,
			`read FILE: toml: line 2 (` + creds + `): ` + notTOML + notRepeated},
		{"a password on a line of its own", "[credentials]\n" + p,
			`read FILE: toml: line 2 (` + creds + `): ` + notTOML + notRepeated},
		{"a password on a line of its own after a good entry", "[credentials]\nalice = \"ok\"\n" + p,
			`read FILE: toml: line 3 (` + creds + `): ` + notTOML + notRepeated},
		{"a password typed as the key", "[credentials]\n" + p + " = = \"x\"",
			`read FILE: toml: line 2 (` + creds + `): ` + notTOML + notRepeated},
		{"a password typed as the key, twice", "[credentials]\n" + p + " = \"a\"\n" + p + " = \"b\"",
			`read FILE: toml: line 3 (` + creds + `): the key is set twice` + notRepeated},
		{"a password as a table's name", "[credentials." + p + "]\nx = 1",
			`read FILE: toml: line 1 (` + creds + `): the value is a table where a string is expected` + notRepeated},
		{"a password in a list", "[credentials]\nalice = [\"" + p + "\"]",
			`read FILE: toml: line 2 (` + creds + `): the value is a list where a string is expected` + notRepeated},
		{"a number for a password", "[credentials]\nalice = 5",
			`read FILE: toml: line 2 (` + creds + `): the value is a number where a string is expected` + notRepeated},
		{"a password with an opening quote only", "[credentials]\n\"" + p + "\n",
			`read FILE: toml: line 2 (` + creds + `): ` + notTOML + notRepeated},
		{"a multi-line password that is not closed", "[credentials]\nalice = '''" + p + "\n",
			`read FILE: toml: line 3 (` + creds + `): ` + notTOML + notRepeated},
		{"an inline table with an unquoted password", "credentials = { alice = " + p + " }",
			`read FILE: toml: line 1 (` + creds + `): ` + notTOML + notRepeated},

		// credentials that is not a table. The TOML library takes any value for a table of
		// strings and decodes nothing, so these loaded: a broker with no users.
		{"credentials as a string", "credentials = \"" + p + "\"",
			`FILE: credentials is not a table: write [credentials] and under it one user = "password" line for each user` + notRepeated},
		{"credentials as a list of tables", "[[credentials]]\nalice = \"" + p + "\"",
			`FILE: credentials is not a table: write [credentials] and under it one user = "password" line for each user` + notRepeated},
		{"credentials as a number", "sasl-mechanisms = [\"PLAIN\"]\ncredentials = 5",
			`FILE: credentials is not a table: write [credentials] and under it one user = "password" line for each user` + notRepeated},
	} {
		path := filepath.Join(t.TempDir(), "doze-kafka.toml")
		if err := os.WriteFile(path, []byte(tc.content+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg := Default()
		err := LoadFile(path, &cfg)
		if err == nil {
			t.Errorf("%s: the file loaded", tc.name)
			continue
		}
		got := err.Error()
		if want := "config: " + strings.ReplaceAll(tc.want, "FILE", path); got != want {
			t.Errorf("%s:\n got %s\nwant %s", tc.name, got, want)
		}
		// Not the value, not the start of it, and not the one letter a parser says it
		// "got": every run of four letters of the planted text is looked for.
		for i := 0; i+4 <= len(p); i++ {
			if strings.Contains(strings.ReplaceAll(got, path, ""), p[i:i+4]) {
				t.Errorf("%s: the error repeats %q of what was written: %s", tc.name, p[i:i+4], got)
			}
		}
		if strings.Contains(got, "'P'") || strings.Contains(got, `"P`) {
			t.Errorf("%s: the error repeats the first letter of what was written: %s", tc.name, got)
		}
	}
}

// An unknown key is named, because the name is how its typo is found; an unknown key
// under [credentials] cannot happen (every key there is a user), and if a file
// arranges one anyway it is counted, not named.
func TestUnknownKeysAreNamedOutsideCredentialsOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "doze-kafka.toml")
	os.WriteFile(path, []byte("retention_ms = 5\n[tls]\ncertt = \"x\"\n"), 0o600)
	cfg := Default()
	err := LoadFile(path, &cfg)
	if err == nil || err.Error() != "config: "+path+": unknown key(s): retention_ms, tls.certt" {
		t.Errorf("unknown keys: %v", err)
	}
	if got := unknownKeys([]string{"credentials.hunter2", "listn", "credentials.a.b"}); got != "listn, 2 under [credentials] (not named: a name there may be a password)" {
		t.Errorf("unknown keys under [credentials]: %q", got)
	}
}

// A file that is not there is said to be not there, with its path.
func TestAMissingConfigFileIsNamed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nope.toml")
	cfg := Default()
	err := LoadFile(path, &cfg)
	if err == nil || !strings.Contains(err.Error(), path) || !os.IsNotExist(unwrapAll(err)) {
		t.Errorf("a missing file: %v", err)
	}
}

func unwrapAll(err error) error {
	for {
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return err
		}
		err = u.Unwrap()
	}
}
