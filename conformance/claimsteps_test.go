package conformance

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// A recorded claim says which steps of its recording show it, and what they
// show, in its Evidence cell after the recording's path:
//
//	conformance/recordings/<file>.json #<step>=<path>:<value> #<step>=<path>:<value> …
//
// <step> is a step's label, in double quotes when it has a space in it. <path> is
// a field of that step's response, written as the replay writes a difference
// (Topics[0].Partitions[0].ErrorCode, or (connection) for a closed connection);
// a path that starts with "request." is a field of the request. A recording that
// is not a scenario's (api-versions.json, client-telemetry.json) has no steps:
// its checks name the step (file), and the path starts at the file's top. <value> is what
// the field holds: a number, true, false, null, or text, in double quotes when it
// has a space in it. TestEveryRecordedClaimNamesStepsThatHoldIt fails a recorded
// row with no check, a step its recording does not have, and a value that is not
// the one recorded, so the row's words cannot drift from its recording.
var stepCheckRE = regexp.MustCompile(`#(?:"([^"]+)"|([^\s="]+))=([^:\s]+):(?:"([^"]*)"|(\S+))`)

const fileStep = "(file)"

// StepCheck is one #step=path:value of a recorded claim.
type StepCheck struct{ Step, Path, Value string }

// EvidenceChecks splits a recorded claim's Evidence cell into the recording's
// path and its step checks.
func EvidenceChecks(evidence string) (file string, checks []StepCheck, bad []string) {
	file, rest, _ := strings.Cut(strings.TrimSpace(evidence), " ")
	rest = strings.TrimSpace(rest)
	for _, m := range stepCheckRE.FindAllStringSubmatchIndex(rest, -1) {
		get := func(i int) string {
			if m[2*i] < 0 {
				return ""
			}
			return rest[m[2*i]:m[2*i+1]]
		}
		c := StepCheck{Step: get(1) + get(2), Path: get(3), Value: get(4) + get(5)}
		checks = append(checks, c)
	}
	if left := strings.TrimSpace(stepCheckRE.ReplaceAllString(rest, "")); left != "" {
		bad = append(bad, "not a #step=path:value check: "+left)
	}
	return file, checks, bad
}

// checkSteps reports what is wrong with a recorded claim's step checks.
func checkSteps(c Claim, root string) []string {
	where := c.File + " " + c.ID
	file, checks, bad := EvidenceChecks(c.Evidence)
	for i := range bad {
		bad[i] = where + ": " + bad[i]
	}
	if len(checks) == 0 {
		return append(bad, where+": a recorded claim names the step(s) that show it: "+file+" #<step>=<path>:<value>")
	}
	raw, err := os.ReadFile(filepath.Join(root, "..", file))
	if err != nil {
		return append(bad, where+": "+err.Error())
	}
	var whole any
	if err := json.Unmarshal(raw, &whole); err != nil {
		return append(bad, where+": "+file+": "+err.Error())
	}
	var steps []any
	if m, ok := whole.(map[string]any); ok {
		steps, _ = m["steps"].([]any)
	}
	for _, ch := range checks {
		var step map[string]any
		if ch.Step == fileStep {
			step = map[string]any{"response": whole}
		}
		for _, s := range steps {
			if m, _ := s.(map[string]any); step == nil && m["label"] == ch.Step {
				step = m
			}
		}
		if step == nil {
			bad = append(bad, fmt.Sprintf("%s: %s has no step %q", where, file, ch.Step))
			continue
		}
		root, path := step["response"], ch.Path
		if p, ok := strings.CutPrefix(path, "request."); ok {
			root, path = step["request"], p
		}
		got, ok := lookup(root, path)
		if !ok {
			bad = append(bad, fmt.Sprintf("%s: step %q of %s has no %s", where, ch.Step, file, ch.Path))
			continue
		}
		if s := plain(got); s != ch.Value {
			bad = append(bad, fmt.Sprintf("%s: step %q of %s has %s = %s, the row says %s", where, ch.Step, file, ch.Path, s, ch.Value))
		}
	}
	return bad
}

var indexRE = regexp.MustCompile(`^([^\[]*)((?:\[\d+\])*)$`)

// lookup follows a dotted path with [i] indexes through decoded JSON.
func lookup(v any, path string) (any, bool) {
	for _, part := range splitPath(path) {
		m := indexRE.FindStringSubmatch(part)
		if m == nil {
			return nil, false
		}
		if m[1] != "" {
			obj, ok := v.(map[string]any)
			if !ok {
				return nil, false
			}
			if v, ok = obj[m[1]]; !ok {
				return nil, false
			}
		}
		for _, ix := range strings.Split(strings.Trim(m[2], "[]"), "][") {
			if ix == "" {
				continue
			}
			i, _ := strconv.Atoi(ix)
			list, ok := v.([]any)
			if !ok || i >= len(list) {
				return nil, false
			}
			v = list[i]
		}
	}
	return v, true
}

// splitPath splits at dots outside parentheses, so (connection) is one part.
func splitPath(p string) []string {
	var out []string
	depth, start := 0, 0
	for i, r := range p {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case '.':
			if depth == 0 {
				out = append(out, p[start:i])
				start = i + 1
			}
		}
	}
	return append(out, p[start:])
}

// plain is a decoded JSON value as a row writes it.
func plain(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	}
	b, _ := json.Marshal(v)
	return string(b)
}
