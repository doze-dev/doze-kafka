package main

import (
	"fmt"
	"sort"
	"strings"
)

// missing is what, for each kind of mutation, a suite that lets it live does
// not assert. It is true of any surviving mutant of the kind that is not an
// equivalent one (a mutant that changes nothing observable); telling those
// apart takes a person, and that is what an entry's reason is for.
var missing = map[string]string{
	"CONDITIONALS_BOUNDARY": "no test puts the two sides equal and asserts which way the comparison goes",
	"CONDITIONALS_NEGATION": "the tests that reach the line assert nothing that depends on which branch is taken",
	"ARITHMETIC_BASE":       "no test asserts the value this expression computes",
	"INCREMENT_DECREMENT":   "no test asserts the count this statement keeps",
	"INVERT_NEGATIVES":      "no test asserts the sign of this value",
}

// Report is plan/evidence/mutation-survivors.md: what the tests do not notice,
// per unit, from the baseline.
func Report(b Baseline) string {
	var w strings.Builder
	w.WriteString("# Mutation survivors\n\n")
	w.WriteString("Written by `go run ./scripts/mutate report` from `testdata/mutation.json`; edit a\n")
	w.WriteString("reason there, not here. A mutant is one operator swapped for another. **Lived**:\n")
	w.WriteString("the tests ran and passed with the code wrong. **Not covered**: no test executes\n")
	w.WriteString("the line. Both are tests to write. **Timed out** and **setup failed** are runs\n")
	w.WriteString("that gave no verdict; neither is a kill. **Not viable** mutants do not compile\n")
	w.WriteString("and say nothing.\n\n")
	w.WriteString("The last column of a lived mutant is its written reason when somebody has\n")
	w.WriteString("examined it. Where it begins *open:* nobody has, and the sentence is the tool's:\n")
	w.WriteString("what a suite that lets that kind of mutant live does not assert, and the tests\n")
	w.WriteString("that execute the line without failing.\n\n")

	names := make([]string, 0, len(b.Units))
	for n := range b.Units {
		names = append(names, n)
	}
	sort.Strings(names)

	w.WriteString("## Every unit\n\n")
	w.WriteString("| Unit | Mutants | Killed | Lived | Not covered | Timed out | Not viable | Setup failed | Open | Tests | Suite | Run | Workers | Platform | Commit | Recorded |\n")
	w.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	var tot UnitBase
	for _, n := range names {
		u := b.Units[n]
		fmt.Fprintf(&w, "| `%s` | %d | %d | %d | %d | %d | %d | %d | %d | %d | %.1f s | %s | %d | %s | `%s` | %s |\n",
			n, u.Mutants, u.Killed, u.Lived, u.NotCovered, u.TimedOut, u.NotViable, u.SetupFailed, u.Open, u.Tests, u.SuiteSeconds, dur(u.Seconds), u.Workers, u.Platform, u.Commit, u.Recorded)
		tot.Mutants += u.Mutants
		tot.Killed += u.Killed
		tot.Lived += u.Lived
		tot.NotCovered += u.NotCovered
		tot.TimedOut += u.TimedOut
		tot.NotViable += u.NotViable
		tot.SetupFailed += u.SetupFailed
		tot.Open += u.Open
		tot.Seconds += u.Seconds
	}
	fmt.Fprintf(&w, "| **all** | %d | %d | %d | %d | %d | %d | %d | %d | | | %s | | | | |\n\n",
		tot.Mutants, tot.Killed, tot.Lived, tot.NotCovered, tot.TimedOut, tot.NotViable, tot.SetupFailed, tot.Open, dur(tot.Seconds))

	for _, n := range names {
		u := b.Units[n]
		fmt.Fprintf(&w, "## %s\n\n", n)
		by := map[Status][]Debt{}
		for _, d := range u.Debt {
			by[d.Status] = append(by[d.Status], d)
		}
		if len(u.Debt) == 0 {
			w.WriteString("Nothing: every mutant that compiles was killed.\n\n")
			continue
		}
		if ds := by[Lived]; len(ds) > 0 {
			fmt.Fprintf(&w, "### Lived (%d)\n\n| Where | Change | Line | What lets it live |\n|---|---|---|---|\n", len(ds))
			for _, d := range ds {
				fmt.Fprintf(&w, "| `%s` | %s | `%s` | %s |\n", where(d), cell(d.Change), cell(d.Source), cell(why(d)))
			}
			w.WriteString("\n")
		}
		for _, s := range []Status{TimedOut, SetupFailed} {
			if ds := by[s]; len(ds) > 0 {
				fmt.Fprintf(&w, "### %s (%d)\n\n| Where | Change | Line | Reason |\n|---|---|---|---|\n", title(s), len(ds))
				for _, d := range ds {
					fmt.Fprintf(&w, "| `%s` | %s | `%s` | %s |\n", where(d), cell(d.Change), cell(d.Source), cell(d.Reason))
				}
				w.WriteString("\n")
			}
		}
		if ds := by[NotCovered]; len(ds) > 0 {
			fmt.Fprintf(&w, "### Not covered (%d)\n\nNo test executes these lines. By declaration:\n\n", len(ds))
			type group struct {
				decl  string
				lines []string
				n     int
			}
			var groups []*group
			idx := map[string]*group{}
			for _, d := range ds {
				file, decl := declOf(d)
				k := file + " " + decl
				g := idx[k]
				if g == nil {
					g = &group{decl: "`" + file + "` `" + decl + "`"}
					idx[k] = g
					groups = append(groups, g)
				}
				g.n++
				if l := lineOf(d); len(g.lines) == 0 || g.lines[len(g.lines)-1] != l {
					g.lines = append(g.lines, l)
				}
			}
			for _, g := range groups {
				fmt.Fprintf(&w, "- %s: %d (lines %s)\n", g.decl, g.n, strings.Join(g.lines, ", "))
			}
			w.WriteString("\n")
		}
	}
	return w.String()
}

func why(d Debt) string {
	if d.Reason != Open && strings.TrimSpace(d.Reason) != "" {
		return d.Reason
	}
	s := "*open:* " + missing[kindOf(d)]
	if d.Reached != "" {
		s += "; reached by " + d.Reached
	}
	return s
}

// where is file:line:col of an entry, without the kind.
func where(d Debt) string {
	if i := strings.LastIndex(d.At, ":"); i >= 0 {
		return d.At[:i]
	}
	return d.At
}

func kindOf(d Debt) string {
	return d.At[strings.LastIndex(d.At, ":")+1:]
}

func lineOf(d Debt) string {
	parts := strings.Split(d.At, ":")
	if len(parts) >= 4 {
		return parts[len(parts)-3]
	}
	return "?"
}

// declOf splits an entry's key (file:declaration:TYPE#n) into file and declaration.
func declOf(d Debt) (file, decl string) {
	i := strings.Index(d.Key, ".go:")
	if i < 0 {
		return d.Key, ""
	}
	file = d.Key[:i+3]
	rest := d.Key[i+4:]
	if j := strings.LastIndex(rest, ":"); j >= 0 {
		rest = rest[:j]
	}
	return file, rest
}

func title(s Status) string {
	switch s {
	case TimedOut:
		return "Timed out"
	case SetupFailed:
		return "Setup failed"
	}
	return string(s)
}

func cell(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "`", "'")
	return strings.ReplaceAll(s, "\n", " ")
}

func dur(seconds float64) string {
	if seconds < 90 {
		return fmt.Sprintf("%.0f s", seconds)
	}
	return fmt.Sprintf("%.0f min", seconds/60)
}
