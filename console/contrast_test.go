package console

import (
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
)

var updateContrast = flag.Bool("update-contrast", false, "rewrite testdata/contrast.txt from the stylesheet")

// Contrast is measured, not believed. Every text token on every surface it is drawn
// on, in all three themes, is computed with WCAG's relative luminance and held to
// 4.5:1 (3:1 for the things that are not text: a control's edge, the focus ring, a
// status dot). The computed ratios are written to testdata/contrast.txt, which is
// where the Themes board's numbers come from.

type contrastPair struct {
	fg, bg string
	min    float64
	what   string
}

func contrastPairs() []contrastPair {
	var ps []contrastPair
	// The board's surfaces: the page (--bg), a card or the bars (--card), a well or
	// a table's head (--well) and the selected row (--sel).
	surfaces := []string{"--bg", "--card", "--well", "--sel"}
	for _, bg := range surfaces {
		ps = append(ps, contrastPair{"--ink", bg, 4.5, "text"}, contrastPair{"--mut", bg, 4.5, "secondary text"})
	}
	// Status words and numbers, on the surfaces and on their own tints (a row, a
	// callout or a pill drawn in that status).
	for _, tok := range []string{"--ok", "--warn", "--bad", "--deny"} {
		for _, bg := range surfaces {
			ps = append(ps, contrastPair{tok, bg, 4.5, "status text"})
		}
		ps = append(ps, contrastPair{tok, tok + "bg", 4.5, "status text on its own tint"})
	}
	ps = append(ps,
		contrastPair{"--card", "--ink", 4.5, "text on the primary button and the selected segment"},
		contrastPair{"--card", "--bad", 4.5, "text on a destructive button"},
		// Not text: held to 3:1.
		contrastPair{"--ink", "--bg", 3, "focus ring and the selection outline"},
		contrastPair{"--ink", "--card", 3, "focus ring and the selection outline"},
	)
	for _, tok := range []string{"--ok", "--warn", "--bad", "--deny"} {
		ps = append(ps, contrastPair{tok, "--bg", 3, "status dot or bar"}, contrastPair{tok, "--card", 3, "status dot or bar"})
	}
	// The section chips: white letters on the section's colour.
	for _, tok := range []string{"--c-tr", "--c-tc", "--c-tp", "--c-gr", "--c-sc", "--c-ac", "--c-cl"} {
		ink := "--chip-ink"
		if tok == "--c-tp" || tok == "--c-tr" {
			ink = "--chip-ink-dark" // the board's fill cannot carry white letters at 4.5:1
		}
		ps = append(ps, contrastPair{ink, tok, 4.5, "letters on a section chip"})
	}
	return ps
}

// themeTokens is a theme's tokens: its own block over the light base.
func themeTokens(css, theme string) map[string]string {
	out := map[string]string{}
	for k, v := range tokensIn(css, ":root {") {
		out[k] = v
	}
	if theme != "light" {
		for k, v := range tokensIn(css, `:root[data-theme="`+theme+`"] {`) {
			out[k] = v
		}
	}
	return out
}

// luminance is WCAG 2's relative luminance of #rrggbb.
func luminance(hex string) (float64, error) {
	h := strings.TrimPrefix(strings.TrimSpace(hex), "#")
	if len(h) == 3 {
		h = string([]byte{h[0], h[0], h[1], h[1], h[2], h[2]})
	}
	if len(h) != 6 {
		return 0, fmt.Errorf("%q is not a #rrggbb colour", hex)
	}
	var c [3]float64
	for i := 0; i < 3; i++ {
		v, err := strconv.ParseUint(h[2*i:2*i+2], 16, 8)
		if err != nil {
			return 0, err
		}
		f := float64(v) / 255
		if f <= 0.03928 {
			c[i] = f / 12.92
		} else {
			c[i] = math.Pow((f+0.055)/1.055, 2.4)
		}
	}
	return 0.2126*c[0] + 0.7152*c[1] + 0.0722*c[2], nil
}

func ratio(a, b string) (float64, error) {
	la, err := luminance(a)
	if err != nil {
		return 0, err
	}
	lb, err := luminance(b)
	if err != nil {
		return 0, err
	}
	if la < lb {
		la, lb = lb, la
	}
	return (la + 0.05) / (lb + 0.05), nil
}

func TestEveryTextAndBackgroundPairHoldsItsContrastInAllThreeThemes(t *testing.T) {
	css := readAll(t, "static/app.css")
	var report strings.Builder
	for _, theme := range []string{"light", "dark", "contrast"} {
		toks := themeTokens(css, theme)
		fmt.Fprintf(&report, "# %s\n", theme)
		for _, p := range contrastPairs() {
			fg, bg := toks[p.fg], toks[p.bg]
			if fg == "" || bg == "" {
				t.Errorf("%s: %s or %s is not defined", theme, p.fg, p.bg)
				continue
			}
			r, err := ratio(fg, bg)
			if err != nil {
				t.Errorf("%s: %s on %s: %v", theme, p.fg, p.bg, err)
				continue
			}
			fmt.Fprintf(&report, "%-22s on %-12s %5.2f  (min %.1f)  %s\n", p.fg, p.bg, r, p.min, p.what)
			if r < p.min {
				t.Errorf("%s: %s (%s) on %s (%s) is %.2f:1, below %.1f:1 (%s)", theme, p.fg, fg, p.bg, bg, r, p.min, p.what)
			}
		}
		// The high-contrast theme is held to more than the minimum: that is its point.
		if theme == "contrast" {
			for _, p := range contrastPairs() {
				// The chips are drawn as the boards draw them in every theme (filled,
				// white letters), so they hold the minimum, not the 7:1 of the text.
				if p.min != 4.5 || strings.HasPrefix(p.fg, "--chip-ink") {
					continue
				}
				if r, err := ratio(toks[p.fg], toks[p.bg]); err == nil && r < 7 {
					t.Errorf("contrast: %s on %s is %.2f:1; the high-contrast theme holds text to 7:1", p.fg, p.bg, r)
				}
			}
		}
	}
	const golden = "testdata/contrast.txt"
	if *updateContrast {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(golden, []byte(report.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("%v (run go test ./console -run Contrast -update-contrast)", err)
	}
	if string(want) != report.String() {
		t.Errorf("the computed ratios changed: run go test ./console -run Contrast -update-contrast and commit testdata/contrast.txt (the Themes board prints these)")
	}
}

// The measure itself is right: WCAG's own examples.
func TestContrastRatioMatchesTheDefinition(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want float64
	}{{"#000000", "#ffffff", 21}, {"#ffffff", "#ffffff", 1}, {"#777777", "#ffffff", 4.48}, {"#767676", "#ffffff", 4.54}} {
		got, err := ratio(tc.a, tc.b)
		if err != nil || math.Abs(got-tc.want) > 0.02 {
			t.Errorf("ratio(%s, %s) = %.2f (%v), want %.2f", tc.a, tc.b, got, err, tc.want)
		}
	}
}
