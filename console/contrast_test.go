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
	surfaces := []string{"--background", "--card", "--popover", "--muted", "--sidebar"}
	for _, bg := range surfaces {
		ps = append(ps, contrastPair{"--foreground", bg, 4.5, "text"}, contrastPair{"--muted-foreground", bg, 4.5, "secondary text"})
	}
	ps = append(ps, contrastPair{"--foreground", "--accent", 4.5, "text on the selected row"}, contrastPair{"--muted-foreground", "--accent", 4.5, "secondary text on the selected row"})
	for _, tok := range []string{"--ok-foreground", "--bad-foreground", "--warn-foreground", "--info-foreground", "--violet-foreground",
		"--refused-foreground", "--denied-foreground", "--notimpl-foreground", "--failed-foreground"} {
		for _, bg := range []string{"--background", "--card", "--muted"} {
			ps = append(ps, contrastPair{tok, bg, 4.5, "status text"})
		}
	}
	ps = append(ps,
		contrastPair{"--brand-foreground", "--brand", 4.5, "text on the brand yellow"},
		contrastPair{"--destructive-foreground", "--destructive", 4.5, "text on a destructive button"},
		// Not text: held to 3:1.
		contrastPair{"--ring", "--background", 3, "focus ring"},
		contrastPair{"--ring", "--card", 3, "focus ring"},
		contrastPair{"--input", "--background", 3, "a control's edge"},
		contrastPair{"--input", "--card", 3, "a control's edge"},
	)
	for _, tok := range []string{"--ok", "--bad", "--warn", "--info", "--refused", "--denied", "--notimpl", "--failed"} {
		ps = append(ps, contrastPair{tok, "--background", 3, "status dot or bar"}, contrastPair{tok, "--card", 3, "status dot or bar"})
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
				if p.min != 4.5 {
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
