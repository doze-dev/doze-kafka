package console

import "testing"

// Edges of the JSON tinter: what a user sees for truncated previews, escaped
// quotes, digits at either end of the range and runs that end the value.
func TestJSONTintEdges(t *testing.T) {
	k := func(s string) string { return `<span class="jt-k">` + s + `</span>` }
	str := func(s string) string { return `<span class="jt-s">` + s + `</span>` }
	n := func(s string) string { return `<span class="jt-n">` + s + `</span>` }
	p := func(s string) string { return `<span class="jt-p">` + s + `</span>` }
	cases := []struct{ name, in, want string }{
		{"escaped quote stays inside the string", `["a\"b"]`, p("[") + str(`&#34;a\&#34;b&#34;`) + p("]")},
		{"escaped backslash then quote closes", `["a\\"]`, p("[") + str(`&#34;a\\&#34;`) + p("]")},
		{"a preview cut right after a backslash", `["ab\`, p("[") + str(`&#34;ab\`)},
		{"a preview cut inside a string", `["ab`, p("[") + str(`&#34;ab`)},
		{"key with spaces before the colon", `{"a" :1}`, p("{") + k(`&#34;a&#34;`) + " " + p(":") + n("1") + p("}")},
		{"key with newline and tab before the colon", "{\"a\"\t\r\n:1}", p("{") + k(`&#34;a&#34;`) + "\t\r\n" + p(":") + n("1") + p("}")},
		{"string cut after a key", `{"a"`, p("{") + str(`&#34;a&#34;`)},
		{"string cut after a key and spaces", `{"a"  `, p("{") + str(`&#34;a&#34;`) + "  "},
		{"string value is not a key", `["a",1]`, p("[") + str(`&#34;a&#34;`) + p(",") + n("1") + p("]")},
		{"zero", `[0]`, p("[") + n("0") + p("]")},
		{"nine", `[9]`, p("[") + n("9") + p("]")},
		{"negative exponent float", `[-1.5e+3]`, p("[") + n("-1.5e+3") + p("]")},
		{"number cut at the end", `{"a":12`, p("{") + k(`&#34;a&#34;`) + p(":") + n("12")},
		{"punctuation run at the end", `{"a":[]}`, p("{") + k(`&#34;a&#34;`) + p(":[]}")},
		{"literals", `[true, false ,null]`, p("[") + n("true") + p(",") + " " + n("false") + " " + p(",") + n("null") + p("]")},
		{"plain text stops at a digit and a minus", `[ab9c-d]`, p("[") + "ab" + n("9") + "c" + n("-") + "d" + p("]")},
		{"plain text stops at zero", `[x0y]`, p("[") + "x" + n("0") + "y" + p("]")},
		{"plain text runs to the end", `[abc`, p("[") + "abc"},
		{"markup in a value is escaped", `{"a":"<b>"}`, p("{") + k(`&#34;a&#34;`) + p(":") + str(`&#34;&lt;b&gt;&#34;`) + p("}")},
	}
	for _, c := range cases {
		if got := string(jsonTint(c.in)); got != c.want {
			t.Errorf("%s: jsonTint(%q)\n got %s\nwant %s", c.name, c.in, got, c.want)
		}
	}
}
