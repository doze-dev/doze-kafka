package console

import (
	"strings"
	"testing"
)

func TestJSONTint(t *testing.T) {
	cases := []struct {
		name, in string
		want     []string // substrings that must appear
		reject   []string // substrings that must not
	}{
		{
			name: "object with key string number",
			in:   `{"id": "x", "n": 42}`,
			want: []string{`<span class="jt-k">&#34;id&#34;</span>`, `<span class="jt-s">&#34;x&#34;</span>`, `<span class="jt-n">42</span>`},
		},
		{
			name:   "non-JSON stays plain",
			in:     `restock sku-885 qty=60`,
			want:   []string{"restock sku-885 qty=60"},
			reject: []string{"<span"},
		},
		{
			name:   "markup in values is escaped",
			in:     `{"k": "<script>alert(1)</script>"}`,
			want:   []string{"&lt;script&gt;"},
			reject: []string{"<script>"},
		},
		{
			name: "truncated preview does not panic and tints the tail",
			in:   `{"paymentId":"pay_1021","amount":15225,"cu`,
			want: []string{`<span class="jt-n">15225</span>`, `jt-s`},
		},
		{
			name: "literals",
			in:   `{"a": true, "b": null, "c": false}`,
			want: []string{`<span class="jt-n">true</span>`, `<span class="jt-n">null</span>`, `<span class="jt-n">false</span>`},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := string(jsonTint(c.in))
			for _, w := range c.want {
				if !strings.Contains(out, w) {
					t.Errorf("output missing %q:\n%s", w, out)
				}
			}
			for _, r := range c.reject {
				if strings.Contains(out, r) {
					t.Errorf("output must not contain %q:\n%s", r, out)
				}
			}
		})
	}
}
