package console

import "testing"

func TestProtoTint(t *testing.T) {
	got := string(protoTint("syntax = \"proto3\";\nmessage A<b> {\n  string order_id = 12;\n}"))
	want := "<span class=\"kw\">syntax</span> = <span class=\"s\">&#34;proto3&#34;</span>;\n<span class=\"kw\">message</span> A&lt;b&gt; {\n  <span class=\"kw\">string</span> order_id = <span class=\"n\">12</span>;\n}"
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}
