package console

import (
	"html/template"
	"regexp"
	"strings"
	"sync"
)

// protoTint colours .proto text the way the boards do (Sc-07): keywords and scalar
// types as .kw, string literals as .s, numbers as .n. The text is escaped; nothing
// else changes.
//
//fuzz:not-a-parser colours tokens of text the registry printed; a pattern finds them and everything between is escaped as it is
func protoTint(s string) template.HTML {
	var b strings.Builder
	last := 0
	for _, m := range protoTokenRe().FindAllStringSubmatchIndex(s, -1) {
		b.WriteString(template.HTMLEscapeString(s[last:m[0]]))
		class := "n"
		switch {
		case m[2] >= 0:
			class = "s"
		case m[4] >= 0:
			class = "kw"
		}
		b.WriteString(`<span class="` + class + `">` + template.HTMLEscapeString(s[m[0]:m[1]]) + `</span>`)
		last = m[1]
	}
	b.WriteString(template.HTMLEscapeString(s[last:]))
	return template.HTML(b.String())
}

// protoTokenRe is built on first use: it is a long alternation, and a broker whose
// console never shows a Protobuf schema should not hold it (testdata/lightness.json).
var protoTokenRe = sync.OnceValue(func() *regexp.Regexp {
	return regexp.MustCompile(`("(?:[^"\\]|\\.)*")|\b(syntax|package|import|option|message|enum|oneof|map|service|rpc|returns|reserved|extend|repeated|optional|required|double|float|int32|int64|uint32|uint64|sint32|sint64|fixed32|fixed64|sfixed32|sfixed64|bool|string|bytes)\b|\b\d+\b`)
})
