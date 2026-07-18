package console

import (
	"html/template"
	"strings"
)

// jsonTint wraps the tokens of a JSON-looking string in tint spans so message
// values read at a glance: keys in one hue, string/number values in another,
// punctuation dimmed. It's a display heuristic, not a parser — it tolerates
// truncated previews (an unterminated string just tints to the end) and falls
// back to plain escaped text for values that don't look like JSON at all.
// Every text chunk passes through template.HTMLEscapeString before being
// stitched into the returned template.HTML, so untrusted message bytes can't
// smuggle markup.
func jsonTint(s string) template.HTML {
	trimmed := strings.TrimLeft(s, " \t\r\n")
	if !strings.HasPrefix(trimmed, "{") && !strings.HasPrefix(trimmed, "[") {
		return template.HTML(template.HTMLEscapeString(s))
	}

	var b strings.Builder
	b.Grow(len(s) + len(s)/2)
	emit := func(class, text string) {
		if text == "" {
			return
		}
		if class == "" {
			b.WriteString(template.HTMLEscapeString(text))
			return
		}
		b.WriteString(`<span class="`)
		b.WriteString(class)
		b.WriteString(`">`)
		b.WriteString(template.HTMLEscapeString(text))
		b.WriteString(`</span>`)
	}

	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == '"':
			// Scan the whole string literal, honoring escapes; a truncated
			// preview may end mid-string, which just means "no closing quote".
			j := i + 1
			for j < len(s) && s[j] != '"' {
				if s[j] == '\\' && j+1 < len(s) {
					j++
				}
				j++
			}
			if j < len(s) {
				j++ // include the closing quote
			}
			// A string directly followed by ':' is a key.
			k := j
			for k < len(s) && (s[k] == ' ' || s[k] == '\t' || s[k] == '\r' || s[k] == '\n') {
				k++
			}
			if k < len(s) && s[k] == ':' {
				emit("jt-k", s[i:j])
			} else {
				emit("jt-s", s[i:j])
			}
			i = j
		case c == '-' || (c >= '0' && c <= '9'):
			j := i
			for j < len(s) && strings.ContainsRune("-+.eE0123456789", rune(s[j])) {
				j++
			}
			emit("jt-n", s[i:j])
			i = j
		case strings.ContainsRune("{}[],:", rune(c)):
			j := i
			for j < len(s) && strings.ContainsRune("{}[],:", rune(s[j])) {
				j++
			}
			emit("jt-p", s[i:j])
			i = j
		default:
			j := i
			for j < len(s) && s[j] != '"' && !strings.ContainsRune("{}[],:", rune(s[j])) &&
				!(s[j] == '-' || (s[j] >= '0' && s[j] <= '9')) {
				j++
			}
			// The JSON literals read as values, same tint as numbers.
			chunk := s[i:j]
			if t := strings.TrimSpace(chunk); t == "true" || t == "false" || t == "null" {
				lead := strings.Index(chunk, t)
				emit("", chunk[:lead])
				emit("jt-n", t)
				emit("", chunk[lead+len(t):])
			} else {
				emit("", chunk)
			}
			i = j
		}
	}
	return template.HTML(b.String())
}
