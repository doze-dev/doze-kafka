package console

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"syscall"
)

// errNotFound marks "this thing does not exist", which deserves a 404 page that
// helps (did you mean…) rather than a generic failure.
var errNotFound = errors.New("not found")

// errBadInput marks a request the person can correct (a malformed header, a
// partition that does not exist). It is shown beside the form, not as a failure.
var errBadInput = errors.New("invalid input")

// isNetworkError reports whether err is a failure to talk to the broker at all.
// It goes by the error's type; the client's own "could not dial" is the one case
// with no type to test, and is matched as a prefix, not anywhere in the text.
func isNetworkError(err error) bool {
	var ne net.Error
	if errors.As(err, &ne) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, kgo.ErrClientClosed) {
		return true
	}
	return strings.HasPrefix(err.Error(), "unable to dial")
}

// wantsPage reports whether this request is a navigation (a plain page load or a
// boosted link) that should get a whole page in the shell, as opposed to a partial
// or a form submission from a control that can only show a fragment.
func wantsPage(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	return r.Header.Get("HX-Request") != "true" || r.Header.Get("HX-Boosted") == "true"
}

// fail reports err. A navigation gets an in-shell page with the way out on it; a
// partial request gets a small fragment the client shows next to the control that
// failed. A nil error (a handler that found nothing without erroring) is still a
// failure to report, not a crash. Either way what is shown is a Refusal, through
// the one "refusal" partial.
func (c *Console) fail(w http.ResponseWriter, r *http.Request, err error) {
	f := refusalFor(err)
	if wantsPage(r) {
		c.renderError(w, r, f, errorPage{})
		return
	}
	c.refuse(w, f)
}

// refuse writes a Refusal as the small fragment for a control that failed.
func (c *Console) refuse(w http.ResponseWriter, f Refusal) {
	c.execute(w, f.Status, "refusal", f.data(nil), false)
}

// failResource is fail for a page about one named thing. When that thing does not
// exist it says so by name and offers the nearest names that do.
func (c *Console) failResource(w http.ResponseWriter, r *http.Request, err error, kind, name string) {
	f := refusalFor(err)
	if !f.NotFound || !wantsPage(r) {
		c.fail(w, r, err)
		return
	}
	f.Heading = strings.ToUpper(kind[:1]) + kind[1:] + " not found"
	f.Message = fmt.Sprintf("There is no %s named %q on this broker.", kind, name)
	page := errorPage{Section: kind + "s", Back: c.prefix + "/" + kind + "s"}
	page.Suggestions = c.suggest(r.Context(), kind, name)
	c.renderError(w, r, f, page)
}

// errorPage is the extra context an error page can carry.
type errorPage struct {
	Section     string // "topics" or "groups": which rail item stays lit.
	Back        string
	Suggestions []suggestion
}

type suggestion struct{ Name, URL string }

func (c *Console) renderError(w http.ResponseWriter, r *http.Request, f Refusal, p errorPage) {
	data := f.data(map[string]any{
		"Title": f.Heading, "Heading": f.Heading, "Message": f.Message, "Kind": string(f.Kind),
		"Status": f.Status, "Suggestions": p.Suggestions, "Back": p.Back, "Retry": f.Retry,
		"Page": p.Section, "View": "error",
	})
	if p.Section == "" {
		data["Page"] = "topics"
	}
	// Tells the client this is a page to show, not an error to toast: htmx does not
	// swap a 4xx/5xx response unless told to (see the beforeSwap handler).
	w.Header().Set("X-Doze-Page", "error")
	c.shellData(r, data)
	c.execute(w, f.Status, "error_page", data, true)
}

// suggest returns up to three existing names close to the one asked for: those
// that contain it (or it them), then those a couple of edits away.
func (c *Console) suggest(ctx context.Context, kind, name string) []suggestion {
	var names []string
	switch kind {
	case "topic":
		if ts, err := c.be.ListTopics(ctx); err == nil {
			for _, t := range ts {
				names = append(names, t.Name)
			}
		}
	case "group":
		if gs, err := c.be.ListGroups(ctx); err == nil {
			for _, g := range gs {
				names = append(names, g.ID)
			}
		}
	}
	urlFor := c.topicURL
	if kind == "group" {
		urlFor = c.groupURL
	}
	return c.closeNames(names, name, urlFor)
}

// closeNames returns up to three of names that are close to name: those that contain
// it (or it them), then those a couple of edits away, each with its link.
func (c *Console) closeNames(names []string, name string, urlFor func(string) string) []suggestion {
	lower := strings.ToLower(name)
	type scored struct {
		name  string
		score int
	}
	var cands []scored
	for _, n := range names {
		ln := strings.ToLower(n)
		switch {
		case ln == lower:
			continue
		case strings.Contains(ln, lower) || strings.Contains(lower, ln):
			cands = append(cands, scored{n, 0})
		default:
			// Close enough to be a typo: at most a third of the name's letters differ
			// (never offering "zz" for "a"), and no more than a couple plus a little.
			if d := editDistance(ln, lower); d <= min(2+len(lower)/6, max(len(lower), len(ln))/3) {
				cands = append(cands, scored{n, d})
			}
		}
	}
	// Stable order: closest first, then alphabetical.
	for i := 1; i < len(cands); i++ {
		for j := i; j > 0 && (cands[j].score < cands[j-1].score || (cands[j].score == cands[j-1].score && cands[j].name < cands[j-1].name)); j-- {
			cands[j], cands[j-1] = cands[j-1], cands[j]
		}
	}
	var out []suggestion
	for _, s := range cands {
		if len(out) == 3 {
			break
		}
		out = append(out, suggestion{s.name, urlFor(s.name)})
	}
	return out
}

// editDistance is the Levenshtein distance between two short strings.
func editDistance(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	prev := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		cur := make([]int, len(rb)+1)
		cur[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev = cur
	}
	return prev[len(rb)]
}
