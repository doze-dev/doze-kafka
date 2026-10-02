package console

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
)

// errNotFound marks "this thing does not exist", which deserves a 404 page that
// helps (did you mean…) rather than a generic failure.
var errNotFound = errors.New("not found")

// errBadInput marks a request the person can correct (a malformed header, a
// partition that does not exist). It is shown beside the form, not as a failure.
var errBadInput = errors.New("invalid input")

// failure is a backend error sorted into what the person looking at the screen
// needs to be told.
type failure struct {
	status  int
	kind    string // "notfound", "unreachable", "refused", "internal"
	title   string
	message string
}

// classify decides how an error is shown. The distinctions matter: "that topic
// does not exist" and "I cannot reach your broker" call for different next steps,
// and both used to be the same raw error string in a toast.
func classify(err error) failure {
	switch {
	case errors.Is(err, errNotFound),
		errors.Is(err, kerr.UnknownTopicOrPartition),
		errors.Is(err, kerr.UnknownTopicID),
		errors.Is(err, kerr.GroupIDNotFound):
		return failure{http.StatusNotFound, "notfound", "Not found", err.Error()}
	case errors.Is(err, errBadInput):
		return failure{http.StatusBadRequest, "refused", "Check the form", err.Error()}
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled), isNetworkError(err):
		return failure{http.StatusBadGateway, "unreachable", "Can't reach the broker",
			"The console could not get an answer from the broker. It may have stopped, or be busy."}
	}
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return failure{http.StatusBadRequest, "refused", "The broker refused that", err.Error()}
	}
	return failure{http.StatusInternalServerError, "internal", "Something went wrong", err.Error()}
}

func isNetworkError(err error) bool {
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "unable to dial") || strings.Contains(s, "connection refused") ||
		strings.Contains(s, "broken pipe") || strings.Contains(s, "connection reset") ||
		strings.Contains(s, "EOF") || strings.Contains(s, "client closed")
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
// failure to report, not a crash.
func (c *Console) fail(w http.ResponseWriter, r *http.Request, err error) {
	if err == nil {
		err = fmt.Errorf("%w: nothing to show", errNotFound)
	}
	f := classify(err)
	if wantsPage(r) {
		c.renderError(w, r, f, errorPage{})
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(f.status)
	io.WriteString(w, `<div class="err">`+template.HTMLEscapeString(err.Error())+`</div>`)
}

// failResource is fail for a page about one named thing. When that thing does not
// exist it says so by name and offers the nearest names that do.
func (c *Console) failResource(w http.ResponseWriter, r *http.Request, err error, kind, name string) {
	f := classify(err)
	if f.kind != "notfound" || !wantsPage(r) {
		c.fail(w, r, err)
		return
	}
	f.title = strings.ToUpper(kind[:1]) + kind[1:] + " not found"
	f.message = fmt.Sprintf("There is no %s named %q on this broker.", kind, name)
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

func (c *Console) renderError(w http.ResponseWriter, r *http.Request, f failure, p errorPage) {
	data := map[string]any{
		"Title": f.title, "Heading": f.title, "Message": f.message, "Kind": f.kind,
		"Status": f.status, "Suggestions": p.Suggestions, "Back": p.Back,
		"Retry": f.kind == "unreachable" || f.kind == "internal",
		"Page":  p.Section,
	}
	if p.Section == "" {
		data["Page"] = "topics"
	}
	// Tells the client this is a page to show, not an error to toast: htmx does not
	// swap a 4xx/5xx response unless told to (see the beforeSwap handler).
	w.Header().Set("X-Doze-Page", "error")
	w.WriteHeader(f.status)
	data["Prefix"] = c.prefix
	data["Endpoint"] = c.be.displayAddr()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := c.templates().ExecuteTemplate(w, "error_page", data); err != nil {
		io.WriteString(w, template.HTMLEscapeString(f.message))
	}
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
			if d := editDistance(ln, lower); d <= 2+len(lower)/6 {
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
		u := c.topicURL(s.name)
		if kind == "group" {
			u = c.groupURL(s.name)
		}
		out = append(out, suggestion{s.name, u})
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
