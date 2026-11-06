package schemaregistry

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The registry's request log (SCH-21): the newest maxRequests requests, kept in memory
// for the console's "wire, underneath" strip, since the broker's traffic recorder sees
// only Kafka requests. It keeps what was asked and what came of it, never a body.

const (
	maxRequests   = 2000
	maxRequestURL = 512 // a path past this is kept cut short
	// ownHeader marks a request as the console's own: it is answered as any other and
	// not kept, so the console's polling does not push out what clients asked.
	ownHeader = "X-Doze-Own"
)

// RegistryRequest is one request the registry answered.
type RegistryRequest struct {
	Seq     uint64        `json:"seq"`
	At      time.Time     `json:"at"`
	Method  string        `json:"method"`
	Path    string        `json:"path"`
	Status  int           `json:"status"`
	Took    time.Duration `json:"tookNanos"`
	User    string        `json:"user,omitempty"`
	From    string        `json:"from,omitempty"`
	Agent   string        `json:"agent,omitempty"`
	Subject string        `json:"subject,omitempty"` // named in the path
	ID      int           `json:"id,omitempty"`      // a schema id named in the path
}

type requestRing struct {
	mu   sync.Mutex
	buf  []RegistryRequest
	next int
	seq  uint64
}

func (q *requestRing) add(rr RegistryRequest) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.seq++
	rr.Seq = q.seq
	if len(q.buf) < maxRequests {
		q.buf = append(q.buf, rr)
		return
	}
	q.buf[q.next] = rr
	q.next = (q.next + 1) % maxRequests
}

// newestFirst copies the ring out, newest first.
func (q *requestRing) newestFirst() []RegistryRequest {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]RegistryRequest, 0, len(q.buf))
	for i := len(q.buf) - 1; i >= 0; i-- {
		out = append(out, q.buf[(q.next+i)%len(q.buf)])
	}
	return out
}

// record keeps a request, unless it is the console's own or an extension route.
func (a *API) record(r *http.Request, status int, took time.Duration) {
	if r.Header.Get(ownHeader) != "" || strings.HasPrefix(r.URL.Path, "/_doze/") || status == 0 {
		return
	}
	o := originOf(r)
	path := r.URL.Path
	if len(path) > maxRequestURL {
		path = path[:maxRequestURL]
	}
	rr := RegistryRequest{At: time.Now().UTC(), Method: r.Method, Path: path, Status: status, Took: took,
		User: o.User, From: o.From, Agent: o.Agent, Subject: r.PathValue("subject")}
	if id, err := strconv.Atoi(r.PathValue("id")); err == nil {
		rr.ID = id
	}
	if len(rr.Subject) > maxRequestURL {
		rr.Subject = rr.Subject[:maxRequestURL]
	}
	a.reqs.add(rr)
}

// RequestFilter chooses requests: by subject (a request by id counts for each subject
// holding that schema), older than a sequence number, and how many.
type RequestFilter struct {
	Subject string
	Before  uint64
	Limit   int
}

// RequestPage is a page of requests, newest first, and the cursor for the next.
type RequestPage struct {
	Requests []RegistryRequest `json:"requests"`
	Next     uint64            `json:"next,omitempty"` // pass as before for older ones; 0 when there are none
	Kept     int               `json:"kept"`           // how many the log holds, of at most Max
	Max      int               `json:"max"`
}

// Requests answers a filter over the request log.
func (a *API) Requests(f RequestFilter) RequestPage {
	switch {
	case f.Limit <= 0:
		f.Limit = 100
	case f.Limit > maxRequests:
		f.Limit = maxRequests
	}
	all := a.reqs.newestFirst()
	var ids map[int]bool
	if f.Subject != "" {
		ids = a.reg.idsOf(f.Subject)
	}
	page := RequestPage{Requests: []RegistryRequest{}, Kept: len(all), Max: maxRequests}
	for _, rr := range all {
		if f.Before != 0 && rr.Seq >= f.Before {
			continue
		}
		if f.Subject != "" && rr.Subject != f.Subject && !(rr.ID != 0 && ids[rr.ID]) {
			continue
		}
		if len(page.Requests) == f.Limit {
			page.Next = page.Requests[len(page.Requests)-1].Seq
			break
		}
		page.Requests = append(page.Requests, rr)
	}
	return page
}

// idsOf is the schema ids a subject holds, deleted versions included.
func (r *Registry) idsOf(subject string) map[int]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[int]bool{}
	for _, v := range r.subj[subject] {
		out[v.ID] = true
	}
	return out
}
