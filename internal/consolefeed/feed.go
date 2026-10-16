// Package consolefeed is what a console is given to show a broker's traffic and its
// clients' own metrics: the recorder and the telemetry store of a server.Server, as
// the console's types. The console is a client of the broker and does not import its
// internals; whoever starts one beside a broker in the same process (the doze-kafka
// command, and the doze module's embedded broker) joins the two with this.
package consolefeed

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/doze-dev/doze-kafka/console"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/telemetry"
	"github.com/doze-dev/doze-kafka/internal/traffic"
)

// trafficSource gives the console what the broker's recorder holds, in the console's
// own types: the console is a client of the broker and does not import its internals,
// so the two are joined here, where both are known.
type trafficSource struct {
	rec     *traffic.Recorder
	started time.Time
}

// Traffic is the console's view of the recorder, or nil when there is none (a
// server without a console keeps no record).
func Traffic(rec *traffic.Recorder) console.TrafficSource {
	if rec == nil {
		return nil
	}
	return trafficSource{rec: rec, started: time.Now()}
}

func filterOf(f console.TrafficFilter) traffic.Filter {
	return traffic.Filter{Client: f.Client, API: f.API, Topic: f.Topic, Group: f.Group, Kind: f.Kind, ErrorsOnly: f.ErrorsOnly, Any: f.Any}
}

func eventOf(e traffic.Event) console.TrafficEvent {
	out := console.TrafficEvent{
		ID: e.ID, Seq: e.Seq, First: e.First, Time: e.Time, Count: e.Count, Conn: e.Conn,
		Remote: e.Remote, ClientID: e.ClientID, Software: e.Software, Principal: e.Principal,
		API: e.API, Version: e.Version, Kind: e.Kind, Correlation: e.Correlation,
		Topics: e.Topics, TopicsMore: e.TopicsMore, Group: e.Group, ViaGroup: e.ViaGroup, TxnID: e.TxnID,
		Detail: e.Detail, Error: e.Error, ErrorCode: e.ErrorCode, Class: string(e.Class), Closed: e.Closed,
		Duration: e.Duration, ReqBytes: e.ReqBytes, RespBytes: e.RespBytes,
		Parts: e.Parts, Gen: e.Gen, HasGen: e.HasGen, AutoCreate: e.AutoCreate,
	}
	out.Facts = console.TrafficFacts{Acks: int(e.Facts.Acks), HasAcks: e.Facts.HasAcks, PartitionsMore: e.Facts.PartitionsMore, Decoded: e.Facts.Decoded}
	for _, p := range e.Facts.Partitions {
		out.Facts.Partitions = append(out.Facts.Partitions, console.TrafficPart{
			Topic: p.Topic, Partition: p.Partition, Offset: p.Offset, HasOffset: p.HasOffset,
			Records: p.Records, Bytes: p.Bytes, ErrorCode: p.ErrorCode, Error: protocol.ErrorName(p.ErrorCode),
		})
	}
	for _, t := range e.Ticks {
		out.Ticks = append(out.Ticks, console.TrafficTick{At: t.At, Duration: t.Duration, Class: string(t.Class)})
	}
	return out
}

func eventsOf(evs []traffic.Event) []console.TrafficEvent {
	out := make([]console.TrafficEvent, len(evs))
	for i, e := range evs {
		out[i] = eventOf(e)
	}
	return out
}

func (s trafficSource) Events(f console.TrafficFilter, limit int) []console.TrafficEvent {
	return eventsOf(s.rec.Events(filterOf(f), limit))
}

func (s trafficSource) Query(f console.TrafficFilter, limit int) console.TrafficResult {
	res := s.rec.Query(traffic.Query{Filter: filterOf(f), Limit: limit})
	return console.TrafficResult{Rows: eventsOf(res.Rows), Total: res.Total, Requests: res.Requests, Held: res.Held,
		Newest: res.Newest, Oldest: res.Oldest}
}

func (s trafficSource) Row(id string) (console.TrafficEvent, bool) {
	e, ok := s.rec.Row(id)
	return eventOf(e), ok
}

func (s trafficSource) NewSince(f console.TrafficFilter, t time.Time) (int, bool) {
	return s.rec.NewSince(filterOf(f), t)
}

// Capacity is how many request rows the recorder keeps, so the Traffic page can say
// what its tail is kept over.
func (s trafficSource) Capacity() int { return s.rec.Capacity() }

// Since is when the broker began, which is when the recorder did.
func (s trafficSource) Since() time.Time { return s.started }

func (s trafficSource) Window(d time.Duration) console.TrafficWindow {
	w := s.rec.Last(d)
	return console.TrafficWindow{Requests: w.Requests, Errors: w.Errors, From: w.From, To: w.To}
}

func (s trafficSource) Refusals() []console.TrafficRefusal {
	rs := s.rec.Refusals()
	out := make([]console.TrafficRefusal, len(rs))
	for i, r := range rs {
		out[i] = console.TrafficRefusal{Code: r.Code, Name: r.Name, Class: string(r.Class), Count: r.Count,
			First: r.First, Last: r.Last, Latest: r.Latest, API: r.API, Topics: r.Topics}
		for _, c := range r.Clients {
			out[i].Clients = append(out[i].Clients, console.TrafficClientCount{Client: c.Client, Count: c.Count})
		}
	}
	return out
}

func (s trafficSource) Clients() []console.TrafficClient {
	cs := s.rec.Clients()
	out := make([]console.TrafficClient, len(cs))
	for i, c := range cs {
		out[i] = console.TrafficClient{
			Conn: c.Conn, Remote: c.Remote, ClientID: c.ClientID, Software: c.Software, Principal: c.Principal,
			Opened: c.Opened, Last: c.Last, Requests: c.Requests, Groups: c.Groups, TxnID: c.TxnID,
			Produces: c.Produces, Fetches: c.Fetches, AskedTelemetry: c.AskedTelemetry,
			Req1m: c.Req1m, Req5m: c.Req5m, Err5m: c.Err5m, Streak: c.Streak, LastError: c.LastError,
			ClientStreak: c.ClientStreak, ClientLastError: c.ClientLastError, ClientLastClass: string(c.ClientLastClass),
			ClientReq1m: c.ClientReq1m, ClientReq5m: c.ClientReq5m, ClientErr5m: c.ClientErr5m,
		}
	}
	return out
}

func (s trafficSource) GroupEvents(group string, limit int) []console.TrafficGroupEvent {
	evs := s.rec.GroupEvents(group, limit)
	out := make([]console.TrafficGroupEvent, len(evs))
	for i, e := range evs {
		out[i] = console.TrafficGroupEvent{Seq: e.Seq, Time: e.Time, Kind: e.Kind, Member: e.Member, Generation: e.Generation,
			Detail: e.Detail, Members: e.Members, Cause: e.Cause}
	}
	return out
}

func (s trafficSource) Story(group string) console.TrafficStory {
	st := s.rec.Story(group)
	out := console.TrafficStory{Group: st.Group, Protocol: st.Protocol, Requests: st.Requests, Folded: st.Folded, Topics: st.Topics,
		Before: eventsOf(st.Before)}
	for _, rb := range st.Rebalances {
		out.Rebalances = append(out.Rebalances, console.TrafficRebalance{
			Start: rb.Start, End: rb.End, Done: rb.Done, From: rb.From, To: rb.To, Cause: rb.Cause,
			Members: rb.Members, MembersBefore: rb.MembersBefore, Rows: eventsOf(rb.Rows),
		})
	}
	for _, g := range st.Generations {
		out.Generations = append(out.Generations, console.TrafficGeneration{Gen: g.Gen, Cause: g.Cause, Members: g.Members, At: g.At})
	}
	return out
}

// clientMetricsSource gives the console what clients have reported about themselves.
type clientMetricsSource struct{ store *telemetry.Store }

// ClientMetrics is the console's view of the store, or nil when client telemetry is off.
func ClientMetrics(store *telemetry.Store) console.ClientMetricsSource {
	if store == nil {
		return nil
	}
	return clientMetricsSource{store}
}

func instanceOf(in telemetry.Instance) console.ClientInstance {
	ci := console.ClientInstance{
		ID: uuidString(in.ID), ClientID: in.ClientID, Software: in.Software, Principal: in.Principal, Host: in.Host,
		Conn: in.Conn, First: in.First, Last: in.Last, Pushes: in.Pushes, Terminated: in.Terminated, Resource: in.Resource,
		Metrics: in.Metrics, Dropped: in.Dropped, Skipped: in.Skipped,
	}
	for _, ser := range in.Series {
		ms := console.MetricSeries{Name: ser.Name, Attrs: ser.Attrs, Unit: ser.Unit, Kind: string(ser.Kind)}
		for _, sm := range ser.Samples {
			ms.Samples = append(ms.Samples, console.MetricSample{T: sm.T, V: sm.V})
		}
		ci.Series = append(ci.Series, ms)
	}
	return ci
}

func (s clientMetricsSource) Instances() []console.ClientInstance {
	ins := s.store.Instances()
	out := make([]console.ClientInstance, len(ins))
	for i, in := range ins {
		out[i] = instanceOf(in)
	}
	return out
}

// Summaries are the clients without their series, which is all the clients table needs
// to say who reports.
func (s clientMetricsSource) Summaries() []console.ClientInstance {
	ins := s.store.Summaries()
	out := make([]console.ClientInstance, len(ins))
	for i, in := range ins {
		out[i] = instanceOf(in)
	}
	return out
}

// ByID is one instance, with its series: the one the inspector asked for, and only that
// one is copied.
func (s clientMetricsSource) ByID(id string) (console.ClientInstance, bool) {
	raw, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	if err != nil || len(raw) != 16 {
		return console.ClientInstance{}, false
	}
	var key [16]byte
	copy(key[:], raw)
	in, ok := s.store.Instance(key)
	if !ok {
		return console.ClientInstance{}, false
	}
	return instanceOf(in), true
}

// uuidString writes a client instance id as a UUID: 8-4-4-4-12 hex digits.
func uuidString(id [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[0:4], id[4:6], id[6:8], id[8:10], id[10:16])
}
