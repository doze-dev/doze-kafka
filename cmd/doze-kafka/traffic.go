package main

import (
	"fmt"

	"github.com/doze-dev/doze-kafka/console"
	"github.com/doze-dev/doze-kafka/internal/telemetry"
	"github.com/doze-dev/doze-kafka/internal/traffic"
)

// trafficSource gives the console what the broker's recorder holds, in the console's
// own types: the console is a client of the broker and does not import its internals,
// so the two are joined here, where both are known.
type trafficSource struct{ rec *traffic.Recorder }

// trafficFor is the console's view of the recorder, or nil when there is none (a
// server without a console keeps no record).
func trafficFor(rec *traffic.Recorder) console.TrafficSource {
	if rec == nil {
		return nil
	}
	return trafficSource{rec}
}

func (s trafficSource) Events(f console.TrafficFilter, limit int) []console.TrafficEvent {
	evs := s.rec.Events(traffic.Filter{Client: f.Client, API: f.API, Topic: f.Topic, Group: f.Group, ErrorsOnly: f.ErrorsOnly}, limit)
	out := make([]console.TrafficEvent, len(evs))
	for i, e := range evs {
		out[i] = console.TrafficEvent{
			Seq: e.Seq, First: e.First, Time: e.Time, Count: e.Count,
			Remote: e.Remote, ClientID: e.ClientID, Software: e.Software, Principal: e.Principal,
			API: e.API, Version: e.Version, Topics: e.Topics, Group: e.Group, Detail: e.Detail, Error: e.Error,
			Duration: e.Duration, ReqBytes: e.ReqBytes, RespBytes: e.RespBytes,
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
			Opened: c.Opened, Last: c.Last, Requests: c.Requests, Groups: c.Groups,
		}
	}
	return out
}

func (s trafficSource) GroupEvents(group string, limit int) []console.TrafficGroupEvent {
	evs := s.rec.GroupEvents(group, limit)
	out := make([]console.TrafficGroupEvent, len(evs))
	for i, e := range evs {
		out[i] = console.TrafficGroupEvent{Seq: e.Seq, Time: e.Time, Kind: e.Kind, Member: e.Member, Generation: e.Generation, Detail: e.Detail}
	}
	return out
}

// clientMetricsSource gives the console what clients have reported about themselves.
type clientMetricsSource struct{ store *telemetry.Store }

// clientMetricsFor is the console's view of the store, or nil when client telemetry is off.
func clientMetricsFor(store *telemetry.Store) console.ClientMetricsSource {
	if store == nil {
		return nil
	}
	return clientMetricsSource{store}
}

func (s clientMetricsSource) Instances() []console.ClientInstance {
	ins := s.store.Instances()
	out := make([]console.ClientInstance, len(ins))
	for i, in := range ins {
		ci := console.ClientInstance{
			ID: uuidString(in.ID), ClientID: in.ClientID, Software: in.Software, Principal: in.Principal, Host: in.Host,
			First: in.First, Last: in.Last, Pushes: in.Pushes, Terminated: in.Terminated, Resource: in.Resource,
		}
		for _, ser := range in.Series {
			ms := console.MetricSeries{Name: ser.Name, Attrs: ser.Attrs, Unit: ser.Unit, Kind: string(ser.Kind)}
			for _, sm := range ser.Samples {
				ms.Samples = append(ms.Samples, console.MetricSample{T: sm.T, V: sm.V})
			}
			ci.Series = append(ci.Series, ms)
		}
		out[i] = ci
	}
	return out
}

// uuidString writes a client instance id as a UUID: 8-4-4-4-12 hex digits.
func uuidString(id [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[0:4], id[4:6], id[6:8], id[8:10], id[10:16])
}
