// Command truth prints, as JSON, what a running broker holds: the seeded world the
// design gate (e2e/design) compares the console's numbers with. It asks the broker
// and the registry over their own protocols, as any client would, and never the
// console, so a number the console gets wrong cannot agree with itself here.
//
//	go run ./e2e/design/truth 127.0.0.1:14192 http://127.0.0.1:14194
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Topic is one topic: its partitions and the records it holds (log end minus log
// start, summed over its partitions).
type Topic struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	Messages   int64  `json:"messages"`
	Compact    bool   `json:"compact"`
	// RetentionMs is the topic's retention.ms as the broker reports it.
	RetentionMs int64 `json:"retentionMs"`
	// Ends are each partition's log end offset, by partition.
	Ends []int64 `json:"ends"`
	// Starts are each partition's log start offset, by partition.
	Starts []int64 `json:"starts"`
}

// Group is one consumer group as ListGroups and DescribeGroups give it, with its
// lag: log end minus committed, summed over what it committed.
type Group struct {
	Name     string   `json:"name"`
	State    string   `json:"state"`
	Protocol string   `json:"protocol"`
	Members  int      `json:"members"`
	Topics   []string `json:"topics"`
	Lag      int64    `json:"lag"`
}

// Subject is one registry subject.
type Subject struct {
	Name     string `json:"name"`
	Versions int    `json:"versions"`
	LatestID int    `json:"latestId"`
	Type     string `json:"type"`
}

// ACL is one stored ACL.
type ACL struct {
	Principal, ResourceType, Name, Pattern, Operation, Permission, Host string
}

// World is everything printed.
type World struct {
	Topics   []Topic   `json:"topics"`
	Groups   []Group   `json:"groups"`
	Subjects []Subject `json:"subjects"`
	ACLs     []ACL     `json:"acls"`
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: truth host:port registry-url")
		os.Exit(2)
	}
	w, err := read(os.Args[1], strings.TrimRight(os.Args[2], "/"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "truth:", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")
	_ = enc.Encode(w)
}

func read(addr, reg string) (*World, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID("doze-design-truth"))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	w := &World{}

	meta := kmsg.NewPtrMetadataRequest()
	mresp, err := meta.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	ends := map[string][]int64{}
	starts := map[string][]int64{}
	for _, t := range mresp.Topics {
		if t.Topic == nil || t.IsInternal {
			continue
		}
		n := len(t.Partitions)
		ends[*t.Topic], starts[*t.Topic] = make([]int64, n), make([]int64, n)
		w.Topics = append(w.Topics, Topic{Name: *t.Topic, Partitions: n})
	}
	sort.Slice(w.Topics, func(i, j int) bool { return w.Topics[i].Name < w.Topics[j].Name })
	for _, ts := range []int64{-1, -2} {
		lo := kmsg.NewPtrListOffsetsRequest()
		for _, t := range w.Topics {
			rt := kmsg.NewListOffsetsRequestTopic()
			rt.Topic = t.Name
			for p := 0; p < t.Partitions; p++ {
				rp := kmsg.NewListOffsetsRequestTopicPartition()
				rp.Partition, rp.Timestamp = int32(p), ts
				rt.Partitions = append(rt.Partitions, rp)
			}
			lo.Topics = append(lo.Topics, rt)
		}
		lresp, err := lo.RequestWith(ctx, cl)
		if err != nil {
			return nil, err
		}
		for _, t := range lresp.Topics {
			for _, p := range t.Partitions {
				if ts == -1 {
					ends[t.Topic][p.Partition] = p.Offset
				} else {
					starts[t.Topic][p.Partition] = p.Offset
				}
			}
		}
	}
	dc := kmsg.NewPtrDescribeConfigsRequest()
	for i := range w.Topics {
		t := &w.Topics[i]
		t.Ends, t.Starts = ends[t.Name], starts[t.Name]
		for p := range t.Ends {
			t.Messages += t.Ends[p] - t.Starts[p]
		}
		r := kmsg.NewDescribeConfigsRequestResource()
		r.ResourceType, r.ResourceName = kmsg.ConfigResourceTypeTopic, t.Name
		dc.Resources = append(dc.Resources, r)
	}
	dresp, err := dc.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	for _, r := range dresp.Resources {
		for i := range w.Topics {
			if w.Topics[i].Name != r.ResourceName {
				continue
			}
			for _, c := range r.Configs {
				if c.Value == nil {
					continue
				}
				switch c.Name {
				case "cleanup.policy":
					w.Topics[i].Compact = strings.Contains(*c.Value, "compact")
				case "retention.ms":
					w.Topics[i].RetentionMs, _ = strconv.ParseInt(*c.Value, 10, 64)
				}
			}
		}
	}

	lg := kmsg.NewPtrListGroupsRequest()
	lgresp, err := lg.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	dg := kmsg.NewPtrDescribeGroupsRequest()
	for _, g := range lgresp.Groups {
		dg.Groups = append(dg.Groups, g.Group)
	}
	sort.Strings(dg.Groups)
	dgresp, err := dg.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	for _, g := range dgresp.Groups {
		grp := Group{Name: g.Group, State: g.State, Protocol: g.ProtocolType, Members: len(g.Members)}
		of := kmsg.NewPtrOffsetFetchRequest()
		of.Group = g.Group
		ofresp, err := of.RequestWith(ctx, cl)
		if err != nil {
			return nil, err
		}
		if e := kerr.ErrorForCode(ofresp.ErrorCode); e != nil {
			return nil, fmt.Errorf("offsets of %s: %w", g.Group, e)
		}
		seen := map[string]bool{}
		for _, t := range ofresp.Topics {
			for _, p := range t.Partitions {
				if p.Offset < 0 {
					continue
				}
				if !seen[t.Topic] {
					seen[t.Topic] = true
					grp.Topics = append(grp.Topics, t.Topic)
				}
				if e := ends[t.Topic]; int(p.Partition) < len(e) && e[p.Partition] > p.Offset {
					grp.Lag += e[p.Partition] - p.Offset
				}
			}
		}
		sort.Strings(grp.Topics)
		w.Groups = append(w.Groups, grp)
	}

	da := kmsg.NewPtrDescribeACLsRequest()
	da.ResourceType = kmsg.ACLResourceTypeAny
	da.ResourcePatternType = kmsg.ACLResourcePatternTypeAny
	da.Operation = kmsg.ACLOperationAny
	da.PermissionType = kmsg.ACLPermissionTypeAny
	daresp, err := da.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	for _, r := range daresp.Resources {
		for _, a := range r.ACLs {
			w.ACLs = append(w.ACLs, ACL{Principal: a.Principal, ResourceType: r.ResourceType.String(), Name: r.ResourceName,
				Pattern: r.ResourcePatternType.String(), Operation: a.Operation.String(), Permission: a.PermissionType.String(), Host: a.Host})
		}
	}

	var names []string
	if err := get(ctx, reg+"/subjects", &names); err != nil {
		return nil, err
	}
	sort.Strings(names)
	for _, n := range names {
		var versions []int
		if err := get(ctx, reg+"/subjects/"+n+"/versions", &versions); err != nil {
			return nil, err
		}
		var latest struct {
			ID         int    `json:"id"`
			SchemaType string `json:"schemaType"`
		}
		if err := get(ctx, reg+"/subjects/"+n+"/versions/latest", &latest); err != nil {
			return nil, err
		}
		if latest.SchemaType == "" {
			latest.SchemaType = "AVRO"
		}
		w.Subjects = append(w.Subjects, Subject{Name: n, Versions: len(versions), LatestID: latest.ID, Type: latest.SchemaType})
	}
	return w, nil
}

func get(ctx context.Context, url string, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(into)
}
