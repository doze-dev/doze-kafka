package console

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// clusterViews are the pages of the Cluster section the console can draw from what
// the broker reports over the wire; Limits, Storage, Compaction, the zone's names and
// Listeners (Cl-03, Cl-04, Cl-07 to Cl-09) need facts the broker does not hand the
// console and belong to the cluster stage.
var clusterViews = []string{"overview", "config", "apis", "transactions"}

// cluster is Cl-Home: one broker, what it is, what it holds, and the pages under it.
// upText is how long the broker has been up as Cl-Home's sentence writes it: "2 h 14 m",
// "25 m 31 s", "40 s".
func upText(d time.Duration) string {
	s := int64(d / time.Second)
	switch {
	case s < 60:
		return fmt.Sprintf("%d s", s)
	case s < 3600:
		return fmt.Sprintf("%d m %d s", s/60, s%60)
	case s < 172800:
		return fmt.Sprintf("%d h %d m", s/3600, s/60%60)
	}
	return fmt.Sprintf("%d d %d h", s/86400, s/3600%24)
}

func (c *Console) cluster(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	info, err := c.be.ClusterInfo(ctx)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	view := r.URL.Query().Get("view")
	ok := false
	for _, v := range clusterViews {
		ok = ok || v == view
	}
	if !ok {
		view = "overview"
	}

	var topics []TopicSummary
	var partitions int
	topics, _ = c.be.ListTopics(ctx)
	for _, t := range topics {
		partitions += int(t.Partitions)
	}
	groups, _ := c.be.ListGroups(ctx)
	// Cl-Home's "Compacted partitions": the partitions of the topics whose
	// cleanup.policy asks for compaction, as the broker reports each topic's
	// configuration (the background cleaner's own status has no source: CLU-24).
	compacted := 0
	if view == "overview" && len(topics) > 0 {
		names := make([]string, len(topics))
		for i, t := range topics {
			names[i] = t.Name
		}
		cfgs := c.be.TopicConfigs(ctx, names)
		for _, t := range topics {
			if strings.Contains(cfgs[t.Name]["cleanup.policy"], "compact") {
				compacted += int(t.Partitions)
			}
		}
	}

	views := []struct {
		Key, Label, URL, Count string
		On, Warn               bool
	}{
		{"overview", "Overview", c.prefix + "/cluster", "", view == "overview", false},
		{"config", "Configuration", c.prefix + "/cluster?view=config", fmt.Sprint(len(info.BrokerConfig)), view == "config", false},
		{"apis", "Supported APIs", c.prefix + "/cluster?view=apis", fmt.Sprint(len(info.SupportedAPIs)), view == "apis", false},
		{"transactions", "Transactions", c.prefix + "/cluster?view=transactions", "", view == "transactions", false},
	}
	if n := len(info.Transactions); n > 0 {
		views[3].Count, views[3].Warn = fmt.Sprintf("%d open", n), true
	}

	data := map[string]any{
		"Cluster": info, "Views": views, "View": "cluster", "ClusterView": view, "Page": "cluster", "Title": "Cluster",
		"TopicCount": len(topics), "PartitionCount": partitions, "GroupCount": len(groups), "CompactedPartitions": compacted,
		"Facts": c.connectFacts(r),
	}
	if !c.facts.Started.IsZero() {
		data["UpFor"] = upText(time.Since(c.facts.Started))
	}
	data["Version"] = c.facts.Version
	if c.traffic != nil {
		data["OpenConnections"], data["HasTraffic"] = len(c.traffic.Clients()), true
	}
	if m := c.facts.KafkaMajor; m > 0 {
		data["Declares"] = fmt.Sprintf("Kafka %d.0", m)
	}
	data["DataDirText"] = strings.TrimSpace(c.facts.DataDir)
	// What the data directory takes on disk, and its metadata database, read from the
	// files (the console is in the broker's process; DescribeLogDirs is not served).
	if total, meta, ok := c.dataDirSize(); ok {
		data["DataDirSize"] = diskSize(total)
		if meta >= 0 {
			data["MetaSize"] = diskSize(meta)
		}
	}
	c.render(w, r, "cluster_detail", data)
}
