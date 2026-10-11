package console

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// APIRange is one API the broker advertises and the versions it speaks.
type APIRange struct {
	Name     string
	Min, Max int16
}

// ClusterInfo is a snapshot of the broker's own identity and capabilities —
// deliberately modest. doze-kafka is single-node, so there's genuinely
// little to show here; padding it out with a dashboard would be dishonest.
type ClusterInfo struct {
	NodeID        int32
	ClusterID     string
	Host          string
	Port          int32
	SupportedAPIs []APIRange    // sorted by name
	BrokerConfig  []ConfigEntry // the broker's effective, read-only configuration

	// How this console's own connection is secured — facts the console knows
	// first-hand from its dial options, not read off the wire.
	AuthMech string // SASL mechanism in use, "" when the broker runs open
	TLS      bool

	// Transactions is every transaction that is open or being ended.
	Transactions []TxnRow
}

// TxnRow is one transaction in progress.
type TxnRow struct {
	ID         string
	State      string
	ProducerID int64
	Epoch      int16
	Began      time.Time // when it began; zero when the broker did not say.
	Timeout    string
	Partitions []string // topic-partition, as the transaction has written to them.
}

// ClusterInfo describes the broker itself, via DescribeCluster + ApiVersions
// — the same two calls a real admin client (kadm, kafka-broker-api-versions)
// would make.
func (b *backend) ClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	dreq := kmsg.NewPtrDescribeClusterRequest()
	dresp, err := dreq.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	if dresp.ErrorCode != 0 {
		return nil, kerr.ErrorForCode(dresp.ErrorCode)
	}
	info := &ClusterInfo{ClusterID: dresp.ClusterID, TLS: b.opts.TLSConfig != nil}
	if b.opts.SASLUser != "" {
		if info.AuthMech = b.opts.SASLMechanism; info.AuthMech == "" {
			info.AuthMech = "PLAIN"
		}
	}
	for _, br := range dresp.Brokers {
		if br.NodeID == dresp.ControllerID {
			info.NodeID, info.Host, info.Port = br.NodeID, br.Host, br.Port
			break
		}
	}
	if info.Host == "" && len(dresp.Brokers) > 0 {
		info.NodeID, info.Host, info.Port = dresp.Brokers[0].NodeID, dresp.Brokers[0].Host, dresp.Brokers[0].Port
	}

	vreq := kmsg.NewPtrApiVersionsRequest()
	vreq.ClientSoftwareName = "doze-kafka-console"
	vreq.ClientSoftwareVersion = "0"
	vresp, err := vreq.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	for _, k := range vresp.ApiKeys {
		// The name comes from the protocol library, not a table kept here: a
		// hand-kept list silently omitted every API added after it was written.
		info.SupportedAPIs = append(info.SupportedAPIs, APIRange{Name: kmsg.NameForKey(k.ApiKey), Min: k.MinVersion, Max: k.MaxVersion})
	}
	sort.Slice(info.SupportedAPIs, func(i, j int) bool { return info.SupportedAPIs[i].Name < info.SupportedAPIs[j].Name })

	// The broker's effective configuration, as `kafka-configs --describe --entity-type
	// brokers` shows it. Best effort: the page is still useful without it.
	creq := kmsg.NewPtrDescribeConfigsRequest()
	cr := kmsg.NewDescribeConfigsRequestResource()
	cr.ResourceType = kmsg.ConfigResourceTypeBroker
	cr.ResourceName = fmt.Sprint(info.NodeID)
	creq.Resources = []kmsg.DescribeConfigsRequestResource{cr}
	if cresp, err := creq.RequestWith(ctx, b.cl); err == nil && len(cresp.Resources) > 0 && cresp.Resources[0].ErrorCode == 0 {
		for _, c := range cresp.Resources[0].Configs {
			v := ""
			if c.Value != nil {
				v = *c.Value
			}
			info.BrokerConfig = append(info.BrokerConfig, ConfigEntry{Name: c.Name, Value: v})
		}
	}
	info.Transactions = b.openTransactions(ctx)
	return info, nil
}

// openTransactions lists the transactions in progress, as `kafka-transactions list`
// and `describe` would. Best effort, like the configuration: a broker that cannot say
// has none to show.
func (b *backend) openTransactions(ctx context.Context) []TxnRow {
	lreq := kmsg.NewPtrListTransactionsRequest()
	lreq.StateFilters = []string{"Ongoing", "PrepareCommit", "PrepareAbort"}
	lresp, err := lreq.RequestWith(ctx, b.cl)
	if err != nil || len(lresp.TransactionStates) == 0 {
		return nil
	}
	dreq := kmsg.NewPtrDescribeTransactionsRequest()
	for _, s := range lresp.TransactionStates {
		dreq.TransactionalIDs = append(dreq.TransactionalIDs, s.TransactionalID)
	}
	dresp, err := dreq.RequestWith(ctx, b.cl)
	if err != nil {
		return nil
	}
	var rows []TxnRow
	for _, s := range dresp.TransactionStates {
		if s.ErrorCode != 0 {
			continue
		}
		row := TxnRow{ID: s.TransactionalID, State: s.State, ProducerID: s.ProducerID, Epoch: s.ProducerEpoch,
			Timeout: (time.Duration(s.TimeoutMillis) * time.Millisecond).String()}
		if s.StartTimestamp > 0 {
			row.Began = time.UnixMilli(s.StartTimestamp)
		}
		for _, t := range s.Topics {
			for _, p := range t.Partitions {
				row.Partitions = append(row.Partitions, fmt.Sprintf("%s-%d", t.Topic, p))
			}
		}
		rows = append(rows, row)
	}
	return rows
}
