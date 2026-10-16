// Package broker implements the Kafka request handlers, wiring the wire protocol
// to the storage engine (partition logs) and the metadata store. Handlers are
// deliberately thin: decode a request, call into log/meta, encode a response.
// See docs/api-support.md for the API set.
package broker

import (
	"errors"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"context"
	"fmt"
	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
	"github.com/doze-dev/doze-kafka/internal/telemetry"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Broker holds the shared state every handler needs.
type Broker struct {
	alterMu sync.Mutex  // one topic-config change at a time (see alterTopic).
	acls    aclState    // the stored ACLs' writers (acls.go).
	authz   *authorizer // enforces them (authorizer.go).
	txns    *txnManager // transactional producers (txn.go).
	cfg     config.Config
	log     *slog.Logger
	logs    *log.Manager
	meta    meta.Store
	claims  *groupClaims  // which protocol each live group id belongs to.
	expiry  *offsetExpiry // applies the offsets retention; nil when it is off.
	groups  *groupManager
	cg848   *cg848Manager    // KIP-848 broker-driven groups (Kafka-4 profile).
	auth    *authenticator   // nil when SASL is disabled.
	idem    *idempotency     // idempotent-producer sequence dedup.
	profile protocol.Profile // version profile selected by cfg.Version.
	produce *notifier        // wakes long-polling fetchers when data is produced.

	// telemetry holds what clients report about themselves (KIP-714); nil when
	// --client-telemetry is off.
	telemetry *telemetry.Store

	// boundPort is the port the TCP listener actually bound, learned after Bind.
	// It is what Metadata must advertise when the configured address names no
	// fixed port (":0"), and 0 until a listener is bound (or when serving on a
	// caller-provided non-TCP listener, where the configured address stands).
	boundPort atomic.Int32

	pidMu   sync.Mutex
	pidNext int64 // next producer id to hand out from the reserved block.
	pidEnd  int64 // one past the last id in the block.
}

// New constructs a Broker over an open log manager and metadata store. The
// version profile is resolved from cfg.Version (already validated by
// config.Validate); an out-of-range value falls back to the default major
// defensively rather than panicking. An error is returned only if the SASL
// credential set fails to initialize.
func New(cfg config.Config, logger *slog.Logger, logs *log.Manager, m meta.Store) (*Broker, error) {
	profile, ok := protocol.ProfileFor(cfg.Version)
	if !ok {
		profile, _ = protocol.ProfileFor(protocol.DefaultKafkaMajor)
	}
	auth, err := newAuthenticator(cfg)
	if err != nil {
		return nil, err
	}
	claims := newGroupClaims()
	acls, err := m.ListACLs()
	if err != nil {
		return nil, fmt.Errorf("broker: read the stored ACLs: %w", err)
	}
	txnRecs, err := m.ListTxns()
	if err != nil {
		return nil, fmt.Errorf("broker: read the stored transactions: %w", err)
	}
	b := &Broker{
		authz:  newAuthorizer(cfg, acls),
		txns:   newTxnManager(txnRecs),
		cfg:    cfg,
		log:    logger,
		logs:   logs,
		meta:   m,
		claims: claims,
		groups: newGroupManager(func(group string) bool {
			entries, err := m.ListOffsets(group)
			return err == nil && len(entries) > 0
		}, claims),
		cg848:   newCG848Manager(m, logger, cfg.ConsumerSessionTimeout, claims),
		auth:    auth,
		idem:    newIdempotency(),
		profile: profile,
		produce: newNotifier(),
	}
	// Only a broker with a console keeps what clients report: the console is its one
	// reader, and a broker without one advertises nothing for clients to push into.
	b.cg848.bridge = b.groups
	if cfg.OffsetsRetention > 0 {
		exp, err := newOffsetExpiry(b, cfg.OffsetsRetention)
		if err != nil {
			return nil, fmt.Errorf("broker: read when groups became empty: %w", err)
		}
		b.expiry = exp
		b.groups.emptied, b.groups.activated = exp.markEmpty, exp.markActive
		b.cg848.emptied, b.cg848.activated = exp.markEmpty, exp.markActive
		exp.start()
	}
	if cfg.ClientTelemetry && cfg.Console {
		b.telemetry = telemetry.NewStore()
		b.telemetry.SetPushInterval(telemetryPushMillis * time.Millisecond)
	}
	return b, nil
}

// Register binds a handler to every API the active profile offers, and tells the
// router to refuse everything else. What is offered comes from protocol.APIs; what
// serves it comes from handlers() below. An offered API with no handler is a
// programming error and panics here, at start-up, rather than surfacing as a
// client whose request is never answered.
func (b *Broker) Register(r *protocol.Router) {
	handlers := b.handlers()
	for _, a := range b.advertised() {
		h, ok := handlers[a.Key]
		if !ok {
			panic(fmt.Sprintf("broker: %s is advertised (protocol.APIs) but has no handler", protocol.APIName(a.Key)))
		}
		r.Advertise(a.Key, a.Min, a.Max)
		r.Register(a.Key, h)
	}
}

// handlers is the handler for every API in protocol.APIs. Most are written
// against their decoded request and a response to fill in (see typed); Produce,
// Fetch and ApiVersions frame their own, for the reasons given where they are
// defined.
func (b *Broker) handlers() map[protocol.APIKey]protocol.Handler {
	return map[protocol.APIKey]protocol.Handler{
		kmsg.ApiVersions: b.handleAPIVersions,
		kmsg.Produce:     b.handleProduce,
		kmsg.Fetch:       b.handleFetch,

		kmsg.GetTelemetrySubscriptions: typed(b.handleGetTelemetrySubscriptions),
		kmsg.PushTelemetry:             typed(b.handlePushTelemetry),

		kmsg.Metadata:                typed(b.handleMetadata),
		kmsg.CreateTopics:            typed(b.handleCreateTopics),
		kmsg.DeleteTopics:            typed(b.handleDeleteTopics),
		kmsg.CreatePartitions:        typed(b.handleCreatePartitions),
		kmsg.DeleteRecords:           typed(b.handleDeleteRecords),
		kmsg.DescribeACLs:            typed(b.handleDescribeACLs),
		kmsg.AddPartitionsToTxn:      typed(b.handleAddPartitionsToTxn),
		kmsg.AddOffsetsToTxn:         typed(b.handleAddOffsetsToTxn),
		kmsg.EndTxn:                  typed(b.handleEndTxn),
		kmsg.TxnOffsetCommit:         typed(b.handleTxnOffsetCommit),
		kmsg.DescribeTransactions:    typed(b.handleDescribeTransactions),
		kmsg.ListTransactions:        typed(b.handleListTransactions),
		kmsg.DescribeProducers:       typed(b.handleDescribeProducers),
		kmsg.CreateACLs:              typed(b.handleCreateACLs),
		kmsg.DeleteACLs:              typed(b.handleDeleteACLs),
		kmsg.ListOffsets:             typed(b.handleListOffsets),
		kmsg.InitProducerID:          typed(b.handleInitProducerID),
		kmsg.DescribeConfigs:         typed(b.handleDescribeConfigs),
		kmsg.AlterConfigs:            typed(b.handleAlterConfigs),
		kmsg.IncrementalAlterConfigs: typed(b.handleIncrementalAlterConfigs),
		kmsg.DescribeCluster:         typed(b.handleDescribeCluster),

		// The classic consumer-group protocol, and what both protocols share.
		kmsg.FindCoordinator: typed(b.handleFindCoordinator),
		kmsg.JoinGroup:       typed(b.handleJoinGroup),
		kmsg.SyncGroup:       typed(b.handleSyncGroup),
		kmsg.Heartbeat:       typed(b.handleHeartbeat),
		kmsg.LeaveGroup:      typed(b.handleLeaveGroup),
		kmsg.OffsetCommit:    typed(b.handleOffsetCommit),
		kmsg.OffsetFetch:     typed(b.handleOffsetFetch),
		kmsg.ListGroups:      typed(b.handleListGroups),
		kmsg.DescribeGroups:  typed(b.handleDescribeGroups),
		kmsg.DeleteGroups:    typed(b.handleDeleteGroups),
		kmsg.OffsetDelete:    typed(b.handleOffsetDelete),

		// KIP-848, offered by the Kafka 4 profile only.
		kmsg.ConsumerGroupHeartbeat: typed(b.handleConsumerGroupHeartbeat),
		kmsg.ConsumerGroupDescribe:  typed(b.handleConsumerGroupDescribe),

		// SASL, offered only when authentication is configured. The connection gate
		// (protocol.Server.RequireAuth) blocks every other API until the handshake
		// completes; these two, and ApiVersions, are the pre-auth exceptions.
		kmsg.SASLHandshake:    typed(b.handleSASLHandshake),
		kmsg.SASLAuthenticate: typed(b.handleSASLAuthenticate),
	}
}

// typed adapts a handler written against its own request and response types to
// the router's: it decodes the request, makes the matching response at the
// request's version, calls the handler to fill it in, and frames the result. That
// is the preamble and the last line every handler used to repeat.
func typed[Q kmsg.Request, R kmsg.Response](fn func(ctx context.Context, req *protocol.Request, q Q, resp R) error) protocol.Handler {
	return func(ctx context.Context, req *protocol.Request) ([]byte, error) {
		kreq, err := req.Decode()
		if err != nil {
			return nil, err
		}
		q, ok := kreq.(Q)
		if !ok {
			return nil, fmt.Errorf("broker: %s decoded as %T", protocol.APIName(req.Header.APIKey), kreq)
		}
		resp, ok := q.ResponseKind().(R)
		if !ok {
			return nil, fmt.Errorf("broker: %s answers with %T", protocol.APIName(req.Header.APIKey), q.ResponseKind())
		}
		if err := fn(ctx, req, q, resp); err != nil {
			return nil, err
		}
		return req.Reply(resp), nil
	}
}

// storageFault logs an internal error that is about to be reported to a client
// and returns the code to report. The client sees only a code; without this, the
// cause of "my produce fails" existed nowhere a developer could look.
//
// An I/O failure (a full disk, a read-only remount, a failing device) is
// KAFKA_STORAGE_ERROR, as Apache Kafka answers when a log directory fails: a
// retriable code that says the broker's storage is the problem. Anything else is
// UNKNOWN_SERVER_ERROR (claim: bad-days Q2a).
func (b *Broker) storageFault(op string, err error, attrs ...any) int16 {
	b.log.Error("request failed: "+op, append(attrs, "err", err)...)
	if isIOError(err) {
		return kerr.KafkaStorageError.Code
	}
	return kerr.UnknownServerError.Code
}

// storageFaultError is storageFault for the handlers that answer with a *kerr.Error.
func (b *Broker) storageFaultError(op string, err error, attrs ...any) *kerr.Error {
	if b.storageFault(op, err, attrs...) == kerr.KafkaStorageError.Code {
		return kerr.KafkaStorageError
	}
	return kerr.UnknownServerError
}

// isIOError reports whether err came from the file system rather than from the
// broker's own logic.
func isIOError(err error) bool {
	var pe *fs.PathError
	var le *os.LinkError
	var se *os.SyscallError
	var en syscall.Errno
	return errors.Is(err, log.ErrStorageOffline) || errors.As(err, &pe) || errors.As(err, &le) || errors.As(err, &se) || errors.As(err, &en)
}

// nodeID is this broker's node id. Single-node, so it is a constant.
func (b *Broker) nodeID() int32 { return 1 }

// clusterID is the (stable, cosmetic) cluster id reported in Metadata.
func (b *Broker) clusterID() string { return "doze-kafka" }

// SetBoundAddr tells the broker which address its listener actually bound, so
// Metadata advertises the real port rather than the configured one. A listen
// address of ":0" is the case this exists for: the configured string says
// nothing, and without this clients are told 9092 and never connect.
func (b *Broker) SetBoundAddr(a net.Addr) {
	if t, ok := a.(*net.TCPAddr); ok && t.Port > 0 {
		b.boundPort.Store(int32(t.Port))
	}
}

// advertisedPort is the port returned to clients in Metadata: the configured
// override if set, else the port the listener bound, else the port from the
// listen address, else 9092.
func (b *Broker) advertisedPort() int32 {
	if b.cfg.AdvertisedPort > 0 {
		return int32(b.cfg.AdvertisedPort)
	}
	if p := b.boundPort.Load(); p > 0 {
		return p
	}
	if _, portStr, err := net.SplitHostPort(b.cfg.ListenAddr); err == nil {
		if p, err := strconv.Atoi(portStr); err == nil && p > 0 {
			return int32(p)
		}
	}
	return 9092
}

// resolveTopic looks up a topic by name, or by UUID when the name is empty (the
// topic-ID addressing used by modern Fetch/Produce). It returns the resolved name,
// the topic record, and whether it was found.
func (b *Broker) resolveTopic(name string, id [16]byte) (string, meta.Topic, bool) {
	if name != "" {
		t, ok, err := b.meta.Topic(name)
		if err != nil || !ok {
			return name, meta.Topic{}, false
		}
		return name, t, true
	}
	t, ok, err := b.meta.TopicByID(meta.TopicID(id))
	if err != nil || !ok {
		return "", meta.Topic{}, false
	}
	return t.Name, t, true
}

// TopicName is the name of the topic with this id, or "" if there is none now.
func (b *Broker) TopicName(id [16]byte) string {
	name, _, _ := b.resolveTopic("", id)
	return name
}

// notifier is a broadcast wakeup: signal() wakes everyone currently waiting on
// wait(). It backs Fetch long-polling — any Produce wakes blocked fetchers, which
// then re-check their partitions.
// notifier wakes long-polling fetchers when data arrives. It is per topic: a
// single global signal woke every waiting fetch on every produce, and each woke
// to re-read all its partitions only to find nothing new. A fetch subscribes to
// the topics it reads BEFORE reading them, so a produce landing between the read
// and the wait is not missed — the signal is buffered in the subscription.
type notifier struct {
	mu   sync.Mutex
	subs map[string]map[*subscription]struct{}
}

type subscription struct {
	// C receives at most one pending signal; a signal while one is already pending
	// is dropped, since one wakeup means "re-read".
	C      chan struct{}
	n      *notifier
	topics []string
}

func newNotifier() *notifier { return &notifier{subs: map[string]map[*subscription]struct{}{}} }

// subscribe registers interest in the given topics. Close it when done.
func (n *notifier) subscribe(topics []string) *subscription {
	s := &subscription{C: make(chan struct{}, 1), n: n, topics: topics}
	n.mu.Lock()
	for _, t := range topics {
		if n.subs[t] == nil {
			n.subs[t] = map[*subscription]struct{}{}
		}
		n.subs[t][s] = struct{}{}
	}
	n.mu.Unlock()
	return s
}

func (s *subscription) Close() {
	s.n.mu.Lock()
	for _, t := range s.topics {
		delete(s.n.subs[t], s)
		if len(s.n.subs[t]) == 0 {
			delete(s.n.subs, t)
		}
	}
	s.n.mu.Unlock()
}

// signal wakes every fetch subscribed to topic.
func (n *notifier) signal(topic string) {
	n.mu.Lock()
	for s := range n.subs[topic] {
		select {
		case s.C <- struct{}{}:
		default:
		}
	}
	n.mu.Unlock()
}

// advertised is the full ApiVersions table: the profile, plus the SASL APIs when
// auth is configured. ApiVersions and the router both read it, so what is offered
// and what is accepted cannot differ.
func (b *Broker) advertised() []protocol.AdvertisedAPI {
	return b.profile.Advertised(b.auth != nil, b.telemetry != nil)
}

// Stop ends the coordinators' timers. The server calls it before closing storage:
// a member's session timer firing afterwards would run an assignment against a
// closed metadata store.
func (b *Broker) Stop() {
	if b.expiry != nil {
		b.expiry.stopAndFlush()
	}
	b.groups.stop()
	b.cg848.stop()
}

// errNone is the error code of a response with nothing wrong. Every other code is
// the protocol library's (kerr), which has no name for zero.
const errNone int16 = 0
