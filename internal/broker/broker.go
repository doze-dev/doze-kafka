// Package broker implements the Kafka request handlers, wiring the wire protocol
// to the storage engine (partition logs) and the metadata store. Handlers are
// deliberately thin: decode a request, call into log/meta, encode a response.
// See docs/api-support.md for the API set.
package broker

import (
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// Broker holds the shared state every handler needs.
type Broker struct {
	cfg     config.Config
	log     *slog.Logger
	logs    *log.Manager
	meta    meta.Store
	claims  *groupClaims // which protocol each live group id belongs to.
	groups  *groupManager
	cg848   *cg848Manager    // KIP-848 broker-driven groups (Kafka-4 profile).
	auth    *authenticator   // nil when SASL is disabled.
	idem    *idempotency     // idempotent-producer sequence dedup.
	profile protocol.Profile // version profile selected by cfg.Version.
	produce *notifier        // wakes long-polling fetchers when data is produced.

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
	return &Broker{
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
	}, nil
}

// Register binds every handler to its API key on the router. Which group-protocol
// handlers are registered depends on the active version profile.
func (b *Broker) Register(r *protocol.Router) {
	// The router enforces exactly what ApiVersions advertises: the profile's
	// ranges, plus SASL when it is configured.
	for _, a := range b.advertised() {
		r.Advertise(a.Key, a.Min, a.Max)
	}

	// Available on every supported Kafka major.
	r.Register(protocol.APIApiVersions, b.handleAPIVersions)
	r.Register(protocol.APIMetadata, b.handleMetadata)
	r.Register(protocol.APICreateTopics, b.handleCreateTopics)
	r.Register(protocol.APIDeleteTopics, b.handleDeleteTopics)
	r.Register(protocol.APIProduce, b.handleProduce)
	r.Register(protocol.APIFetch, b.handleFetch)
	r.Register(protocol.APIListOffsets, b.handleListOffsets)
	r.Register(protocol.APIInitProducerID, b.handleInitProducerID)

	// Classic consumer-group protocol (the JoinGroup/SyncGroup relay).
	r.Register(protocol.APIFindCoordinator, b.handleFindCoordinator)
	r.Register(protocol.APIJoinGroup, b.handleJoinGroup)
	r.Register(protocol.APISyncGroup, b.handleSyncGroup)
	r.Register(protocol.APIHeartbeat, b.handleHeartbeat)
	r.Register(protocol.APILeaveGroup, b.handleLeaveGroup)
	r.Register(protocol.APIOffsetCommit, b.handleOffsetCommit)
	r.Register(protocol.APIOffsetFetch, b.handleOffsetFetch)
	r.Register(protocol.APIListGroups, b.handleListGroups)
	r.Register(protocol.APIDeleteGroups, b.handleDeleteGroups)
	r.Register(protocol.APIOffsetDelete, b.handleOffsetDelete)
	r.Register(protocol.APIDescribeGroups, b.handleDescribeGroups)
	r.Register(protocol.APIDescribeConfigs, b.handleDescribeConfigs)
	r.Register(protocol.APIDescribeCluster, b.handleDescribeCluster)

	// SASL authentication — only when configured. The connection gate
	// (protocol.Server.RequireAuth) blocks every other API until the handshake
	// completes; these two handlers, and ApiVersions, are the pre-auth exceptions.
	if b.auth != nil {
		r.Register(protocol.APISASLHandshake, b.handleSASLHandshake)
		r.Register(protocol.APISASLAuthenticate, b.handleSASLAuthenticate)
	}

	// KIP-848 broker-driven group protocol — Kafka 4+ profiles only. A client at
	// the default group.protocol=consumer negotiates this via the advertised
	// ConsumerGroupHeartbeat/Describe APIs.
	if b.profile.GroupProtocol == protocol.GroupClassicAndConsumer {
		r.Register(protocol.APIConsumerGroupHeartbeat, b.handleConsumerGroupHeartbeat)
		r.Register(protocol.APIConsumerGroupDescribe, b.handleConsumerGroupDescribe)
	}
}

// storageFault logs an internal error that is about to be reported to a client as
// UNKNOWN_SERVER_ERROR and returns that code. The client sees only a code; without
// this, the cause of "my produce fails" existed nowhere a developer could look.
func (b *Broker) storageFault(op string, err error, attrs ...any) int16 {
	b.log.Error("request failed: "+op, append(attrs, "err", err)...)
	return int16(protocol.ErrUnknownServer)
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
	out := b.profile.Advertised()
	if b.auth != nil {
		// SaslHandshake v1 only: v0 means "raw tokens follow", which is not served.
		out = append(out,
			protocol.AdvertisedAPI{Key: protocol.APISASLHandshake, Min: 1, Max: 1},
			protocol.AdvertisedAPI{Key: protocol.APISASLAuthenticate, Min: 0, Max: 2})
	}
	return out
}
