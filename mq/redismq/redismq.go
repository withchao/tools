// Package redismq implements the message-queue contract (github.com/openimsdk/tools/mq)
// on top of Redis Streams. It is the multi-instance alternative to the in-process
// simmq (memory) implementation: simmq can only run inside a single process, while
// this package lets several instances share one queue through a Redis Stream
// consumer group.
//
// Design notes (why it looks the way it does):
//
//   - One Redis Stream per topic (no key-sharding in this version). The existing
//     consumers (e.g. msgtransfer's OnlineHistoryRedisConsumerHandler) only call
//     Mark/Commit on the LAST message of a batch and rely on offset-range commit
//     semantics. Redis XACK is per-entry, so we emulate offset semantics with a
//     monotonic watermark over a single ordered stream. Splitting a topic into
//     multiple shard-streams would break that watermark unless the callers are
//     changed, which is intentionally out of scope here.
//
//   - Exactly one active consumer per (stream, group) at a time, elected with a
//     Redis lease. This gives Kafka-with-one-partition semantics: strict per-key
//     order plus active/standby failover across instances. Horizontal scaling
//     within a single topic (true partitioning) is future work.
//
//   - At-least-once delivery. Messages stay in the group PEL until acked; a new
//     leader drains the dead leader's PEL via XAUTOCLAIM before reading new
//     messages, preserving stream order. A demoted leader is fenced: it stops
//     delivering its buffered messages, leaving them for the successor to reclaim.
//
//   - The leader bounds memory and metadata: it trims acked stream entries by MINID
//     (never touching unacked/undelivered entries) and prunes stale group consumer
//     names left by previous instances.
//
// Limitation — the lease is not a fencing token. Leadership is mutually exclusive in
// the common case, but during a renewal-failure window a zombie leader and a new
// leader can briefly both XREADGROUP `>`, splitting (and thus reordering) NEW
// messages for that window. Redis Streams offer no fencing on read/ack to prevent
// this. The renewal logic keeps the window small (it only steps down on a confirmed
// loss or after the lease could have expired), and ClaimMinIdle keeps reclaim from
// stealing in-flight work, but strict ordering under a partition cannot be fully
// guaranteed without a fencing token. For workloads needing that, use Kafka.
//
// Operational requirements — unlike Kafka, the broker here is a general-purpose
// Redis, so the deployment must guarantee:
//
//   - Redis >= 6.2: XAUTOCLAIM (failover reclaim) and XTRIM MINID (memory
//     bounding) do not exist before it. The Builder fail-fast checks this once,
//     because on an older server everything would appear to work until the first
//     failover, which would then silently strand the dead leader's messages.
//   - The Redis instance holding the queues (currently the shared cache Redis)
//     must run with maxmemory-policy noeviction: an evicted lease key means two
//     simultaneous leaders, an evicted stream key loses the whole backlog. It
//     should have AOF enabled (appendonly yes) — XADD is acknowledged before any
//     fsync, so an unclean Redis restart without AOF drops recently acknowledged
//     messages, unlike Kafka's acks=all. And it must never be FLUSHed.
//   - There is no retention backstop: entries are deleted only by the consumer
//     group leader's trim, so a topic whose consuming service is never deployed
//     grows without bound.
package redismq

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openimsdk/tools/mq"
	"github.com/redis/go-redis/v9"
)

// stream entry field names (kept short to save Redis memory).
const (
	fieldKey      = "k"
	fieldValue    = "v"
	fieldOpID     = "o"
	fieldOpUserID = "u"
	fieldPlatform = "p"
	fieldConnID   = "c"
)

// Config controls stream naming, delivery, ack and leader-election behaviour.
// The zero value is usable: fillDefault supplies sane defaults for every field.
type Config struct {
	// StreamPrefix is prepended to the topic to form the Redis Stream key.
	StreamPrefix string
	// MaxLen caps the stream length with an approximate MAXLEN on XADD. 0 = no cap.
	//
	// XACK only removes an entry from the consumer-group PEL; it never deletes the
	// stream entry, so a stream with no cap grows without bound (Redis is in-memory).
	// MAXLEN trims the OLDEST entries regardless of whether they have been acked, so
	// it must be sized well above the peak number of in-flight (unacked / un-trimmed)
	// entries — otherwise a trim can drop an entry that is still pending, and a
	// subsequent reclaim will not find the original message (message loss). For a
	// production deployment prefer a generous MaxLen, or a leader-side MINID trimmer
	// driven by the lowest pending id (not yet implemented here).
	MaxLen int64
	// ReadCount is the COUNT for XREADGROUP / XAUTOCLAIM.
	ReadCount int64
	// BlockTimeout is the BLOCK for XREADGROUP; it also bounds how often the
	// read loop re-checks for cancellation.
	BlockTimeout time.Duration
	// Buffer is the size of the in-process channel between the read loop and Subscribe.
	Buffer int
	// AutoCommit is how often marked (processed) entries are flushed with XACK,
	// emulating Kafka's auto-commit ticker for consumers that never call Commit.
	AutoCommit time.Duration
	// ClaimMinIdle is the minimum idle time before a pending entry may be reclaimed
	// by a new leader. It guards against stealing an entry still being processed by
	// the predecessor (a zombie/slow leader, or in-flight in an async downstream),
	// so it must exceed the worst-case downstream processing time. It is always
	// honoured, including on a graceful (Close) handoff.
	ClaimMinIdle time.Duration
	// ReclaimRetry is how long the startup reclaim waits before re-scanning, when
	// orphaned pending entries exist but are not yet idle enough to claim or when
	// a scan hit a transient Redis error.
	ReclaimRetry time.Duration
	// LeaseTTL is the lifetime of the leader lease.
	LeaseTTL time.Duration
	// RenewInterval is how often the leader lease is renewed / contested.
	RenewInterval time.Duration
	// MaintenanceInterval is how often the leader trims the stream and prunes stale
	// group consumers.
	MaintenanceInterval time.Duration
	// ConsumerIdleTimeout is how long a group consumer with no pending entries must
	// have been idle before the leader prunes it (it is a previous instance's leftover
	// name). Must be comfortably larger than any planned quiet period.
	ConsumerIdleTimeout time.Duration
}

func (c Config) fillDefault() Config {
	if c.ReadCount <= 0 {
		c.ReadCount = 64
	}
	if c.BlockTimeout <= 0 {
		c.BlockTimeout = 2 * time.Second
	}
	if c.Buffer <= 0 {
		c.Buffer = 256
	}
	if c.AutoCommit <= 0 {
		c.AutoCommit = time.Second
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 15 * time.Second
	}
	if c.RenewInterval <= 0 {
		c.RenewInterval = c.LeaseTTL / 3
	}
	if c.ClaimMinIdle <= 0 {
		// After a crash the new leader can only win once the old lease expired, so
		// its PEL is already idle ~LeaseTTL; requiring that much idle keeps a brief
		// zombie leader from having its in-flight messages stolen.
		c.ClaimMinIdle = c.LeaseTTL
	}
	if c.ReclaimRetry <= 0 {
		c.ReclaimRetry = 500 * time.Millisecond
	}
	if c.MaintenanceInterval <= 0 {
		c.MaintenanceInterval = 30 * time.Second
	}
	if c.ConsumerIdleTimeout <= 0 {
		c.ConsumerIdleTimeout = 30 * time.Minute
	}
	return c
}

// Builder produces topic Producers/Consumers backed by Redis Streams. It mirrors
// the shape of mqbuild.Builder so it can be wired in later without changing callers.
type Builder struct {
	cli          redis.UniversalClient
	cfg          Config
	topicGroupID map[string]string

	versionOnce sync.Once
	versionErr  error
}

// NewBuilder builds a Redis Streams MQ builder. cli is an externally owned client
// (e.g. from dbbuild.NewRedis); it is never closed by this package. topicGroupID
// maps each topic to its consumer-group id, exactly like the Kafka builder; it also
// serves as the set of known topics for both producers and consumers.
func NewBuilder(cli redis.UniversalClient, topicGroupID map[string]string, cfg Config) *Builder {
	return &Builder{cli: cli, cfg: cfg.fillDefault(), topicGroupID: topicGroupID}
}

func (b *Builder) streamName(topic string) string {
	return b.cfg.StreamPrefix + topic
}

// check guards every Get*: a usable client, a known topic (matching the kafka
// builder, which rejects unknown topics — instead of silently creating a stream
// nothing consumes or trims), and a compatible server version (checked once).
func (b *Builder) check(ctx context.Context, topic string) error {
	if b.cli == nil {
		return fmt.Errorf("redismq: nil redis client (is redis disabled?)")
	}
	if _, ok := b.topicGroupID[topic]; !ok {
		return fmt.Errorf("redismq: topic %s not found", topic)
	}
	b.versionOnce.Do(func() {
		b.versionErr = checkServerVersion(ctx, b.cli)
	})
	return b.versionErr
}

func (b *Builder) GetTopicProducer(ctx context.Context, topic string) (mq.Producer, error) {
	if err := b.check(ctx, topic); err != nil {
		return nil, err
	}
	return NewProducer(b.cli, b.streamName(topic), b.cfg.MaxLen), nil
}

func (b *Builder) GetTopicConsumer(ctx context.Context, topic string) (mq.Consumer, error) {
	if err := b.check(ctx, topic); err != nil {
		return nil, err
	}
	return NewConsumer(ctx, b.cli, b.streamName(topic), b.topicGroupID[topic], b.cfg)
}

// minRedisMajor.minRedisMinor is the oldest server this package works on:
// XAUTOCLAIM and XTRIM MINID both appeared in Redis 6.2.
const (
	minRedisMajor = 6
	minRedisMinor = 2
)

// checkServerVersion refuses to run against a Redis older than 6.2 (fail closed):
// on such a server every consumer would start fine and consume normally, but every
// leadership takeover would fail to reclaim the predecessor's pending entries,
// silently stranding them.
func checkServerVersion(ctx context.Context, cli redis.UniversalClient) error {
	info, err := cli.Info(ctx, "server").Result()
	if err != nil {
		return fmt.Errorf("redismq: read redis server info: %w", err)
	}
	return checkVersionInfo(info)
}

func checkVersionInfo(info string) error {
	version := ""
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:"); ok {
			version = v
			break
		}
	}
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return fmt.Errorf("redismq: cannot determine redis server version (queue engine redis needs >= %d.%d)", minRedisMajor, minRedisMinor)
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return fmt.Errorf("redismq: cannot parse redis server version %q (queue engine redis needs >= %d.%d)", version, minRedisMajor, minRedisMinor)
	}
	if major < minRedisMajor || (major == minRedisMajor && minor < minRedisMinor) {
		return fmt.Errorf("redismq: redis server %s is too old for queue engine redis, need >= %d.%d (XAUTOCLAIM, XTRIM MINID)", version, minRedisMajor, minRedisMinor)
	}
	return nil
}
