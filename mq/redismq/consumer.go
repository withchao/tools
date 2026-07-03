package redismq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mq"
	"github.com/redis/go-redis/v9"
)

// ErrConsumerClosed is returned by Subscribe after the consumer has been closed.
var ErrConsumerClosed = errors.New("redismq: consumer closed")

// leaseScript atomically acquires or renews the leader lease: it sets the key to
// our name with a fresh TTL iff the key is unset or already ours. Returns 1 if we
// hold the lease afterwards, 0 otherwise.
var leaseScript = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if v == false or v == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0`)

// releaseScript releases the lease on a graceful shutdown iff we still hold it, so
// a standby can win leadership immediately instead of waiting for the lease to
// expire. It touches a single key, which keeps it valid under Redis Cluster.
// KEYS[1]=lease; ARGV[1]=name.
var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0`)

// deliveryTerm isolates one leadership term: its own delivery channel and its own
// acker. When a term ends (demotion/close) the term is abandoned — its
// buffered-but-undelivered messages are dropped (they stay unacked in the PEL and
// the next leader reclaims them) and its acker is discarded, so neither stale
// buffered work nor the ack watermark leaks across leadership changes.
type deliveryTerm struct {
	ch  chan *message
	ack *acker
}

// Consumer reads one Redis Stream as a member of a consumer group. At most one
// Consumer per (stream, group) is active at a time (leader lease); the rest stand
// by and take over on failure.
type Consumer struct {
	cli    redis.UniversalClient
	stream string
	group  string
	name   string // unique per Consumer instance, used as the Redis consumer name
	cfg    Config

	mu     sync.Mutex
	term   *deliveryTerm // current leadership term, nil when standby
	wakeCh chan struct{} // closed+replaced whenever term changes, to wake Subscribe

	closeCtx context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewConsumer creates the consumer group (if absent) and starts the leader-election
// manager. The returned Consumer satisfies mq.Consumer.
func NewConsumer(ctx context.Context, cli redis.UniversalClient, stream, group string, cfg Config) (mq.Consumer, error) {
	cfg = cfg.fillDefault()
	// "$": a brand-new group skips pre-existing history (like Kafka OffsetNewest);
	// an already-existing group is left untouched (BUSYGROUP) and resumes where it
	// left off, so backlog accumulated while every instance was down is still served.
	if err := cli.XGroupCreateMkStream(ctx, stream, group, "$").Err(); err != nil &&
		!strings.Contains(strings.ToUpper(err.Error()), "BUSYGROUP") {
		return nil, fmt.Errorf("redismq: create group %s on %s: %w", group, stream, err)
	}
	c := &Consumer{
		cli:    cli,
		stream: stream,
		group:  group,
		name:   consumerName(),
		cfg:    cfg,
		wakeCh: make(chan struct{}),
	}
	// Derive the lifecycle from the caller's ctx (matching the Kafka consumer): if
	// the parent ctx is cancelled the background loops stop, in addition to Close().
	c.closeCtx, c.cancel = context.WithCancel(ctx)
	c.wg.Add(1)
	go c.manage()
	return c, nil
}

func consumerName() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), uuid.New().String()[:8])
}

func (c *Consumer) leaseKey() string {
	return "mq:lease:" + c.group + ":" + c.stream
}

// beginTerm installs a fresh delivery term and wakes any waiting Subscribe.
func (c *Consumer) beginTerm() *deliveryTerm {
	term := &deliveryTerm{
		ch:  make(chan *message, c.cfg.Buffer),
		ack: &acker{cli: c.cli, stream: c.stream, group: c.group},
	}
	c.mu.Lock()
	c.term = term
	c.signalLocked()
	c.mu.Unlock()
	return term
}

// endTerm clears the term (if it is still current) and wakes Subscribe so it stops
// delivering from the abandoned channel.
func (c *Consumer) endTerm(term *deliveryTerm) {
	c.mu.Lock()
	if c.term == term {
		c.term = nil
		c.signalLocked()
	}
	c.mu.Unlock()
}

func (c *Consumer) signalLocked() {
	close(c.wakeCh)
	c.wakeCh = make(chan struct{})
}

func (c *Consumer) currentTerm() (*deliveryTerm, <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.term, c.wakeCh
}

func (c *Consumer) isCurrentTerm(term *deliveryTerm) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.term == term
}

// Subscribe blocks until one message is available, then invokes fn with it. A
// standby (non-leader) consumer blocks here until it wins leadership or is closed.
// On a leadership change it re-evaluates rather than draining a stale term's buffer,
// so a demoted leader does not keep processing messages the new leader now owns.
// Callers are expected to loop on Subscribe.
func (c *Consumer) Subscribe(ctx context.Context, fn mq.Handler) error {
	for {
		term, wake := c.currentTerm()
		if term == nil {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-c.closeCtx.Done():
				return ErrConsumerClosed
			case <-wake: // a term may have begun
				continue
			}
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-c.closeCtx.Done():
			return ErrConsumerClosed
		case <-wake: // term changed; re-evaluate without delivering the old buffer
			continue
		case msg, ok := <-term.ch:
			if !ok {
				continue
			}
			// select may pick a buffered message even though the term just ended;
			// drop it (it stays unacked and the next leader reclaims it) rather than
			// process it after losing leadership.
			if !c.isCurrentTerm(term) {
				continue
			}
			return fn(msg)
		}
	}
}

func (c *Consumer) Close() error {
	c.cancel()
	c.wg.Wait()
	// Each term's autoCommit loop already flushed its processed work on cancel.
	// Release the lease so a standby can win leadership immediately instead of
	// waiting for it to expire; the successor still reclaims any unacked entries via
	// the idle-guarded path, so in-flight work in an async downstream is not stolen.
	c.releaseLease()
	return nil
}

// releaseLease drops the lease iff we still hold it (a standby's Close is a no-op
// here). It uses a fresh context because closeCtx is already cancelled by the time
// Close calls it.
func (c *Consumer) releaseLease() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := releaseScript.Run(ctx, c.cli, []string{c.leaseKey()}, c.name).Err(); err != nil &&
		!errors.Is(err, redis.Nil) {
		log.ZWarn(ctx, "redismq release lease failed", err, "stream", c.stream, "group", c.group)
	}
}

// manage runs the leader-election loop: it keeps contesting the lease and, while
// held, runs one leadership term (runLeader) whose work loops are torn down as
// soon as the lease is lost or the consumer is closed.
func (c *Consumer) manage() {
	defer c.wg.Done()
	for c.closeCtx.Err() == nil {
		if held, _ := c.tryLease(); held {
			c.runLeader()
			continue
		}
		if !c.sleepRenew() {
			return
		}
	}
}

// runLeader serves one leadership term. Its context is cancelled (stopping the
// work loops) when leadership is lost or the consumer closes.
//
// A transient Redis error during renewal does NOT surrender leadership: the lease
// in Redis has not necessarily expired, and stepping down would cause needless
// churn and widen the split-brain window. We only step down when another holder is
// seen (res==0), or when enough time has passed since the last successful renewal
// that the lease could have expired.
func (c *Consumer) runLeader() {
	ctx, cancel := context.WithCancel(c.closeCtx)
	defer cancel()
	c.startLeader(ctx)

	ticker := time.NewTicker(c.cfg.RenewInterval)
	defer ticker.Stop()
	lastRenew := time.Now()
	for {
		select {
		case <-c.closeCtx.Done():
			return
		case <-ticker.C:
		}
		held, errored := c.tryLease()
		switch {
		case held:
			lastRenew = time.Now()
		case errored:
			if time.Since(lastRenew) >= c.cfg.LeaseTTL {
				return // the lease may have expired during the outage; step down
			}
		default:
			return // another instance holds the lease now
		}
	}
}

// sleepRenew waits one renew interval; it returns false if the consumer is closing.
func (c *Consumer) sleepRenew() bool {
	timer := time.NewTimer(c.cfg.RenewInterval)
	defer timer.Stop()
	select {
	case <-c.closeCtx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// tryLease acquires or renews the lease. held is true iff we hold it afterwards;
// errored is true iff the attempt failed with a Redis error — distinct from losing
// the lease to another holder (held=false, errored=false) — so the caller can avoid
// surrendering leadership on a transient blip.
func (c *Consumer) tryLease() (held, errored bool) {
	res, err := leaseScript.Run(c.closeCtx, c.cli, []string{c.leaseKey()},
		c.name, c.cfg.LeaseTTL.Milliseconds()).Int()
	if err != nil {
		if c.closeCtx.Err() == nil {
			log.ZWarn(c.closeCtx, "redismq lease acquire/renew failed", err, "stream", c.stream, "group", c.group)
		}
		return false, true
	}
	return res == 1, false
}

// startLeader launches the work loops for one leadership term. Reclaiming the dead
// leader's pending entries runs to completion (in stream order) before the live
// read loop starts, so the delivered stream stays monotonic and the watermark ack
// stays correct.
func (c *Consumer) startLeader(ctx context.Context) {
	term := c.beginTerm()
	c.wg.Add(3)
	go func() {
		defer c.wg.Done()
		defer c.endTerm(term)
		c.reclaimPending(ctx, term)
		c.readLoop(ctx, term)
	}()
	go func() {
		defer c.wg.Done()
		c.autoCommitLoop(ctx, term)
	}()
	go func() {
		defer c.wg.Done()
		c.maintenanceLoop(ctx)
	}()
}

func (c *Consumer) readLoop(ctx context.Context, term *deliveryTerm) {
	for {
		if ctx.Err() != nil {
			return
		}
		res, err := c.cli.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.group,
			Consumer: c.name,
			Streams:  []string{c.stream, ">"},
			Count:    c.cfg.ReadCount,
			Block:    c.cfg.BlockTimeout,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // BLOCK timed out with no new messages
			}
			if ctx.Err() != nil {
				return
			}
			log.ZWarn(ctx, "redismq XReadGroup failed", err, "stream", c.stream, "group", c.group)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, s := range res {
			for _, entry := range s.Messages {
				if !c.deliver(ctx, term, entry) {
					return
				}
			}
		}
	}
}

// reclaimPending takes over entries still pending from previous leaders and
// delivers them before the live read loop starts, so the delivered stream stays
// monotonic. It first COLLECTS claimed entries across as many scan passes as
// needed (retrying while orphaned entries are not yet idle enough, or after a
// transient Redis error) and only delivers them — sorted by id — once no entry
// remains owned by another (departed) consumer.
//
// Collect-then-deliver, rather than delivering as each pass claims, for two
// reasons:
//
//   - Claimability follows idle time, not id order: after chained failovers an
//     intermediate leader's claim resets an entry's idle clock, so a LOWER id can
//     become claimable only on a later pass than a higher one. Delivering pass by
//     pass would then emit the lower id after the higher, and a batch handler
//     marking the higher id would watermark-ack the out-of-order entry before it
//     was processed — losing it on the next failover.
//   - A transient Redis error must retry, never abandon the reclaim: falling
//     through to the live read loop would strand the orphans for the whole
//     leadership term (reclaim runs only at term start; a mid-term reclaim would
//     itself break the ordered-delivery contract above).
//
// The collected map holds at most the predecessors' delivered-but-unacked
// backlog, which is bounded by their in-process buffers — not by stream length.
//
// It always honours ClaimMinIdle (never claims with zero idle): a predecessor's
// entry may still be in flight in an async downstream (e.g. a batcher worker that
// has not finished), and Close/lease-loss does not prove that work is done. Waiting
// out the idle guard lets the predecessor finish and ack, or the entry age past the
// guard, before we redeliver it. ClaimMinIdle must therefore exceed the worst-case
// downstream processing time, exactly like Kafka's max.poll.interval.ms.
func (c *Consumer) reclaimPending(ctx context.Context, term *deliveryTerm) {
	claimed := make(map[string]redis.XMessage) // keyed by id: dedups re-claims across passes
	for ctx.Err() == nil {
		err := c.claimScan(ctx, claimed)
		if err == nil {
			var orphaned int64
			orphaned, err = c.orphanedPending(ctx)
			if err == nil && orphaned == 0 {
				c.deliverClaimed(ctx, term, claimed)
				return
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.ZWarn(ctx, "redismq reclaim scan failed, retrying", err, "stream", c.stream, "group", c.group)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.cfg.ReclaimRetry):
		}
	}
}

// claimScan XAUTOCLAIMs the whole PEL once (paginated) at the ClaimMinIdle
// threshold, collecting each claimed entry. An entry claimed again on a later
// pass (its idle resets on claim, so this needs the loop to outlast ClaimMinIdle)
// just overwrites its previous copy.
func (c *Consumer) claimScan(ctx context.Context, claimed map[string]redis.XMessage) error {
	cursor := "0-0"
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, next, err := c.cli.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   c.stream,
			Group:    c.group,
			Consumer: c.name,
			MinIdle:  c.cfg.ClaimMinIdle,
			Start:    cursor,
			Count:    c.cfg.ReadCount,
		}).Result()
		if err != nil {
			return err
		}
		for _, entry := range entries {
			claimed[entry.ID] = entry
		}
		if next == "0-0" || next == "" {
			return nil // a full scan completed
		}
		cursor = next
	}
}

// deliverClaimed delivers the collected entries in id order.
func (c *Consumer) deliverClaimed(ctx context.Context, term *deliveryTerm, claimed map[string]redis.XMessage) {
	entries := make([]redis.XMessage, 0, len(claimed))
	for _, entry := range claimed {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return compareID(entries[i].ID, entries[j].ID) < 0 })
	for _, entry := range entries {
		if !c.deliver(ctx, term, entry) {
			return
		}
	}
}

// orphanedPending counts pending entries still owned by other (departed) consumers.
func (c *Consumer) orphanedPending(ctx context.Context) (int64, error) {
	res, err := c.cli.XPending(ctx, c.stream, c.group).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}
	var n int64
	for name, cnt := range res.Consumers {
		if name != c.name {
			n += cnt
		}
	}
	return n, nil
}

// deliver hands an entry to a Subscribe caller and only then records it as
// in-flight for the watermark ack. Recording AFTER the successful send ensures an
// entry aborted by shutdown/demotion never lingers in the ack window (where a later
// watermark could ack it without it ever being processed). The single reader sends
// in id order, so pending stays ordered; the brief window where a handler may Mark
// before onDeliver runs is benign — the next flush acks it. It returns false if the
// consumer is shutting down or the term has ended.
func (c *Consumer) deliver(ctx context.Context, term *deliveryTerm, entry redis.XMessage) bool {
	msg := buildMessage(entry, term.ack)
	select {
	case <-ctx.Done():
		return false
	case <-c.closeCtx.Done():
		return false
	case term.ch <- msg:
		term.ack.onDeliver(entry.ID)
		return true
	}
}

func (c *Consumer) autoCommitLoop(ctx context.Context, term *deliveryTerm) {
	ticker := time.NewTicker(c.cfg.AutoCommit)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			term.ack.flush(context.Background())
			return
		case <-ticker.C:
			term.ack.flush(ctx)
		}
	}
}

// maintenanceLoop runs leader-only housekeeping: bounding stream memory and pruning
// stale consumer names. Only the single active leader runs it, so the operations
// never race between instances.
func (c *Consumer) maintenanceLoop(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.MaintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.trimStream(ctx)
			c.pruneConsumers(ctx)
		}
	}
}

// trimStream bounds stream memory by deleting entries already delivered AND acked
// by the group, never touching pending (unacked) or undelivered entries. The safe
// lower bound is the group's smallest pending id (oldest unacked); with nothing
// pending it is the group's last-delivered id (everything below it is acked). It
// uses an exact MINID trim, which removes only entries strictly older than minID,
// so an unacked entry is never dropped.
//
// This assumes a single consumer group per stream (the case here): trimming by one
// group's progress would drop entries another group has not yet read.
func (c *Consumer) trimStream(ctx context.Context) {
	groups, err := c.cli.XInfoGroups(ctx, c.stream).Result()
	if err != nil {
		if ctx.Err() == nil {
			log.ZWarn(ctx, "redismq XInfoGroups failed", err, "stream", c.stream)
		}
		return
	}
	var g *redis.XInfoGroup
	for i := range groups {
		if groups[i].Name == c.group {
			g = &groups[i]
			break
		}
	}
	if g == nil {
		return
	}
	minID := g.LastDeliveredID
	if g.Pending > 0 {
		p, err := c.cli.XPending(ctx, c.stream, c.group).Result()
		if err != nil {
			if ctx.Err() == nil {
				log.ZWarn(ctx, "redismq XPending failed", err, "stream", c.stream, "group", c.group)
			}
			return
		}
		if p.Lower != "" {
			minID = p.Lower
		}
	}
	if minID == "" || minID == "0-0" {
		return
	}
	if err := c.cli.XTrimMinID(ctx, c.stream, minID).Err(); err != nil {
		if ctx.Err() == nil {
			log.ZWarn(ctx, "redismq XTrimMinID failed", err, "stream", c.stream, "minID", minID)
		}
	}
}

// pruneConsumers removes group consumer entries left behind by previous leaders.
// Each process instance uses a fresh consumer name, so without pruning they pile up
// in the group forever. Only consumers with no pending entries that have been idle
// past ConsumerIdleTimeout are removed, never ourselves.
func (c *Consumer) pruneConsumers(ctx context.Context) {
	consumers, err := c.cli.XInfoConsumers(ctx, c.stream, c.group).Result()
	if err != nil {
		if ctx.Err() == nil {
			log.ZWarn(ctx, "redismq XInfoConsumers failed", err, "stream", c.stream, "group", c.group)
		}
		return
	}
	for _, con := range consumers {
		if con.Name == c.name || con.Pending > 0 || con.Idle < c.cfg.ConsumerIdleTimeout {
			continue
		}
		if err := c.cli.XGroupDelConsumer(ctx, c.stream, c.group, con.Name).Err(); err != nil {
			if ctx.Err() == nil {
				log.ZWarn(ctx, "redismq XGroupDelConsumer failed", err, "stream", c.stream, "consumer", con.Name)
			}
		}
	}
}
