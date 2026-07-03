package redismq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/openimsdk/tools/mcontext"
	"github.com/openimsdk/tools/mq"
	"github.com/redis/go-redis/v9"
)

// These tests run against a real Redis at localhost:16379, DB 2. They skip if it
// is unreachable so `go test ./...` stays green elsewhere.
const (
	testAddr   = "127.0.0.1:16379"
	testPasswd = "openIM123"
	testDB     = 2
)

func dialTest(t *testing.T) redis.UniversalClient {
	t.Helper()
	cli := redis.NewClient(&redis.Options{Addr: testAddr, Password: testPasswd, DB: testDB})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		_ = cli.Close()
		t.Skipf("redis %s db%d unreachable: %v", testAddr, testDB, err)
	}
	return cli
}

// testConfig uses short timings so leader election / failover complete quickly.
func testConfig() Config {
	return Config{
		ReadCount:     32,
		BlockTimeout:  300 * time.Millisecond,
		Buffer:        128,
		AutoCommit:    150 * time.Millisecond,
		LeaseTTL:      2 * time.Second,
		RenewInterval: 400 * time.Millisecond,
		ClaimMinIdle:  700 * time.Millisecond, // < LeaseTTL so graceful handoff is visibly faster
	}.fillDefault()
}

func uniqueTopic() string { return "mqtest:" + uuid.New().String() }

func cleanup(cli redis.UniversalClient, stream, group string) {
	ctx := context.Background()
	cli.Del(ctx, stream)
	cli.Del(ctx, "mq:lease:"+group+":"+stream)
}

// crashForTest stops the consumer like a hard crash: loops are cancelled but the
// lease is NOT released, so takeover must wait for the lease to expire. Used to
// exercise the unclean-failover path.
func (c *Consumer) crashForTest() {
	c.cancel()
	c.wg.Wait()
}

func pendingCount(t *testing.T, cli redis.UniversalClient, stream, group string) int64 {
	t.Helper()
	res, err := cli.XPending(context.Background(), stream, group).Result()
	if err != nil {
		t.Fatalf("XPending: %v", err)
	}
	return res.Count
}

func produce(t *testing.T, p mq.Producer, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		ctx := mcontext.SetOperationID(context.Background(), fmt.Sprintf("op-%d", i))
		if err := p.SendMessage(ctx, fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
}

// drain runs a Subscribe loop until ctx is cancelled, calling onMsg for each message.
func drain(ctx context.Context, c mq.Consumer, onMsg func(mq.Message)) {
	for {
		err := c.Subscribe(ctx, func(m mq.Message) error {
			onMsg(m)
			return nil
		})
		if err != nil {
			return
		}
	}
}

// waitFor polls cond until true or the deadline elapses.
func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

// Scenario 1: basic produce -> consume, FIFO order, key/value fidelity and context
// (operationID) propagation.
func TestRedisMQ_ProduceConsumeOrderAndContext(t *testing.T) {
	cli := dialTest(t)
	defer cli.Close()
	stream, group := uniqueTopic(), "g1"
	defer cleanup(cli, stream, group)

	const n = 20
	consumer, err := NewConsumer(context.Background(), cli, stream, group, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	produce(t, NewProducer(cli, stream, 0), n)

	var (
		mu   sync.Mutex
		keys []string
		vals []string
		ops  []string
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go drain(ctx, consumer, func(m mq.Message) {
		mu.Lock()
		keys = append(keys, m.Key())
		vals = append(vals, string(m.Value()))
		ops = append(ops, mcontext.GetOperationID(m.Context()))
		mu.Unlock()
		m.Mark()
		m.Commit()
	})

	ok := waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(vals) == n
	})
	if !ok {
		mu.Lock()
		t.Fatalf("got %d/%d messages", len(vals), n)
	}
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < n; i++ {
		if want := fmt.Sprintf("v%d", i); vals[i] != want {
			t.Fatalf("order mismatch at %d: got %s want %s", i, vals[i], want)
		}
		if want := fmt.Sprintf("k%d", i); keys[i] != want {
			t.Fatalf("key mismatch at %d: got %s want %s", i, keys[i], want)
		}
		if want := fmt.Sprintf("op-%d", i); ops[i] != want {
			t.Fatalf("operationID mismatch at %d: got %s want %s", i, ops[i], want)
		}
	}
	if !waitFor(t, 3*time.Second, func() bool { return pendingCount(t, cli, stream, group) == 0 }) {
		t.Fatalf("pending not drained: %d", pendingCount(t, cli, stream, group))
	}
}

// Scenario 2: per-message Mark+Commit (push / async-msg pattern) acks every entry.
func TestRedisMQ_PerMessageAck(t *testing.T) {
	cli := dialTest(t)
	defer cli.Close()
	stream, group := uniqueTopic(), "g2"
	defer cleanup(cli, stream, group)

	const n = 15
	consumer, err := NewConsumer(context.Background(), cli, stream, group, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	produce(t, NewProducer(cli, stream, 0), n)

	var count int
	var mu sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go drain(ctx, consumer, func(m mq.Message) {
		m.Mark()
		m.Commit()
		mu.Lock()
		count++
		mu.Unlock()
	})

	if !waitFor(t, 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return count == n }) {
		t.Fatalf("processed %d/%d", count, n)
	}
	if !waitFor(t, 3*time.Second, func() bool { return pendingCount(t, cli, stream, group) == 0 }) {
		t.Fatalf("pending not zero: %d", pendingCount(t, cli, stream, group))
	}
}

// Scenario 3: the OnlineHistory pattern — only the LAST message of a batch is
// Mark+Commit'd, and the watermark must ack the whole prefix.
func TestRedisMQ_WatermarkAckMarkLastOnly(t *testing.T) {
	cli := dialTest(t)
	defer cli.Close()
	stream, group := uniqueTopic(), "g3"
	defer cleanup(cli, stream, group)

	const n = 10
	consumer, err := NewConsumer(context.Background(), cli, stream, group, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	produce(t, NewProducer(cli, stream, 0), n)

	var (
		mu   sync.Mutex
		recv []mq.Message
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go drain(ctx, consumer, func(m mq.Message) {
		mu.Lock()
		recv = append(recv, m)
		got := len(recv)
		mu.Unlock()
		// Emulate batcher OnComplete: mark+commit only when the whole batch arrived.
		if got == n {
			m.Mark() // mark only the last message
			m.Commit()
		}
	})

	if !waitFor(t, 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(recv) == n }) {
		t.Fatalf("received %d/%d", len(recv), n)
	}
	// Marking only the last entry must ack all earlier ones too.
	if !waitFor(t, 3*time.Second, func() bool { return pendingCount(t, cli, stream, group) == 0 }) {
		t.Fatalf("watermark did not ack whole batch, pending=%d", pendingCount(t, cli, stream, group))
	}
}

// Scenario 4: two consumers, one group -> exactly one is active; every message is
// delivered exactly once (no double processing across instances).
func TestRedisMQ_LeaderExclusive(t *testing.T) {
	cli := dialTest(t)
	defer cli.Close()
	stream, group := uniqueTopic(), "g4"
	defer cleanup(cli, stream, group)

	const n = 30
	c1, err := NewConsumer(context.Background(), cli, stream, group, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := NewConsumer(context.Background(), cli, stream, group, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	produce(t, NewProducer(cli, stream, 0), n)

	var (
		mu   sync.Mutex
		seen = map[string]int{}
		tot  int
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	record := func(m mq.Message) {
		mu.Lock()
		seen[m.Key()]++
		tot++
		mu.Unlock()
		m.Mark()
		m.Commit()
	}
	go drain(ctx, c1, record)
	go drain(ctx, c2, record)

	if !waitFor(t, 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return tot >= n }) {
		mu.Lock()
		t.Fatalf("processed %d/%d", tot, n)
	}
	// Give a moment to surface any erroneous duplicate from the standby.
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if tot != n {
		t.Fatalf("expected exactly %d deliveries, got %d", n, tot)
	}
	for i := 0; i < n; i++ {
		if c := seen[fmt.Sprintf("k%d", i)]; c != 1 {
			t.Fatalf("key k%d delivered %d times (want 1)", i, c)
		}
	}
}

// Scenario 5: failover / at-least-once. The leader consumes everything but acks
// nothing, then "crashes" (Close). A standby must take over, reclaim the pending
// entries via XAUTOCLAIM, process them and leave the PEL empty.
func TestRedisMQ_FailoverReclaim(t *testing.T) {
	cli := dialTest(t)
	defer cli.Close()
	stream, group := uniqueTopic(), "g5"
	defer cleanup(cli, stream, group)

	const n = 12
	cfg := testConfig()
	c1, err := NewConsumer(context.Background(), cli, stream, group, cfg)
	if err != nil {
		t.Fatal(err)
	}
	produce(t, NewProducer(cli, stream, 0), n)

	var c1count int
	var mu sync.Mutex
	ctx1, cancel1 := context.WithCancel(context.Background())
	go drain(ctx1, c1, func(m mq.Message) {
		mu.Lock()
		c1count++
		mu.Unlock()
		// deliberately no Mark/Commit -> stays pending
	})
	if !waitFor(t, 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return c1count == n }) {
		t.Fatalf("c1 received %d/%d", c1count, n)
	}
	if got := pendingCount(t, cli, stream, group); got != n {
		t.Fatalf("expected %d pending after no-ack, got %d", n, got)
	}
	// Simulate a hard crash: stop the loop without releasing the lease, so the
	// standby can only take over once the lease expires.
	cancel1()
	c1.(*Consumer).crashForTest()

	// Standby takes over.
	c2, err := NewConsumer(context.Background(), cli, stream, group, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	var c2count int
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go drain(ctx2, c2, func(m mq.Message) {
		mu.Lock()
		c2count++
		mu.Unlock()
		m.Mark()
		m.Commit()
	})

	// c2 can only win after c1's lease expires (~LeaseTTL) and then reclaim entries
	// idle >= ClaimMinIdle.
	if !waitFor(t, 15*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return c2count == n }) {
		mu.Lock()
		t.Fatalf("c2 reclaimed %d/%d", c2count, n)
	}
	if !waitFor(t, 3*time.Second, func() bool { return pendingCount(t, cli, stream, group) == 0 }) {
		t.Fatalf("pending not drained after failover: %d", pendingCount(t, cli, stream, group))
	}
}

// Scenario 7: Close must not deadlock even when the internal buffer is full and no
// Subscribe is draining it — deliver() selects on closeCtx so a full channel cannot
// pin the work loops past cancellation.
func TestRedisMQ_CloseNoDeadlockWhenBufferFull(t *testing.T) {
	cli := dialTest(t)
	defer cli.Close()
	stream, group := uniqueTopic(), "g7"
	defer cleanup(cli, stream, group)

	cfg := testConfig()
	cfg.Buffer = 4 // tiny buffer so it fills immediately
	consumer, err := NewConsumer(context.Background(), cli, stream, group, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Produce far more than the buffer and never Subscribe, so the read loop fills
	// the channel and blocks in deliver().
	produce(t, NewProducer(cli, stream, 0), 50)
	time.Sleep(time.Second) // let the read loop fill the buffer and block

	done := make(chan error, 1)
	go func() { done <- consumer.Close() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close deadlocked with a full buffer and no consumer")
	}
}

// Scenario 6: graceful handoff. The leader Close()s cleanly (releasing the lease);
// a standby must win leadership without waiting the full LeaseTTL and reclaim the
// pending entries (still idle-guarded), completing in well under a LeaseTTL.
func TestRedisMQ_GracefulHandoff(t *testing.T) {
	cli := dialTest(t)
	defer cli.Close()
	stream, group := uniqueTopic(), "g6"
	defer cleanup(cli, stream, group)

	const n = 12
	cfg := testConfig()
	c1, err := NewConsumer(context.Background(), cli, stream, group, cfg)
	if err != nil {
		t.Fatal(err)
	}
	produce(t, NewProducer(cli, stream, 0), n)

	var (
		mu      sync.Mutex
		c1count int
		c2count int
	)
	ctx1, cancel1 := context.WithCancel(context.Background())
	go drain(ctx1, c1, func(m mq.Message) {
		mu.Lock()
		c1count++
		mu.Unlock()
		// no ack: leave them pending for the successor to reclaim
	})
	if !waitFor(t, 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return c1count == n }) {
		t.Fatalf("c1 received %d/%d", c1count, n)
	}

	// Bring up the standby, then gracefully close the leader.
	c2, err := NewConsumer(context.Background(), cli, stream, group, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go drain(ctx2, c2, func(m mq.Message) {
		mu.Lock()
		c2count++
		mu.Unlock()
		m.Mark()
		m.Commit()
	})

	cancel1()
	start := time.Now()
	if err := c1.Close(); err != nil { // graceful: releases the lease immediately
		t.Fatal(err)
	}

	if !waitFor(t, 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return c2count == n }) {
		mu.Lock()
		t.Fatalf("c2 reclaimed %d/%d after graceful close", c2count, n)
	}
	if elapsed := time.Since(start); elapsed >= cfg.LeaseTTL {
		t.Fatalf("graceful takeover took %v, expected well under LeaseTTL %v", elapsed, cfg.LeaseTTL)
	}
	if !waitFor(t, 3*time.Second, func() bool { return pendingCount(t, cli, stream, group) == 0 }) {
		t.Fatalf("pending not drained after graceful handoff: %d", pendingCount(t, cli, stream, group))
	}
}

func xlen(t *testing.T, cli redis.UniversalClient, stream string) int64 {
	t.Helper()
	n, err := cli.XLen(context.Background(), stream).Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	return n
}

func consumerExists(cli redis.UniversalClient, stream, group, name string) bool {
	cons, err := cli.XInfoConsumers(context.Background(), stream, group).Result()
	if err != nil {
		return false
	}
	for _, c := range cons {
		if c.Name == name {
			return true
		}
	}
	return false
}

// Scenario 8 (fix A): a demoted leader must NOT keep delivering its buffered
// messages — the successor owns them. We fill a leader's buffer, steal its lease so
// it steps down, then assert its Subscribe delivers nothing while the entries stay
// pending for a successor to reclaim.
func TestRedisMQ_DemotedLeaderFencing(t *testing.T) {
	cli := dialTest(t)
	defer cli.Close()
	stream, group := uniqueTopic(), "g8"
	defer cleanup(cli, stream, group)

	const n = 20
	cfg := testConfig()
	cfg.Buffer = 50 // hold all n in the term buffer without any Subscribe draining it
	c1, err := NewConsumer(context.Background(), cli, stream, group, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	produce(t, NewProducer(cli, stream, 0), n)
	// Wait until the leader has read every entry into its PEL (buffered, undelivered).
	if !waitFor(t, 10*time.Second, func() bool { return pendingCount(t, cli, stream, group) == n }) {
		t.Fatalf("leader did not buffer all messages, pending=%d", pendingCount(t, cli, stream, group))
	}

	// Steal the lease (long TTL) so c1's next renew sees another holder and steps down.
	leaseKey := "mq:lease:" + group + ":" + stream
	if err := cli.Set(context.Background(), leaseKey, "stolen-by-test", 10*time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * cfg.RenewInterval) // let c1 notice it lost the lease and drop its term

	// A demoted leader must not deliver its abandoned buffer.
	var c1count int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go drain(ctx, c1, func(m mq.Message) {
		atomic.AddInt32(&c1count, 1)
		m.Mark()
		m.Commit()
	})
	time.Sleep(time.Second)
	if got := atomic.LoadInt32(&c1count); got != 0 {
		t.Fatalf("demoted leader delivered %d buffered messages (want 0 — should be fenced)", got)
	}
	if got := pendingCount(t, cli, stream, group); got != n {
		t.Fatalf("expected %d entries still pending for a successor, got %d", n, got)
	}
}

// Scenario 9 (fix D): the leader prunes stale consumer names left by previous
// instances (each instance uses a fresh name, so they accumulate forever otherwise).
func TestRedisMQ_PruneStaleConsumers(t *testing.T) {
	cli := dialTest(t)
	defer cli.Close()
	stream, group := uniqueTopic(), "g9"
	defer cleanup(cli, stream, group)

	cfg := testConfig()
	cfg.MaintenanceInterval = 300 * time.Millisecond
	cfg.ConsumerIdleTimeout = 500 * time.Millisecond
	c1, err := NewConsumer(context.Background(), cli, stream, group, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	// Simulate a previous instance's leftover consumer (zero pending).
	if err := cli.XGroupCreateConsumer(context.Background(), stream, group, "ghost-instance").Err(); err != nil {
		t.Fatal(err)
	}
	if !consumerExists(cli, stream, group, "ghost-instance") {
		t.Fatal("precondition: ghost consumer should exist")
	}
	// Once it idles past ConsumerIdleTimeout, the leader's maintenance prunes it.
	if !waitFor(t, 10*time.Second, func() bool { return !consumerExists(cli, stream, group, "ghost-instance") }) {
		t.Fatal("stale consumer was not pruned")
	}
}

// Kafka parity: a context missing the mandatory operationID must fail the send
// (the kafka producer rejects it in GetMQHeaderWithContext) instead of silently
// emitting an empty header. Fails before touching Redis, so no server is needed.
func TestRedisMQ_ProducerRequiresOperationID(t *testing.T) {
	p := NewProducer(nil, "unused", 0)
	if err := p.SendMessage(context.Background(), "k", []byte("v")); err == nil {
		t.Fatal("expected error for ctx without operationID")
	}
}

// Kafka parity: the builder rejects unknown topics (kafkaBuilder errors on them)
// instead of silently creating a stream nothing consumes or trims, and fail-fast
// rejects a nil client.
func TestRedisMQ_BuilderValidation(t *testing.T) {
	cli := dialTest(t)
	defer cli.Close()
	stream, group := "mqtest:validate:known", "gv"
	defer cleanup(cli, stream, group)

	ctx := context.Background()
	b := NewBuilder(cli, map[string]string{"known": group}, Config{StreamPrefix: "mqtest:validate:"})
	if _, err := b.GetTopicProducer(ctx, "unknown"); err == nil {
		t.Fatal("expected unknown-topic error from GetTopicProducer")
	}
	if _, err := b.GetTopicConsumer(ctx, "unknown"); err == nil {
		t.Fatal("expected unknown-topic error from GetTopicConsumer")
	}
	if _, err := b.GetTopicProducer(ctx, "known"); err != nil {
		t.Fatalf("known topic producer: %v", err)
	}
	c, err := b.GetTopicConsumer(ctx, "known")
	if err != nil {
		t.Fatalf("known topic consumer: %v", err)
	}
	c.Close()

	nb := NewBuilder(nil, map[string]string{"known": group}, Config{})
	if _, err := nb.GetTopicProducer(ctx, "known"); err == nil {
		t.Fatal("expected nil-client error")
	}
}

// The version gate refuses servers without XAUTOCLAIM / XTRIM MINID (< 6.2) and
// unparsable versions (fail closed).
func TestRedisMQ_VersionGate(t *testing.T) {
	for _, tc := range []struct {
		info string
		ok   bool
	}{
		{"# Server\r\nredis_version:7.4.1\r\nredis_mode:standalone\r\n", true},
		{"redis_version:6.2.14", true},
		{"redis_version:6.2", true},
		{"redis_version:6.0.9", false},
		{"redis_version:5.0.7", false},
		{"# Server\r\nredis_mode:standalone\r\n", false},
		{"redis_version:banana.split", false},
	} {
		err := checkVersionInfo(tc.info)
		if tc.ok && err != nil {
			t.Errorf("info %q: unexpected error %v", tc.info, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("info %q: expected error, got nil", tc.info)
		}
	}
}

// Scenario 11: reclaim must deliver in id order even when claimability is not
// aligned with id order (an intermediate leader's claim resets an entry's idle
// clock, so a lower id can become claimable later than a higher one). Delivering
// pass-by-pass would emit e1 after e2; collect-then-deliver must not. Out-of-order
// delivery here is a real loss path: a batch handler marking the higher id would
// watermark-ack the unprocessed lower id.
func TestRedisMQ_ReclaimDeliversInIDOrder(t *testing.T) {
	cli := dialTest(t)
	defer cli.Close()
	stream, group := uniqueTopic(), "g11"
	defer cleanup(cli, stream, group)

	ctx := context.Background()
	if err := cli.XGroupCreateMkStream(ctx, stream, group, "$").Err(); err != nil {
		t.Fatal(err)
	}
	const n = 3
	produce(t, NewProducer(cli, stream, 0), n)

	// Pull all entries into a departed ghost consumer's PEL.
	res, err := cli.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: "ghost", Streams: []string{stream, ">"}, Count: n,
	}).Result()
	if err != nil || len(res) != 1 || len(res[0].Messages) != n {
		t.Fatalf("ghost read: %v (res=%v)", err, res)
	}
	ids := make([]string, 0, n)
	for _, m := range res[0].Messages {
		ids = append(ids, m.ID)
	}
	// e0 and e2 look long-abandoned (idle 1h); e1 was recently touched, so it only
	// becomes claimable ClaimMinIdle after the read.
	for _, id := range []string{ids[0], ids[2]} {
		if err := cli.Do(ctx, "XCLAIM", stream, group, "ghost", "0", id, "IDLE", "3600000").Err(); err != nil {
			t.Fatal(err)
		}
	}

	cfg := testConfig()
	cfg.ClaimMinIdle = 1500 * time.Millisecond
	cfg.ReclaimRetry = 200 * time.Millisecond
	consumer, err := NewConsumer(ctx, cli, stream, group, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	var (
		mu   sync.Mutex
		vals []string
	)
	dctx, dcancel := context.WithCancel(context.Background())
	defer dcancel()
	go drain(dctx, consumer, func(m mq.Message) {
		mu.Lock()
		vals = append(vals, string(m.Value()))
		mu.Unlock()
		m.Mark()
		m.Commit()
	})

	if !waitFor(t, 15*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(vals) == n }) {
		mu.Lock()
		t.Fatalf("reclaimed %d/%d: %v", len(vals), n, vals)
	}
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < n; i++ {
		if want := fmt.Sprintf("v%d", i); vals[i] != want {
			t.Fatalf("delivery order broken at %d: got %v", i, vals)
		}
	}
	if !waitFor(t, 3*time.Second, func() bool { return pendingCount(t, cli, stream, group) == 0 }) {
		t.Fatalf("pending not drained: %d", pendingCount(t, cli, stream, group))
	}
}

// Scenario 10 (fix 5): the leader trims acked entries (XACK alone never deletes
// them) while never dropping unacked ones. We ack the first 20 of 30 and leave 10
// pending; trimming must shrink the stream to exactly the 10 unacked entries.
func TestRedisMQ_TrimAckedKeepUnacked(t *testing.T) {
	cli := dialTest(t)
	defer cli.Close()
	stream, group := uniqueTopic(), "g10"
	defer cleanup(cli, stream, group)

	const n, ackUpTo = 30, 20
	cfg := testConfig()
	cfg.MaintenanceInterval = 300 * time.Millisecond
	c1, err := NewConsumer(context.Background(), cli, stream, group, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	produce(t, NewProducer(cli, stream, 0), n)
	var seen int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go drain(ctx, c1, func(m mq.Message) {
		if atomic.AddInt32(&seen, 1) <= ackUpTo { // ack only the first ackUpTo (in order)
			m.Mark()
			m.Commit()
		}
	})
	if !waitFor(t, 10*time.Second, func() bool { return atomic.LoadInt32(&seen) == n }) {
		t.Fatalf("delivered %d/%d", atomic.LoadInt32(&seen), n)
	}
	// The last n-ackUpTo stay pending.
	if !waitFor(t, 3*time.Second, func() bool { return pendingCount(t, cli, stream, group) == n-ackUpTo }) {
		t.Fatalf("expected %d pending, got %d", n-ackUpTo, pendingCount(t, cli, stream, group))
	}
	// Maintenance trims the acked prefix but keeps the unacked entries.
	if !waitFor(t, 5*time.Second, func() bool { return xlen(t, cli, stream) == n-ackUpTo }) {
		t.Fatalf("expected XLEN trimmed to %d (acked removed, unacked kept), got %d", n-ackUpTo, xlen(t, cli, stream))
	}
}
