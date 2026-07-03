package redismq

import (
	"context"
	"sync"

	"github.com/openimsdk/tools/log"
	"github.com/redis/go-redis/v9"
)

// acker emulates Kafka's offset-commit semantics on top of per-entry XACK.
//
// Redis Streams ack individual entry ids, but the existing consumers commit only
// the LAST message of a batch and expect everything before it to be acked too
// (offset-range commit). We reproduce that with a monotonic watermark:
//
//   - onDeliver appends each delivered id, in stream order, to pending.
//   - mark raises the watermark to the highest processed id.
//   - flush XACKs every pending id <= watermark (a contiguous prefix, since both
//     pending and the watermark advance in id order) and drops them.
//
// CONTRACT (same one Kafka offset-commit relies on): marking id X must mean every
// delivered id <= X is also done. This holds here because the stream is single and
// ordered, one active consumer delivers it in id order, and the batcher runs with
// syncWait=true — its OnComplete fires once per batch, AFTER every worker for that
// batch has finished, and marks the batch's last id. So a higher id is never marked
// while a lower delivered id is still in flight.
//
// We deliberately do NOT switch to a "longest contiguous explicitly-marked prefix"
// model: that would be safer against an out-of-order mark, but it would also break
// the OnlineHistory consumer, which marks ONLY the last id of a batch — under that
// model nothing earlier would ever be acked. A future async (non-syncWait) consumer
// that marks out of order would violate this contract; such consumers must mark
// every processed id, not just the last.
type acker struct {
	cli    redis.UniversalClient
	stream string
	group  string

	mu        sync.Mutex
	pending   []string // delivered-but-unacked ids, ascending by id
	watermark string   // highest marked id; "" means nothing marked yet
}

func (a *acker) onDeliver(id string) {
	a.mu.Lock()
	a.pending = append(a.pending, id)
	a.mu.Unlock()
}

func (a *acker) mark(id string) {
	a.mu.Lock()
	if compareID(id, a.watermark) > 0 {
		a.watermark = id
	}
	a.mu.Unlock()
}

func (a *acker) commit() {
	a.flush(context.Background())
}

// flush acks the contiguous prefix of pending ids that are <= watermark.
func (a *acker) flush(ctx context.Context) {
	a.mu.Lock()
	if a.watermark == "" || len(a.pending) == 0 {
		a.mu.Unlock()
		return
	}
	n := 0
	for n < len(a.pending) && compareID(a.pending[n], a.watermark) <= 0 {
		n++
	}
	if n == 0 {
		a.mu.Unlock()
		return
	}
	ackIDs := a.pending[:n:n]
	a.pending = a.pending[n:]
	a.mu.Unlock()

	if err := a.cli.XAck(ctx, a.stream, a.group, ackIDs...).Err(); err != nil {
		// On failure the ids stay in the PEL and will be reclaimed later, so
		// re-queue them locally for the next flush instead of dropping them.
		a.mu.Lock()
		a.pending = append(ackIDs, a.pending...)
		a.mu.Unlock()
		if ctx.Err() == nil {
			log.ZWarn(ctx, "redismq XAck failed", err, "stream", a.stream, "group", a.group, "count", len(ackIDs))
		}
	}
}
