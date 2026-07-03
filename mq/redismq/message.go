package redismq

import (
	"context"
	"strconv"
	"strings"

	"github.com/openimsdk/tools/mcontext"
	"github.com/redis/go-redis/v9"
)

// message adapts a Redis Stream entry to mq.Message. Mark/Commit delegate to the
// owning consumer's acker so that XACK is issued the same way Kafka commits offsets.
type message struct {
	ctx   context.Context
	id    string
	key   string
	value []byte
	acker *acker
}

func (m *message) Context() context.Context { return m.ctx }
func (m *message) Key() string              { return m.key }
func (m *message) Value() []byte            { return m.value }
func (m *message) Mark()                    { m.acker.mark(m.id) }
func (m *message) Commit()                  { m.acker.commit() }

// encodeHeader serialises the context fields propagated through the queue, matching
// the Kafka producer (operationID, opUserID, platform, connID) — including rejecting
// a context that lacks the mandatory operationID, so a send that would fail on the
// kafka engine fails identically here instead of silently emitting an empty header.
func encodeHeader(ctx context.Context, values map[string]any) error {
	operationID, opUserID, platform, connID, err := mcontext.GetCtxInfos(ctx)
	if err != nil {
		return err
	}
	values[fieldOpID] = operationID
	values[fieldOpUserID] = opUserID
	values[fieldPlatform] = platform
	values[fieldConnID] = connID
	return nil
}

// decodeContext rebuilds the context from a stream entry, mirroring the Kafka
// consumer's GetContextWithMQHeader (values are ordered operationID, opUserID,
// platform, connID — the order WithMustInfoCtx expects).
func decodeContext(values map[string]any) context.Context {
	return mcontext.WithMustInfoCtx([]string{
		valString(values[fieldOpID]),
		valString(values[fieldOpUserID]),
		valString(values[fieldPlatform]),
		valString(values[fieldConnID]),
	})
}

func buildMessage(entry redis.XMessage, acker *acker) *message {
	return &message{
		ctx:   decodeContext(entry.Values),
		id:    entry.ID,
		key:   valString(entry.Values[fieldKey]),
		value: []byte(valString(entry.Values[fieldValue])),
		acker: acker,
	}
}

func valString(v any) string {
	s, _ := v.(string)
	return s
}

// compareID orders Redis Stream entry ids ("<ms>-<seq>"). An empty id sorts lowest.
func compareID(a, b string) int {
	am, as := parseID(a)
	bm, bs := parseID(b)
	switch {
	case am != bm:
		if am < bm {
			return -1
		}
		return 1
	case as != bs:
		if as < bs {
			return -1
		}
		return 1
	default:
		return 0
	}
}

func parseID(id string) (ms, seq int64) {
	if id == "" {
		return -1, -1
	}
	dash := strings.IndexByte(id, '-')
	if dash < 0 {
		ms, _ = strconv.ParseInt(id, 10, 64)
		return ms, 0
	}
	ms, _ = strconv.ParseInt(id[:dash], 10, 64)
	seq, _ = strconv.ParseInt(id[dash+1:], 10, 64)
	return ms, seq
}
