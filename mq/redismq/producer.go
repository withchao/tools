package redismq

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Producer publishes to a single Redis Stream with XADD. The Redis client is owned
// externally, so Close is a no-op.
type Producer struct {
	cli    redis.UniversalClient
	stream string
	maxLen int64
}

func NewProducer(cli redis.UniversalClient, stream string, maxLen int64) *Producer {
	return &Producer{cli: cli, stream: stream, maxLen: maxLen}
}

func (p *Producer) SendMessage(ctx context.Context, key string, value []byte) error {
	values := map[string]any{
		fieldKey:   key,
		fieldValue: string(value),
	}
	if err := encodeHeader(ctx, values); err != nil {
		return err
	}
	args := &redis.XAddArgs{Stream: p.stream, Values: values}
	if p.maxLen > 0 {
		args.MaxLen = p.maxLen
		args.Approx = true // MAXLEN ~ : cheaper, trims in whole macro-nodes
	}
	return p.cli.XAdd(ctx, args).Err()
}

func (p *Producer) Close() error { return nil }
