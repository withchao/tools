package inprocess

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openimsdk/tools/log"
)

func newLocalConn(target func() string) *localConn {
	return &localConn{
		methods: newMethodRegistry(),
		codec:   newProtoCodec(),
		target:  target,
	}
}

type localConn struct {
	methods *methodRegistry
	codec   messageCodec
	target  func() string
}

func (c *localConn) Target() string {
	if c.target == nil {
		return ""
	}
	return c.target()
}

func (c *localConn) Invoke(ctx context.Context, method string, args any, reply any, _ ...grpc.CallOption) error {
	handler, err := c.methods.getUnaryHandler(ctx, method)
	if err != nil {
		return err
	}
	log.ZInfo(ctx, "inprocess rpc server request", "method", method, "req", args)
	start := time.Now()
	resp, err := handler(ctx, args, nil)
	if err == nil {
		log.ZInfo(ctx, "inprocess rpc server response success", "method", method, "cost", time.Since(start), "req", args, "resp", resp)
	} else {
		log.ZError(ctx, "inprocess rpc server response error", err, "method", method, "cost", time.Since(start), "req", args)
	}
	if err != nil {
		return err
	}
	tmp, err := c.codec.Marshal(resp)
	if err != nil {
		return err
	}
	return c.codec.Unmarshal(tmp, reply)
}

func (c *localConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Errorf(codes.Unimplemented, "method stream not implemented")
}
