package inprocess

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type unaryServerHandler func(ctx context.Context, req any, interceptor grpc.UnaryServerInterceptor) (any, error)

// methodWaitTimeout bounds how long an invoke waits for the target method to be
// registered. It only covers startup ordering (all methods register at process
// start) and must stay below remoteInvokeTimeout so a cross-instance caller gets
// this timeout as an error response instead of its http client giving up first.
const methodWaitTimeout = time.Second * 10

func newMethodRegistry() *methodRegistry {
	return &methodRegistry{
		methods: make(map[string]unaryServerHandler),
		codec:   newProtoCodec(),
	}
}

type methodRegistry struct {
	mu      sync.Mutex
	methods map[string]unaryServerHandler
	codec   messageCodec
	waiters map[string]*methodWaiter
}

type methodWaiter struct {
	ready chan struct{}
	refs  int
}

func (r *methodRegistry) RegisterService(desc *grpc.ServiceDesc, impl any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range desc.Methods {
		method := desc.Methods[i]
		fullMethod := fmt.Sprintf("/%s/%s", desc.ServiceName, method.MethodName)
		if _, ok := r.methods[fullMethod]; ok {
			panic(fmt.Errorf("service %s already registered, method %s", desc.ServiceName, method.MethodName))
		}
		r.methods[fullMethod] = func(ctx context.Context, req any, interceptor grpc.UnaryServerInterceptor) (any, error) {
			return method.Handler(impl, ctx, func(in any) error {
				tmp, err := r.codec.Marshal(req)
				if err != nil {
					return err
				}
				return r.codec.Unmarshal(tmp, in)
			}, interceptor)
		}
		if wait, ok := r.waiters[fullMethod]; ok {
			delete(r.waiters, fullMethod)
			close(wait.ready)
		}
	}
}

func (r *methodRegistry) getUnaryHandler(ctx context.Context, fullMethod string) (unaryServerHandler, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	r.mu.Lock()
	handler, ok := r.methods[fullMethod]
	if ok {
		r.mu.Unlock()
		return handler, nil
	}
	if r.waiters == nil {
		r.waiters = make(map[string]*methodWaiter)
	}
	wait, ok := r.waiters[fullMethod]
	if !ok {
		wait = &methodWaiter{ready: make(chan struct{})}
		r.waiters[fullMethod] = wait
	}
	wait.refs++
	r.mu.Unlock()
	defer r.releaseWaiter(fullMethod, wait)

	timeout := time.NewTimer(methodWaitTimeout)
	defer timeout.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timeout.C:
		return nil, fmt.Errorf("get service %s timeout", fullMethod)
	case <-wait.ready:
		r.mu.Lock()
		handler, ok = r.methods[fullMethod]
		r.mu.Unlock()
		if !ok {
			return nil, fmt.Errorf("get service %s internal error", fullMethod)
		}
		return handler, nil
	}
}

func (r *methodRegistry) releaseWaiter(fullMethod string, wait *methodWaiter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.waiters[fullMethod]
	if !ok || current != wait {
		return
	}
	wait.refs--
	if wait.refs <= 0 {
		delete(r.waiters, fullMethod)
	}
}

func (r *methodRegistry) close() {
	r.mu.Lock()
	waiters := r.waiters
	r.waiters = nil
	r.mu.Unlock()

	for _, wait := range waiters {
		close(wait.ready)
	}
}
