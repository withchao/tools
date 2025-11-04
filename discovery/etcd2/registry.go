package etcd2

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultDialTimeout        = 5 * time.Second
	defaultLeaseTTL           = 30 * time.Second
	defaultRequestTimeout     = 5 * time.Second
	defaultInitialResolveWait = 5 * time.Second
	resolveRetryInterval      = 200 * time.Millisecond
	keepAliveInitialBackoff   = time.Second
	keepAliveMaxBackoff       = 30 * time.Second
)

var errNoEndpoints = errors.New("etcd: no available endpoints")

type optionState struct {
	leaseTTL           time.Duration
	initialResolveWait time.Duration
}

// Option 用于自定义etcd客户端或注册行为。
type Option func(cfg *clientv3.Config, state *optionState)

// WithDialTimeout 设置etcd客户端拨号超时时间。
func WithDialTimeout(timeout time.Duration) Option {
	return func(cfg *clientv3.Config, state *optionState) {
		cfg.DialTimeout = timeout
	}
}

// WithUsernamePassword 配置etcd客户端账号密码。
func WithUsernamePassword(username, password string) Option {
	return func(cfg *clientv3.Config, state *optionState) {
		cfg.Username = username
		cfg.Password = password
	}
}

// WithTLSConfig 配置etcd客户端TLS参数。
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(cfg *clientv3.Config, state *optionState) {
		cfg.TLS = tlsConfig
	}
}

// WithLeaseTTL 设置注册租约的TTL。
func WithLeaseTTL(ttl time.Duration) Option {
	return func(cfg *clientv3.Config, state *optionState) {
		if ttl > 0 {
			state.leaseTTL = ttl
		}
	}
}

// WithInitialResolveWait 设置首次解析服务地址时的等待时间。
func WithInitialResolveWait(wait time.Duration) Option {
	return func(cfg *clientv3.Config, state *optionState) {
		if wait <= 0 {
			state.initialResolveWait = 0
			return
		}
		state.initialResolveWait = wait
	}
}

type registration struct {
	serviceName string
	key         string
	target      string
	leaseID     clientv3.LeaseID
	manager     endpoints.Manager
	cancelMu    sync.Mutex
	cancel      context.CancelFunc
	stopOnce    sync.Once
	stopCh      chan struct{}
}

func (r *registration) setCancel(cancel context.CancelFunc) {
	r.cancelMu.Lock()
	r.cancel = cancel
	r.cancelMu.Unlock()
}

func (r *registration) cancelKeepAlive() {
	r.cancelMu.Lock()
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	r.cancelMu.Unlock()
}

func (r *registration) stop() {
	r.cancelKeepAlive()
	r.stopOnce.Do(func() {
		if r.stopCh != nil {
			close(r.stopCh)
		}
	})
}

func (r *registration) stopChan() <-chan struct{} {
	if r.stopCh == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return r.stopCh
}

// Registry 基于etcd实现的服务注册与发现。
type Registry struct {
	client *clientv3.Client
	root   string

	leaseTTL           time.Duration
	initialResolveWait time.Duration

	mu            sync.RWMutex
	dialOptions   []grpc.DialOption
	connCache     map[string]map[string]*grpc.ClientConn // service -> addr -> conn
	rrIndex       map[string]int
	registrations map[string]*registration
}

// NewRegistry 创建一个新的etcd注册中心实现。
func NewRegistry(rootDirectory string, endpointsList []string, opts ...Option) (SvcDiscoveryRegistry, error) {
	if len(endpointsList) == 0 {
		return nil, fmt.Errorf("etcd: endpoints is empty")
	}
	cfg := clientv3.Config{
		Endpoints:           endpointsList,
		DialTimeout:         defaultDialTimeout,
		PermitWithoutStream: true,
		Logger:              zap.NewNop(),
	}
	state := &optionState{
		leaseTTL:           defaultLeaseTTL,
		initialResolveWait: defaultInitialResolveWait,
	}
	for _, opt := range opts {
		opt(&cfg, state)
	}

	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}

	r := &Registry{
		client:             client,
		root:               strings.TrimSuffix(rootDirectory, "/"),
		leaseTTL:           state.leaseTTL,
		initialResolveWait: state.initialResolveWait,
		dialOptions:        nil,
		connCache:          make(map[string]map[string]*grpc.ClientConn),
		rrIndex:            make(map[string]int),
		registrations:      make(map[string]*registration),
	}
	return r, nil
}

// GetConn 返回某个服务的单个grpc连接，采用简单的轮询策略。
func (r *Registry) GetConn(ctx context.Context, serviceName string, opts ...grpc.DialOption) (grpc.ClientConnInterface, error) {
	conns, err := r.GetConns(ctx, serviceName, opts...)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(conns) == 0 {
		return nil, errNoEndpoints
	}
	index := r.rrIndex[serviceName] % len(conns)
	r.rrIndex[serviceName] = (index + 1) % len(conns)
	return conns[index], nil
}

// GetConns 返回某个服务的全部grpc连接。
func (r *Registry) GetConns(ctx context.Context, serviceName string, opts ...grpc.DialOption) ([]grpc.ClientConnInterface, error) {
	ctx = contextOrBackground(ctx)
	prefix := r.servicePrefix(serviceName)
	waitDeadline := time.Time{}
	if r.initialResolveWait > 0 {
		waitDeadline = time.Now().Add(r.initialResolveWait)
	}

	for {
		resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
		if err != nil {
			if waitDeadline.IsZero() || time.Now().After(waitDeadline) {
				return nil, err
			}
			if waitErr := waitUntil(ctx, waitDeadline); waitErr != nil {
				return nil, waitErr
			}
			continue
		}

		addresses := make([]string, 0, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			if addr := r.extractAddress(string(kv.Key)); addr != "" {
				addresses = append(addresses, addr)
			}
		}
		if len(addresses) == 0 {
			if !waitDeadline.IsZero() && time.Now().Before(waitDeadline) {
				if waitErr := waitUntil(ctx, waitDeadline); waitErr != nil {
					return nil, waitErr
				}
				continue
			}
			r.clearServiceCache(serviceName)
			return nil, fmt.Errorf("%w: service=%s", errNoEndpoints, serviceName)
		}

		sort.Strings(addresses)

		baseOptions := r.getDialOptions()
		dialOptions := append(baseOptions, opts...)

		conns, err := r.ensureConnections(ctx, serviceName, addresses, dialOptions)
		if err != nil {
			if waitDeadline.IsZero() || time.Now().After(waitDeadline) {
				return nil, err
			}
			if waitErr := waitUntil(ctx, waitDeadline); waitErr != nil {
				return nil, waitErr
			}
			continue
		}
		if len(conns) == 0 {
			if !waitDeadline.IsZero() && time.Now().Before(waitDeadline) {
				if waitErr := waitUntil(ctx, waitDeadline); waitErr != nil {
					return nil, waitErr
				}
				continue
			}
			return nil, fmt.Errorf("%w: service=%s", errNoEndpoints, serviceName)
		}

		result := make([]grpc.ClientConnInterface, len(conns))
		for i := range conns {
			result[i] = conns[i]
		}
		return result, nil
	}
}

// IsSelfNode 判断连接是否指向当前节点。
func (r *Registry) IsSelfNode(cc grpc.ClientConnInterface) bool {
	conn, ok := cc.(*grpc.ClientConn)
	if !ok {
		return false
	}
	target := conn.Target()

	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, reg := range r.registrations {
		if target == reg.target || strings.HasSuffix(target, "/"+reg.target) || strings.Contains(target, reg.target) {
			return true
		}
	}
	return false
}

// AddOption 为后续拨号追加grpc.DialOption，并关闭已有连接以便生效。
func (r *Registry) AddOption(opts ...grpc.DialOption) {
	if len(opts) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dialOptions = append(r.dialOptions, opts...)
	r.resetConnCacheLocked()
}

// Register 在etcd中注册服务。
func (r *Registry) Register(ctx context.Context, serviceName, host string, port int, _ ...grpc.DialOption) error {
	address := net.JoinHostPort(host, strconv.Itoa(port))
	servicePrefix := r.servicePrefix(serviceName)
	manager, err := endpoints.NewManager(r.client, servicePrefix)
	if err != nil {
		return err
	}

	ttlSeconds := int64(r.leaseTTL / time.Second)
	if ttlSeconds <= 0 {
		ttlSeconds = int64(defaultLeaseTTL / time.Second)
	}

	leaseResp, err := r.client.Grant(ctx, ttlSeconds)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/%s", servicePrefix, address)
	endpoint := endpoints.Endpoint{Addr: address}
	if err := manager.AddEndpoint(ctx, key, endpoint, clientv3.WithLease(leaseResp.ID)); err != nil {
		_, _ = r.client.Revoke(context.Background(), leaseResp.ID)
		return err
	}

	reg := &registration{
		serviceName: serviceName,
		key:         key,
		target:      address,
		leaseID:     leaseResp.ID,
		manager:     manager,
		stopCh:      make(chan struct{}),
	}

	r.mu.Lock()
	if old := r.registrations[serviceName]; old != nil {
		delete(r.registrations, serviceName)
		r.mu.Unlock()
		r.unregister(old)
		r.mu.Lock()
	}
	r.registrations[serviceName] = reg
	r.mu.Unlock()
	go r.keepRegistrationAlive(reg)
	return nil
}

// Close 关闭所有连接并取消注册。
func (r *Registry) Close() {
	r.mu.Lock()
	conns := r.connCache
	r.connCache = make(map[string]map[string]*grpc.ClientConn)
	registrations := r.registrations
	r.registrations = make(map[string]*registration)
	r.mu.Unlock()

	for _, pool := range conns {
		for _, conn := range pool {
			_ = conn.Close()
		}
	}

	for _, reg := range registrations {
		r.unregister(reg)
	}

	if r.client != nil {
		_ = r.client.Close()
	}
}

func (r *Registry) ensureConnections(ctx context.Context, serviceName string, addresses []string, dialOptions []grpc.DialOption) ([]*grpc.ClientConn, error) {
	addressSet := make(map[string]struct{}, len(addresses))
	for _, addr := range addresses {
		addressSet[addr] = struct{}{}
	}

	r.mu.Lock()
	pool, ok := r.connCache[serviceName]
	if !ok {
		pool = make(map[string]*grpc.ClientConn)
		r.connCache[serviceName] = pool
	}

	var stale []*grpc.ClientConn
	for addr, conn := range pool {
		if _, exists := addressSet[addr]; !exists {
			stale = append(stale, conn)
			delete(pool, addr)
		}
	}
	r.mu.Unlock()

	for _, conn := range stale {
		_ = conn.Close()
	}

	toDial := make([]string, 0, len(addresses))

	r.mu.RLock()
	pool = r.connCache[serviceName]
	for _, addr := range addresses {
		if _, ok := pool[addr]; !ok {
			toDial = append(toDial, addr)
		}
	}
	r.mu.RUnlock()

	dialled := make(map[string]*grpc.ClientConn, len(toDial))
	for _, addr := range toDial {
		conn, err := r.dialConn(ctx, addr, dialOptions)
		if err != nil {
			continue
		}
		dialled[addr] = conn
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	pool = r.connCache[serviceName]
	if pool == nil {
		pool = make(map[string]*grpc.ClientConn)
		r.connCache[serviceName] = pool
	}
	for addr, conn := range dialled {
		if existing, ok := pool[addr]; ok {
			// 其他协程已经建立连接，关闭当前连接避免泄露。
			_ = conn.Close()
			pool[addr] = existing
			continue
		}
		pool[addr] = conn
	}

	conns := make([]*grpc.ClientConn, 0, len(addresses))
	for _, addr := range addresses {
		if conn, ok := pool[addr]; ok {
			conns = append(conns, conn)
		}
	}
	return conns, nil
}

func (r *Registry) getDialOptions() []grpc.DialOption {
	r.mu.RLock()
	defer r.mu.RUnlock()
	base := make([]grpc.DialOption, len(r.dialOptions))
	copy(base, r.dialOptions)
	return base
}

func (r *Registry) resetConnCacheLocked() {
	for _, pool := range r.connCache {
		for _, conn := range pool {
			_ = conn.Close()
		}
	}
	r.connCache = make(map[string]map[string]*grpc.ClientConn)
	r.rrIndex = make(map[string]int)
}

func (r *Registry) clearServiceCache(serviceName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if pool, ok := r.connCache[serviceName]; ok {
		for _, conn := range pool {
			_ = conn.Close()
		}
		delete(r.connCache, serviceName)
	}
	delete(r.rrIndex, serviceName)
}

func (r *Registry) unregister(reg *registration) {
	if reg == nil {
		return
	}
	reg.stop()
	if reg.manager != nil && reg.key != "" {
		ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
		_ = reg.manager.DeleteEndpoint(ctx, reg.key)
		cancel()
	}
	if reg.leaseID != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
		_, _ = r.client.Revoke(ctx, reg.leaseID)
		cancel()
	}
}

func (r *Registry) keepRegistrationAlive(reg *registration) {
	backoff := keepAliveInitialBackoff
	for {
		if r.isRegistryStopped(reg) {
			return
		}

		ctx, cancel := context.WithCancel(context.Background())
		reg.setCancel(cancel)

		ch, err := r.client.KeepAlive(ctx, reg.leaseID)
		if err != nil {
			reg.cancelKeepAlive()
			if r.waitForRetryOrStop(reg.stopChan(), backoff) {
				return
			}
			if err := r.refreshRegistration(reg); err != nil {
				backoff = nextBackoff(backoff)
				continue
			}
			backoff = keepAliveInitialBackoff
			continue
		}

		for {
			select {
			case <-reg.stopChan():
				reg.cancelKeepAlive()
				return
			case _, ok := <-ch:
				if !ok {
					reg.cancelKeepAlive()
					if r.waitForRetryOrStop(reg.stopChan(), backoff) {
						return
					}
					if err := r.refreshRegistration(reg); err != nil {
						backoff = nextBackoff(backoff)
					} else {
						backoff = keepAliveInitialBackoff
					}
					break
				}
			}
		}
	}
}

func (r *Registry) servicePrefix(serviceName string) string {
	service := strings.Trim(serviceName, "/")
	if r.root == "" {
		return service
	}
	return r.root + "/" + service
}

func (r *Registry) extractAddress(key string) string {
	segments := strings.Split(strings.Trim(key, "/"), "/")
	if len(segments) == 0 {
		return ""
	}
	addr := segments[len(segments)-1]
	if addr == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return ""
	}
	return addr
}

func (r *Registry) dialConn(ctx context.Context, addr string, opts []grpc.DialOption) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err == nil {
		return conn, nil
	}
	if strings.Contains(err.Error(), "no transport security set") {
		fallback := append([]grpc.DialOption{}, opts...)
		fallback = append(fallback, grpc.WithTransportCredentials(insecure.NewCredentials()))
		return grpc.DialContext(ctx, addr, fallback...)
	}
	return nil, err
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}

func (r *Registry) refreshRegistration(reg *registration) error {
	if reg.manager == nil {
		return errors.New("nil endpoint manager")
	}
	select {
	case <-reg.stopChan():
		return context.Canceled
	default:
	}

	ttlSeconds := r.leaseTTLSeconds()

	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	leaseResp, err := r.client.Grant(ctx, ttlSeconds)
	if err != nil {
		return err
	}

	endpoint := endpoints.Endpoint{Addr: reg.target}
	if err := reg.manager.AddEndpoint(ctx, reg.key, endpoint, clientv3.WithLease(leaseResp.ID)); err != nil {
		revokeCtx, revokeCancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
		_, _ = r.client.Revoke(revokeCtx, leaseResp.ID)
		revokeCancel()
		return err
	}

	reg.leaseID = leaseResp.ID
	return nil
}

func (r *Registry) leaseTTLSeconds() int64 {
	ttl := r.leaseTTL
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	seconds := int64(ttl / time.Second)
	if seconds <= 0 {
		seconds = 1
	}
	return seconds
}

func (r *Registry) isRegistryStopped(reg *registration) bool {
	select {
	case <-reg.stopChan():
		return true
	default:
		return false
	}
}

func (r *Registry) waitForRetryOrStop(stop <-chan struct{}, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-stop:
		return true
	case <-timer.C:
		return false
	}
}

func nextBackoff(current time.Duration) time.Duration {
	if current <= 0 {
		return keepAliveInitialBackoff
	}
	current *= 2
	if current > keepAliveMaxBackoff {
		return keepAliveMaxBackoff
	}
	return current
}

func waitUntil(ctx context.Context, deadline time.Time) error {
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		wait := remaining
		if wait > resolveRetryInterval {
			wait = resolveRetryInterval
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}
