package etcd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openimsdk/tools/discovery"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"go.etcd.io/etcd/client/v3/naming/resolver"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	gresolver "google.golang.org/grpc/resolver"
)

const (
	defaultDialTimeout        = 5 * time.Second
	defaultLeaseTTL           = 30 * time.Second
	defaultRequestTimeout     = 5 * time.Second
	defaultInitialResolveWait = 3 * time.Second
	resolveRetryInterval      = 200 * time.Millisecond
	keepAliveInitialBackoff   = time.Second
	keepAliveMaxBackoff       = 30 * time.Second
)

var errNoEndpoints = errors.New("etcd: no available endpoints")

type contextKey string

const (
	contextKeyInitialResolveWait contextKey = "etcd.initialResolveWait"
)

// CfgOption allows customizing the etcd client configuration.
type CfgOption func(*clientv3.Config)

type registration struct {
	serviceName string
	key         string
	target      string
	leaseID     clientv3.LeaseID
	manager     endpoints.Manager

	cancelMu sync.Mutex
	cancel   context.CancelFunc

	stopOnce sync.Once
	stopCh   chan struct{}
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

// SvcDiscoveryRegistryImpl 实现服务注册与发现
type SvcDiscoveryRegistryImpl struct {
	client   *clientv3.Client
	resolver gresolver.Builder

	rootDirectory      string
	watchNames         []string
	initialResolveWait time.Duration
	leaseTTL           time.Duration

	mu            sync.RWMutex
	dialOptions   []grpc.DialOption
	connCache     map[string]map[string]*grpc.ClientConn // service -> addr -> conn
	rrIndex       map[string]int
	registrations map[string]*registration

	serviceKey        string
	endpointMgr       endpoints.Manager
	leaseID           clientv3.LeaseID
	rpcRegisterTarget string

	watchCancels []context.CancelFunc
}

// NewSvcDiscoveryRegistry creates a new service discovery registry implementation.
func NewSvcDiscoveryRegistry(rootDirectory string, endpointsList []string, watchNames []string, options ...CfgOption) (*SvcDiscoveryRegistryImpl, error) {
	if len(endpointsList) == 0 {
		return nil, fmt.Errorf("etcd: endpoints is empty")
	}

	cfg := clientv3.Config{
		Endpoints:           endpointsList,
		DialTimeout:         defaultDialTimeout,
		PermitWithoutStream: true,
		Logger:              zap.NewNop(),
		MaxCallSendMsgSize:  10 * 1024 * 1024,
	}
	for _, opt := range options {
		opt(&cfg)
	}

	initialResolveWait := defaultInitialResolveWait
	if cfg.Context != nil {
		if v, ok := cfg.Context.Value(contextKeyInitialResolveWait).(time.Duration); ok {
			if v < 0 {
				v = 0
			}
			initialResolveWait = v
		}
	}

	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	r, err := resolver.NewBuilder(client)
	if err != nil {
		return nil, err
	}

	s := &SvcDiscoveryRegistryImpl{
		client:             client,
		resolver:           r,
		rootDirectory:      strings.TrimSuffix(rootDirectory, "/"),
		watchNames:         append([]string(nil), watchNames...),
		initialResolveWait: initialResolveWait,
		leaseTTL:           defaultLeaseTTL,
		dialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithResolvers(r),
		},
		connCache:     make(map[string]map[string]*grpc.ClientConn),
		rrIndex:       make(map[string]int),
		registrations: make(map[string]*registration),
	}

	s.watchServiceChanges()
	if len(s.watchNames) > 0 {
		go func() {
			if err := s.initializeConnMap(); err != nil && !errors.Is(err, errNoEndpoints) {
				log.ZWarn(context.Background(), "initializeConnMap err", err)
			}
		}()
	}
	return s, nil
}

// WithDialTimeout sets the dial timeout for the etcd client.
func WithDialTimeout(timeout time.Duration) CfgOption {
	return func(cfg *clientv3.Config) {
		cfg.DialTimeout = timeout
	}
}

// WithMaxCallSendMsgSize sets MaxCallSendMsgSize.
func WithMaxCallSendMsgSize(size int) CfgOption {
	return func(cfg *clientv3.Config) {
		cfg.MaxCallSendMsgSize = size
	}
}

// WithUsernameAndPassword sets username/password for the etcd client.
func WithUsernameAndPassword(username, password string) CfgOption {
	return func(cfg *clientv3.Config) {
		cfg.Username = username
		cfg.Password = password
	}
}

// WithInitialResolveWait configures the resolve wait duration on start.
func WithInitialResolveWait(wait time.Duration) CfgOption {
	return func(cfg *clientv3.Config) {
		if wait < 0 {
			wait = 0
		}
		cfg.Context = withConfigValue(cfg.Context, contextKeyInitialResolveWait, wait)
	}
}

// GetUserIdHashGatewayHost returns the gateway host for a given user ID hash.
func (r *SvcDiscoveryRegistryImpl) GetUserIdHashGatewayHost(ctx context.Context, userID string) (string, error) {
	return "", nil
}

// initializeConnMap warms up connections for watched services.
func (r *SvcDiscoveryRegistryImpl) initializeConnMap(opts ...grpc.DialOption) error {
	for _, name := range r.watchNames {
		if _, err := r.GetConns(context.Background(), name, opts...); err != nil && !errors.Is(err, errNoEndpoints) {
			return err
		}
	}
	return nil
}

// GetConns returns all connections for a service.
func (r *SvcDiscoveryRegistryImpl) GetConns(ctx context.Context, serviceName string, opts ...grpc.DialOption) ([]grpc.ClientConnInterface, error) {
	ctx = contextOrBackground(ctx)
	prefix := r.servicePrefix(serviceName)

	waitDeadline := time.Time{}
	if r.initialResolveWait > 0 {
		waitDeadline = time.Now().Add(r.initialResolveWait)
	}

	for {
		reqCtx, cancel := withRequestTimeout(ctx)
		resp, err := r.client.Get(reqCtx, prefix, clientv3.WithPrefix())
		cancel()
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

		dialOptions := append(r.getDialOptions(), opts...)
		if err := r.checkOpts(dialOptions...); err != nil {
			return nil, errs.WrapMsg(err, "checkOpts is failed")
		}

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

// GetConn returns a single connection for the service using round robin.
func (r *SvcDiscoveryRegistryImpl) GetConn(ctx context.Context, serviceName string, opts ...grpc.DialOption) (grpc.ClientConnInterface, error) {
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

// GetSelfConnTarget returns the target registered for the current service.
func (r *SvcDiscoveryRegistryImpl) GetSelfConnTarget() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.rpcRegisterTarget
}

// IsSelfNode checks if the connection points to the current node.
func (r *SvcDiscoveryRegistryImpl) IsSelfNode(cc grpc.ClientConnInterface) bool {
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

// AddOption adds dial options and resets the current connection cache.
func (r *SvcDiscoveryRegistryImpl) AddOption(opts ...grpc.DialOption) {
	if len(opts) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dialOptions = append(r.dialOptions, opts...)
	r.resetConnCacheLocked()
}

// Register registers a service endpoint.
func (r *SvcDiscoveryRegistryImpl) Register(ctx context.Context, serviceName, host string, port int, _ ...grpc.DialOption) error {
	address := net.JoinHostPort(host, strconv.Itoa(port))
	servicePrefix := r.servicePrefix(serviceName)
	manager, err := endpoints.NewManager(r.client, servicePrefix)
	if err != nil {
		return err
	}

	ttlSeconds := r.leaseTTLSeconds()
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

	r.serviceKey = key
	r.endpointMgr = manager
	r.leaseID = leaseResp.ID
	r.rpcRegisterTarget = address
	r.mu.Unlock()

	go r.keepRegistrationAlive(reg)
	return nil
}

// UnRegister removes all registered service endpoints.
func (r *SvcDiscoveryRegistryImpl) UnRegister() error {
	r.mu.Lock()
	regs := make([]*registration, 0, len(r.registrations))
	for serviceName, reg := range r.registrations {
		regs = append(regs, reg)
		delete(r.registrations, serviceName)
	}
	r.serviceKey = ""
	r.endpointMgr = nil
	r.leaseID = 0
	r.mu.Unlock()

	var firstErr error
	for _, reg := range regs {
		if err := r.unregister(reg); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Close releases all resources.
func (r *SvcDiscoveryRegistryImpl) Close() {
	r.mu.Lock()
	connCache := r.connCache
	r.connCache = make(map[string]map[string]*grpc.ClientConn)
	registrations := r.registrations
	r.registrations = make(map[string]*registration)
	watchCancels := r.watchCancels
	r.watchCancels = nil
	r.mu.Unlock()

	for _, cancel := range watchCancels {
		cancel()
	}

	for _, pool := range connCache {
		for _, conn := range pool {
			_ = conn.Close()
		}
	}

	for _, reg := range registrations {
		_ = r.unregister(reg)
	}

	if r.client != nil {
		_ = r.client.Close()
	}
}

// Check verifies etcd status.
func Check(ctx context.Context, etcdServers []string, etcdRoot string, createIfNotExist bool, options ...CfgOption) error {
	cfg := clientv3.Config{
		Endpoints: etcdServers,
	}
	for _, opt := range options {
		opt(&cfg)
	}

	client, err := clientv3.New(cfg)
	if err != nil {
		return errs.WrapMsg(err, "failed to connect to etcd")
	}
	defer client.Close()

	var opCtx context.Context
	var cancel context.CancelFunc
	if cfg.DialTimeout != 0 {
		opCtx, cancel = context.WithTimeout(ctx, cfg.DialTimeout)
	} else {
		opCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
	}
	defer cancel()

	resp, err := client.Get(opCtx, etcdRoot)
	if err != nil {
		return errs.WrapMsg(err, "failed to get the root node from etcd")
	}

	if len(resp.Kvs) == 0 {
		if createIfNotExist {
			var leaseTTL int64 = 10
			var leaseResp *clientv3.LeaseGrantResponse
			if leaseTTL > 0 {
				leaseResp, err = client.Grant(opCtx, leaseTTL)
				if err != nil {
					return errs.WrapMsg(err, "failed to create lease in etcd")
				}
			}
			putOpts := []clientv3.OpOption{}
			if leaseResp != nil {
				putOpts = append(putOpts, clientv3.WithLease(leaseResp.ID))
			}

			if _, err := client.Put(opCtx, etcdRoot, "", putOpts...); err != nil {
				return errs.WrapMsg(err, "failed to create the root node in etcd")
			}
		} else {
			return fmt.Errorf("root node %s does not exist in etcd", etcdRoot)
		}
	}
	return nil
}

// GetClient returns the embedded etcd client.
func (r *SvcDiscoveryRegistryImpl) GetClient() *clientv3.Client {
	return r.client
}

func (r *SvcDiscoveryRegistryImpl) ensureConnections(ctx context.Context, serviceName string, addresses []string, dialOptions []grpc.DialOption) ([]*grpc.ClientConn, error) {
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
		if err := conn.Close(); err != nil {
			log.ZWarn(context.Background(), "failed to close stale conn", err)
		}
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

func (r *SvcDiscoveryRegistryImpl) getDialOptions() []grpc.DialOption {
	r.mu.RLock()
	defer r.mu.RUnlock()
	opts := make([]grpc.DialOption, len(r.dialOptions))
	copy(opts, r.dialOptions)
	return opts
}

func (r *SvcDiscoveryRegistryImpl) resetConnCacheLocked() {
	for service, pool := range r.connCache {
		for _, conn := range pool {
			if err := conn.Close(); err != nil {
				log.ZWarn(context.Background(), "failed to close conn", err, "service", service)
			}
		}
	}
	r.connCache = make(map[string]map[string]*grpc.ClientConn)
	r.rrIndex = make(map[string]int)
}

func (r *SvcDiscoveryRegistryImpl) clearServiceCache(serviceName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if pool, ok := r.connCache[serviceName]; ok {
		for _, conn := range pool {
			if err := conn.Close(); err != nil {
				log.ZWarn(context.Background(), "failed to close conn", err, "service", serviceName)
			}
		}
		delete(r.connCache, serviceName)
	}
	delete(r.rrIndex, serviceName)
}

func (r *SvcDiscoveryRegistryImpl) unregister(reg *registration) error {
	if reg == nil {
		return nil
	}
	reg.stop()
	if reg.manager != nil && reg.key != "" {
		ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
		err := reg.manager.DeleteEndpoint(ctx, reg.key)
		cancel()
		if err != nil {
			return err
		}
	}
	if reg.leaseID != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
		_, err := r.client.Revoke(ctx, reg.leaseID)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *SvcDiscoveryRegistryImpl) keepRegistrationAlive(reg *registration) {
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
				if ok {
					continue
				}
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
			break
		}
	}
}

func (r *SvcDiscoveryRegistryImpl) keepAliveLease(leaseID clientv3.LeaseID) {
	ch, err := r.client.KeepAlive(context.Background(), leaseID)
	if err != nil {
		return
	}
	for range ch {
	}
}

func (r *SvcDiscoveryRegistryImpl) servicePrefix(serviceName string) string {
	service := strings.Trim(serviceName, "/")
	if r.rootDirectory == "" {
		return service
	}
	return r.rootDirectory + "/" + service
}

func (r *SvcDiscoveryRegistryImpl) extractAddress(key string) string {
	parts := strings.Split(strings.Trim(key, "/"), "/")
	if len(parts) == 0 {
		return ""
	}
	addr := parts[len(parts)-1]
	if addr == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return ""
	}
	return addr
}

func (r *SvcDiscoveryRegistryImpl) dialConn(ctx context.Context, addr string, opts []grpc.DialOption) (*grpc.ClientConn, error) {
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

func (r *SvcDiscoveryRegistryImpl) refreshRegistration(reg *registration) error {
	if reg.manager == nil {
		return errors.New("nil endpoint manager")
	}
	select {
	case <-reg.stopChan():
		return context.Canceled
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()

	leaseResp, err := r.client.Grant(ctx, r.leaseTTLSeconds())
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

func (r *SvcDiscoveryRegistryImpl) leaseTTLSeconds() int64 {
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

func (r *SvcDiscoveryRegistryImpl) isRegistryStopped(reg *registration) bool {
	select {
	case <-reg.stopChan():
		return true
	default:
		return false
	}
}

func (r *SvcDiscoveryRegistryImpl) waitForRetryOrStop(stop <-chan struct{}, delay time.Duration) bool {
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

func withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	ctx, cancel := context.WithTimeout(ctx, defaultRequestTimeout)
	return ctx, cancel
}

func (r *SvcDiscoveryRegistryImpl) SetKey(ctx context.Context, key string, data []byte) error {
	if _, err := r.client.Put(ctx, key, string(data)); err != nil {
		return errs.WrapMsg(err, "etcd put err")
	}
	return nil
}

func (r *SvcDiscoveryRegistryImpl) SetWithLease(ctx context.Context, key string, val []byte, ttl int64) error {
	leaseResp, err := r.client.Grant(ctx, ttl)
	if err != nil {
		return errs.Wrap(err)
	}

	if _, err = r.client.Put(context.Background(), key, string(val), clientv3.WithLease(leaseResp.ID)); err != nil {
		return errs.Wrap(err)
	}

	go r.keepAliveLease(leaseResp.ID)
	return nil
}

func (r *SvcDiscoveryRegistryImpl) GetKey(ctx context.Context, key string) ([]byte, error) {
	resp, err := r.client.Get(ctx, key)
	if err != nil {
		return nil, errs.WrapMsg(err, "etcd get err")
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

func (r *SvcDiscoveryRegistryImpl) GetKeyWithPrefix(ctx context.Context, key string) ([][]byte, error) {
	resp, err := r.client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, errs.WrapMsg(err, "etcd get err")
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	result := make([][]byte, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		result = append(result, kv.Value)
	}
	return result, nil
}

func (r *SvcDiscoveryRegistryImpl) DelData(ctx context.Context, key string) error {
	if _, err := r.client.Delete(ctx, key); err != nil {
		return errs.WrapMsg(err, "etcd delete err")
	}
	return nil
}

func (r *SvcDiscoveryRegistryImpl) WatchKey(ctx context.Context, key string, fn discovery.WatchKeyHandler) error {
	watchChan := r.client.Watch(ctx, key)
	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			if event.IsModify() && string(event.Kv.Key) == key {
				if err := fn(&discovery.WatchKey{Value: event.Kv.Value}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *SvcDiscoveryRegistryImpl) watchServiceChanges() {
	if len(r.watchNames) == 0 {
		return
	}
	for _, name := range r.watchNames {
		service := name
		ctx, cancel := context.WithCancel(context.Background())

		r.mu.Lock()
		r.watchCancels = append(r.watchCancels, cancel)
		r.mu.Unlock()

		go func() {
			watchChan := r.client.Watch(ctx, r.servicePrefix(service), clientv3.WithPrefix())
			for range watchChan {
				r.clearServiceCache(service)
			}
		}()
	}
}

func (r *SvcDiscoveryRegistryImpl) checkOpts(opts ...grpc.DialOption) error {
	return nil
}

func withConfigValue(ctx context.Context, key contextKey, value interface{}) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, key, value)
}
