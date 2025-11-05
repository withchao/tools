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
	"go.etcd.io/etcd/api/v3/mvccpb"
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

type serviceEntry struct {
	mu        sync.RWMutex
	addresses map[string]struct{}

	ready     chan struct{}
	readyOnce sync.Once

	updateCh chan struct{}

	initOnce sync.Once

	cancelMu sync.Mutex
	cancel   context.CancelFunc
}

func newServiceEntry() *serviceEntry {
	return &serviceEntry{
		addresses: make(map[string]struct{}),
		ready:     make(chan struct{}),
		updateCh:  make(chan struct{}),
	}
}

func (e *serviceEntry) markReady() {
	e.readyOnce.Do(func() {
		close(e.ready)
	})
}

func (e *serviceEntry) waitReady(ctx context.Context) error {
	select {
	case <-e.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *serviceEntry) setAddresses(addrs []string) {
	m := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		m[addr] = struct{}{}
	}
	e.mu.Lock()
	e.addresses = m
	e.mu.Unlock()
	e.notify()
}

func (e *serviceEntry) addAddress(addr string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.addresses == nil {
		e.addresses = make(map[string]struct{})
	}
	if _, ok := e.addresses[addr]; ok {
		return false
	}
	e.addresses[addr] = struct{}{}
	return true
}

func (e *serviceEntry) removeAddress(addr string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.addresses == nil {
		return false
	}
	if _, ok := e.addresses[addr]; !ok {
		return false
	}
	delete(e.addresses, addr)
	return true
}

func (e *serviceEntry) addressesList() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]string, 0, len(e.addresses))
	for addr := range e.addresses {
		result = append(result, addr)
	}
	return result
}

func (e *serviceEntry) notify() {
	e.mu.Lock()
	old := e.updateCh
	e.updateCh = make(chan struct{})
	e.mu.Unlock()
	close(old)
}

func (e *serviceEntry) subscribe() <-chan struct{} {
	e.mu.RLock()
	ch := e.updateCh
	e.mu.RUnlock()
	return ch
}

func (e *serviceEntry) setCancel(cancel context.CancelFunc) {
	e.cancelMu.Lock()
	e.cancel = cancel
	e.cancelMu.Unlock()
}

func (e *serviceEntry) stop() {
	e.cancelMu.Lock()
	cancel := e.cancel
	e.cancel = nil
	e.cancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
	e.notify()
}

type keyEntry struct {
	mu     sync.RWMutex
	value  []byte
	exists bool

	ready     chan struct{}
	readyOnce sync.Once
	initOnce  sync.Once

	updateCh chan struct{}

	cancelMu sync.Mutex
	cancel   context.CancelFunc

	watchersMu sync.Mutex
	watchers   map[chan *discovery.WatchKey]struct{}
}

func newKeyEntry() *keyEntry {
	return &keyEntry{
		ready:    make(chan struct{}),
		updateCh: make(chan struct{}),
		watchers: make(map[chan *discovery.WatchKey]struct{}),
	}
}

func (e *keyEntry) markReady() {
	e.readyOnce.Do(func() {
		close(e.ready)
	})
}

func (e *keyEntry) waitReady(ctx context.Context) error {
	select {
	case <-e.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *keyEntry) setValue(val []byte, exists bool) {
	e.mu.Lock()
	if exists && val != nil {
		e.value = cloneBytes(val)
	} else {
		e.value = nil
	}
	e.exists = exists
	e.mu.Unlock()
	e.notify()
	e.broadcast(val, exists)
}

func (e *keyEntry) getValue() ([]byte, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.exists {
		return nil, false
	}
	return cloneBytes(e.value), true
}

func (e *keyEntry) notify() {
	e.mu.Lock()
	old := e.updateCh
	e.updateCh = make(chan struct{})
	e.mu.Unlock()
	close(old)
}

func (e *keyEntry) subscribeUpdates() <-chan struct{} {
	e.mu.RLock()
	ch := e.updateCh
	e.mu.RUnlock()
	return ch
}

func (e *keyEntry) subscribeWatcher() (<-chan *discovery.WatchKey, func()) {
	ch := make(chan *discovery.WatchKey, 1)
	e.watchersMu.Lock()
	e.watchers[ch] = struct{}{}
	e.watchersMu.Unlock()
	return ch, func() {
		e.watchersMu.Lock()
		if _, ok := e.watchers[ch]; ok {
			delete(e.watchers, ch)
			close(ch)
		}
		e.watchersMu.Unlock()
	}
}

func (e *keyEntry) broadcast(val []byte, exists bool) {
	e.watchersMu.Lock()
	defer e.watchersMu.Unlock()
	if len(e.watchers) == 0 {
		return
	}
	for ch := range e.watchers {
		var payload *discovery.WatchKey
		if exists && val != nil {
			payload = &discovery.WatchKey{Value: cloneBytes(val)}
		} else {
			payload = &discovery.WatchKey{Value: nil}
		}
		select {
		case ch <- payload:
		default:
		}
	}
}

func (e *keyEntry) setCancel(cancel context.CancelFunc) {
	e.cancelMu.Lock()
	e.cancel = cancel
	e.cancelMu.Unlock()
}

func (e *keyEntry) stop() {
	e.cancelMu.Lock()
	cancel := e.cancel
	e.cancel = nil
	e.cancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
	e.notify()
	e.watchersMu.Lock()
	for ch := range e.watchers {
		close(ch)
	}
	e.watchers = make(map[chan *discovery.WatchKey]struct{})
	e.watchersMu.Unlock()
}

type prefixEntry struct {
	mu     sync.RWMutex
	values map[string][]byte

	ready     chan struct{}
	readyOnce sync.Once
	initOnce  sync.Once

	cancelMu sync.Mutex
	cancel   context.CancelFunc

	watchersMu sync.Mutex
	watchers   map[chan *discovery.WatchKey]struct{}
}

func newPrefixEntry() *prefixEntry {
	return &prefixEntry{
		values:   make(map[string][]byte),
		ready:    make(chan struct{}),
		watchers: make(map[chan *discovery.WatchKey]struct{}),
	}
}

func (e *prefixEntry) markReady() {
	e.readyOnce.Do(func() {
		close(e.ready)
	})
}

func (e *prefixEntry) waitReady(ctx context.Context) error {
	select {
	case <-e.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *prefixEntry) setValues(values map[string][]byte) {
	e.mu.Lock()
	e.values = make(map[string][]byte, len(values))
	for k, v := range values {
		e.values[k] = cloneBytes(v)
	}
	e.mu.Unlock()
}

func (e *prefixEntry) upsert(key string, val []byte) {
	e.mu.Lock()
	if e.values == nil {
		e.values = make(map[string][]byte)
	}
	e.values[key] = cloneBytes(val)
	e.mu.Unlock()
}

func (e *prefixEntry) remove(key string) {
	e.mu.Lock()
	if e.values != nil {
		delete(e.values, key)
	}
	e.mu.Unlock()
}

func (e *prefixEntry) valuesList() [][]byte {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.values) == 0 {
		return nil
	}
	result := make([][]byte, 0, len(e.values))
	for _, v := range e.values {
		result = append(result, cloneBytes(v))
	}
	return result
}

func (e *prefixEntry) snapshot() map[string][]byte {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.values) == 0 {
		return nil
	}
	result := make(map[string][]byte, len(e.values))
	for k, v := range e.values {
		result[k] = cloneBytes(v)
	}
	return result
}

func (e *prefixEntry) setCancel(cancel context.CancelFunc) {
	e.cancelMu.Lock()
	e.cancel = cancel
	e.cancelMu.Unlock()
}

func (e *prefixEntry) stop() {
	e.cancelMu.Lock()
	cancel := e.cancel
	e.cancel = nil
	e.cancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
	e.watchersMu.Lock()
	for ch := range e.watchers {
		close(ch)
	}
	e.watchers = make(map[chan *discovery.WatchKey]struct{})
	e.watchersMu.Unlock()
}

func (e *prefixEntry) subscribeWatcher() (<-chan *discovery.WatchKey, func()) {
	ch := make(chan *discovery.WatchKey, 1)
	e.watchersMu.Lock()
	e.watchers[ch] = struct{}{}
	e.watchersMu.Unlock()
	return ch, func() {
		e.watchersMu.Lock()
		if _, ok := e.watchers[ch]; ok {
			delete(e.watchers, ch)
			close(ch)
		}
		e.watchersMu.Unlock()
	}
}

func (e *prefixEntry) broadcast(key string, val []byte, t discovery.WatchType) {
	e.watchersMu.Lock()
	defer e.watchersMu.Unlock()
	if len(e.watchers) == 0 {
		return
	}
	var payload *discovery.WatchKey
	if t == discovery.WatchTypeDelete {
		payload = &discovery.WatchKey{Key: []byte(key), Value: nil, Type: t}
	} else {
		payload = &discovery.WatchKey{Key: []byte(key), Value: cloneBytes(val), Type: t}
	}
	for ch := range e.watchers {
		select {
		case ch <- payload:
		default:
		}
	}
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

	serviceEntries map[string]*serviceEntry
	keyEntries     map[string]*keyEntry
	prefixEntries  map[string]*prefixEntry

	serviceKey        string
	endpointMgr       endpoints.Manager
	leaseID           clientv3.LeaseID
	rpcRegisterTarget string
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
		connCache:      make(map[string]map[string]*grpc.ClientConn),
		rrIndex:        make(map[string]int),
		registrations:  make(map[string]*registration),
		serviceEntries: make(map[string]*serviceEntry),
		keyEntries:     make(map[string]*keyEntry),
		prefixEntries:  make(map[string]*prefixEntry),
	}

	for _, name := range s.watchNames {
		if _, err := s.ensureServiceEntry(context.Background(), name); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			log.ZWarn(context.Background(), "preload service entry failed", err, "service", name)
		}
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

// GetConns returns all connections for a service.
func (r *SvcDiscoveryRegistryImpl) GetConns(ctx context.Context, serviceName string, opts ...grpc.DialOption) ([]grpc.ClientConnInterface, error) {

	entry, err := r.ensureServiceEntry(ctx, serviceName)
	if err != nil {
		return nil, err
	}

	waitDeadline := time.Time{}
	if r.initialResolveWait > 0 {
		waitDeadline = time.Now().Add(r.initialResolveWait)
	}

	dialOptions := append(r.getDialOptions(), opts...)

	for {
		addresses := entry.addressesList()
		if len(addresses) == 0 {
			if !waitDeadline.IsZero() && time.Now().Before(waitDeadline) {
				ch := entry.subscribe()
				addresses = entry.addressesList()
				if len(addresses) > 0 {
					continue
				}
				if err := waitForEntryUpdate(ctx, ch, waitDeadline); err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						return nil, fmt.Errorf("%w: service=%s", errNoEndpoints, serviceName)
					}
					return nil, err
				}
				continue
			}
			return nil, fmt.Errorf("%w: service=%s", errNoEndpoints, serviceName)
		}

		sort.Strings(addresses)

		conns, err := r.ensureConnections(ctx, serviceName, addresses, dialOptions)
		if err != nil {
			return nil, err
		}

		if len(conns) == 0 {
			return nil, fmt.Errorf("%w: service=%s", errNoEndpoints, serviceName)
		}

		result := make([]grpc.ClientConnInterface, len(conns))
		for i := range conns {
			result[i] = conns[i]
		}
		return result, nil
	}
}

func (r *SvcDiscoveryRegistryImpl) ensureServiceEntry(ctx context.Context, serviceName string) (*serviceEntry, error) {
	r.mu.Lock()
	entry, ok := r.serviceEntries[serviceName]
	if !ok {
		entry = newServiceEntry()
		r.serviceEntries[serviceName] = entry
	}
	r.mu.Unlock()

	entry.initOnce.Do(func() {
		if err := r.loadInitialAddresses(serviceName, entry); err != nil {
			log.ZWarn(context.Background(), "load initial endpoints failed", err, "service", serviceName)
		}
		entry.markReady()
		entry.setCancel(r.startServiceWatcher(serviceName, entry))
	})

	if err := entry.waitReady(ctx); err != nil {
		return entry, err
	}
	return entry, nil
}

func (r *SvcDiscoveryRegistryImpl) loadInitialAddresses(serviceName string, entry *serviceEntry) error {
	ctx, cancel := withRequestTimeout(context.Background())
	defer cancel()

	prefix := r.servicePrefix(serviceName)
	resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		entry.setAddresses(nil)
		r.syncConnCache(serviceName, nil)
		return err
	}

	addresses := r.parseAddresses(resp.Kvs)
	entry.setAddresses(addresses)
	r.syncConnCache(serviceName, addresses)
	return nil
}

func (r *SvcDiscoveryRegistryImpl) startServiceWatcher(serviceName string, entry *serviceEntry) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer entry.notify()
		prefix := r.servicePrefix(serviceName)
		for {
			if ctx.Err() != nil {
				return
			}
			watchChan := r.client.Watch(ctx, prefix, clientv3.WithPrefix())
			for watchResp := range watchChan {
				if watchResp.Canceled {
					if ctx.Err() != nil {
						return
					}
					break
				}
				changed := false
				for _, event := range watchResp.Events {
					addr := r.extractAddress(string(event.Kv.Key))
					if addr == "" {
						continue
					}
					switch event.Type {
					case mvccpb.PUT:
						if entry.addAddress(addr) {
							changed = true
						}
					case mvccpb.DELETE:
						if entry.removeAddress(addr) {
							changed = true
						}
					}
				}
				if changed {
					entry.notify()
					r.syncConnCache(serviceName, entry.addressesList())
				}
			}
			if ctx.Err() != nil {
				return
			}
			time.Sleep(time.Second)
		}
	}()
	return cancel
}

func (r *SvcDiscoveryRegistryImpl) ensureKeyEntry(ctx context.Context, key string) (*keyEntry, error) {
	r.mu.Lock()
	entry, ok := r.keyEntries[key]
	if !ok {
		entry = newKeyEntry()
		r.keyEntries[key] = entry
	}
	r.mu.Unlock()

	entry.initOnce.Do(func() {
		if err := r.loadKeyValue(key, entry); err != nil {
			log.ZWarn(context.Background(), "load key value failed", err, "key", key)
		}
		entry.markReady()
		entry.setCancel(r.startKeyWatcher(key, entry))
	})

	if err := entry.waitReady(ctx); err != nil {
		return entry, err
	}
	return entry, nil
}

func (r *SvcDiscoveryRegistryImpl) loadKeyValue(key string, entry *keyEntry) error {
	ctx, cancel := withRequestTimeout(context.Background())
	defer cancel()

	resp, err := r.client.Get(ctx, key)
	if err != nil {
		entry.setValue(nil, false)
		return err
	}
	if len(resp.Kvs) == 0 {
		entry.setValue(nil, false)
		return nil
	}
	entry.setValue(resp.Kvs[0].Value, true)
	return nil
}

func (r *SvcDiscoveryRegistryImpl) startKeyWatcher(key string, entry *keyEntry) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer entry.notify()
		for {
			if ctx.Err() != nil {
				return
			}
			watchChan := r.client.Watch(ctx, key, clientv3.WithPrefix())
			for watchResp := range watchChan {
				if watchResp.Canceled {
					if ctx.Err() != nil {
						return
					}
					break
				}
				for _, event := range watchResp.Events {
					switch event.Type {
					case mvccpb.PUT:
						entry.setValue(event.Kv.Value, true)
					case mvccpb.DELETE:
						entry.setValue(nil, false)
					}
				}
			}
			if ctx.Err() != nil {
				return
			}
			time.Sleep(time.Second)
		}
	}()
	return cancel
}

func (r *SvcDiscoveryRegistryImpl) ensurePrefixEntry(ctx context.Context, prefix string) (*prefixEntry, error) {
	r.mu.Lock()
	entry, ok := r.prefixEntries[prefix]
	if !ok {
		entry = newPrefixEntry()
		r.prefixEntries[prefix] = entry
	}
	r.mu.Unlock()

	entry.initOnce.Do(func() {
		if err := r.loadPrefixValues(prefix, entry); err != nil {
			log.ZWarn(context.Background(), "load prefix values failed", err, "prefix", prefix)
		}
		entry.markReady()
		entry.setCancel(r.startPrefixWatcher(prefix, entry))
	})

	if err := entry.waitReady(ctx); err != nil {
		return entry, err
	}
	return entry, nil
}

func (r *SvcDiscoveryRegistryImpl) loadPrefixValues(prefix string, entry *prefixEntry) error {
	ctx, cancel := withRequestTimeout(context.Background())
	defer cancel()

	resp, err := r.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		entry.setValues(nil)
		return err
	}
	values := make(map[string][]byte, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		values[string(kv.Key)] = cloneBytes(kv.Value)
	}
	entry.setValues(values)
	return nil
}

func (r *SvcDiscoveryRegistryImpl) startPrefixWatcher(prefix string, entry *prefixEntry) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			watchChan := r.client.Watch(ctx, prefix, clientv3.WithPrefix())
			for watchResp := range watchChan {
				if watchResp.Canceled {
					if ctx.Err() != nil {
						return
					}
					break
				}
				for _, event := range watchResp.Events {
					key := string(event.Kv.Key)
					switch event.Type {
					case mvccpb.PUT:
						entry.upsert(key, event.Kv.Value)
						entry.broadcast(key, event.Kv.Value, discovery.WatchTypePut)
					case mvccpb.DELETE:
						entry.remove(key)
						entry.broadcast(key, nil, discovery.WatchTypeDelete)
					}
				}
			}
			if ctx.Err() != nil {
				return
			}
			time.Sleep(time.Second)
		}
	}()
	return cancel
}

func (r *SvcDiscoveryRegistryImpl) parseAddresses(kvs []*mvccpb.KeyValue) []string {
	if len(kvs) == 0 {
		return nil
	}
	temp := make(map[string]struct{}, len(kvs))
	for _, kv := range kvs {
		addr := r.extractAddress(string(kv.Key))
		if addr == "" {
			continue
		}
		temp[addr] = struct{}{}
	}
	if len(temp) == 0 {
		return nil
	}
	result := make([]string, 0, len(temp))
	for addr := range temp {
		result = append(result, addr)
	}
	return result
}

func (r *SvcDiscoveryRegistryImpl) syncConnCache(serviceName string, validAddrs []string) {
	valid := make(map[string]struct{}, len(validAddrs))
	for _, addr := range validAddrs {
		valid[addr] = struct{}{}
	}

	var toClose []*grpc.ClientConn

	r.mu.Lock()
	if pool, ok := r.connCache[serviceName]; ok {
		for addr, conn := range pool {
			if _, ok := valid[addr]; !ok {
				toClose = append(toClose, conn)
				delete(pool, addr)
			}
		}
		if len(pool) == 0 {
			delete(r.connCache, serviceName)
			delete(r.rrIndex, serviceName)
		}
	}
	r.mu.Unlock()

	for _, conn := range toClose {
		if conn != nil {
			_ = conn.Close()
		}
	}
}

func (r *SvcDiscoveryRegistryImpl) updateLocalKeyValue(key string, value []byte, exists bool) {
	r.mu.RLock()
	keyEntry := r.keyEntries[key]
	prefixEntries := make([]*prefixEntry, 0)
	for prefix, entry := range r.prefixEntries {
		if strings.HasPrefix(key, prefix) {
			prefixEntries = append(prefixEntries, entry)
		}
	}
	r.mu.RUnlock()

	if keyEntry != nil {
		keyEntry.setValue(value, exists)
	}

	for _, entry := range prefixEntries {
		if exists {
			entry.upsert(key, value)
			entry.broadcast(key, value, discovery.WatchTypePut)
		} else {
			entry.remove(key)
			entry.broadcast(key, nil, discovery.WatchTypeDelete)
		}
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
	entries := make([]*serviceEntry, 0, len(r.serviceEntries))
	for _, entry := range r.serviceEntries {
		entries = append(entries, entry)
	}
	r.serviceEntries = make(map[string]*serviceEntry)
	keyEntries := make([]*keyEntry, 0, len(r.keyEntries))
	for _, entry := range r.keyEntries {
		keyEntries = append(keyEntries, entry)
	}
	r.keyEntries = make(map[string]*keyEntry)
	prefixEntries := make([]*prefixEntry, 0, len(r.prefixEntries))
	for _, entry := range r.prefixEntries {
		prefixEntries = append(prefixEntries, entry)
	}
	r.prefixEntries = make(map[string]*prefixEntry)
	r.mu.Unlock()

	for _, entry := range entries {
		if entry != nil {
			entry.stop()
		}
	}
	for _, entry := range keyEntries {
		if entry != nil {
			entry.stop()
		}
	}
	for _, entry := range prefixEntries {
		if entry != nil {
			entry.stop()
		}
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

func waitForEntryUpdate(ctx context.Context, ch <-chan struct{}, deadline time.Time) error {
	if deadline.IsZero() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			return nil
		}
	}

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
			return context.DeadlineExceeded
		case <-ch:
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		}
	}
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, defaultRequestTimeout)
	return ctx, cancel
}

func (r *SvcDiscoveryRegistryImpl) SetKey(ctx context.Context, key string, data []byte) error {
	if _, err := r.client.Put(ctx, key, string(data)); err != nil {
		return errs.WrapMsg(err, "etcd put err")
	}
	r.updateLocalKeyValue(key, data, true)
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

	r.updateLocalKeyValue(key, val, true)
	go r.keepAliveLease(leaseResp.ID)
	return nil
}

func (r *SvcDiscoveryRegistryImpl) GetKey(ctx context.Context, key string) ([]byte, error) {
	entry, err := r.ensureKeyEntry(ctx, key)
	if err != nil {
		return nil, err
	}
	value, ok := entry.getValue()
	if !ok {
		return nil, nil
	}
	return value, nil
}

func (r *SvcDiscoveryRegistryImpl) GetKeyWithPrefix(ctx context.Context, prefix string) ([][]byte, error) {
	entry, err := r.ensurePrefixEntry(ctx, prefix)
	if err != nil {
		return nil, err
	}
	return entry.valuesList(), nil
}

func (r *SvcDiscoveryRegistryImpl) DelData(ctx context.Context, key string) error {
	if _, err := r.client.Delete(ctx, key); err != nil {
		return errs.WrapMsg(err, "etcd delete err")
	}
	r.updateLocalKeyValue(key, nil, false)
	return nil
}

func (r *SvcDiscoveryRegistryImpl) WatchKey(ctx context.Context, name string, fn discovery.WatchKeyHandler) error {
	prefix := r.servicePrefix(name)
	entry, err := r.ensurePrefixEntry(ctx, prefix)
	if err != nil {
		return err
	}

	// deliver current snapshot first
	for k, v := range entry.snapshot() {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		data := &discovery.WatchKey{Key: []byte(k), Value: cloneBytes(v), Type: discovery.WatchTypePut}
		if err := fn(data); err != nil {
			return err
		}
	}

	updates, cancel := entry.subscribeWatcher()
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, ok := <-updates:
			if !ok {
				return nil
			}
			if data == nil {
				continue
			}
			keyBytes := cloneBytes(data.Key)
			keyStr := string(keyBytes)
			if !strings.HasPrefix(keyStr, prefix) {
				continue
			}
			var valueCopy []byte
			if data.Value != nil {
				valueCopy = cloneBytes(data.Value)
			}
			if err := fn(&discovery.WatchKey{Key: keyBytes, Value: valueCopy, Type: data.Type}); err != nil {
				return err
			}
		}
	}
}

func withConfigValue(ctx context.Context, key contextKey, value interface{}) context.Context {
	return context.WithValue(ctx, key, value)
}
