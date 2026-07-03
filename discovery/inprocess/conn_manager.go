package inprocess

import (
	"context"
	"strings"
	"sync"

	"google.golang.org/grpc"
)

func newConnManager() *connManager {
	manager := &connManager{
		serviceTargets: make(map[string]string),
	}
	manager.local = newLocalConn(manager.Target)
	return manager
}

type connManager struct {
	local *localConn

	mu                sync.RWMutex
	localTarget       string
	serviceTargets    map[string]string
	broadcastSecret   string
	broadcastProvider BroadcastAddressProvider
}

func (m *connManager) setLocalTarget(target string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.localTarget = strings.TrimSpace(target)
}

func (m *connManager) setBroadcastAddressProvider(secret string, provider BroadcastAddressProvider) {
	m.mu.Lock()
	m.broadcastSecret = secret
	m.broadcastProvider = provider
	m.mu.Unlock()
}

func (m *connManager) broadcastConfig() (string, BroadcastAddressProvider) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.broadcastSecret, m.broadcastProvider
}

func (m *connManager) Target() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.localTarget
}

func (m *connManager) registerLocalTarget(ctx context.Context, serviceName, target string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	m.mu.Lock()
	if m.serviceTargets == nil {
		m.serviceTargets = make(map[string]string)
	}
	m.serviceTargets[serviceName] = target
	if m.localTarget == "" {
		m.localTarget = target
	}
	m.mu.Unlock()
	return nil
}

func (m *connManager) serviceTarget(serviceName string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if target := m.serviceTargets[serviceName]; target != "" {
		return target
	}
	return m.localTarget
}

func (m *connManager) close() {
	m.mu.Lock()
	m.localTarget = ""
	m.serviceTargets = make(map[string]string)
	m.broadcastSecret = ""
	m.broadcastProvider = nil
	m.mu.Unlock()
}

// GetConn returns the in-process conn: single-conn calls always run locally.
func (m *connManager) GetConn(context.Context, string, ...grpc.DialOption) (grpc.ClientConnInterface, error) {
	return m.local, nil
}

// GetConns returns the local in-memory conn plus one remote conn per address
// reported by the broadcast callback (other in-process-mode instances' http api).
// Without a callback it behaves as before: local only.
func (m *connManager) GetConns(ctx context.Context, serviceName string, _ ...grpc.DialOption) ([]grpc.ClientConnInterface, error) {
	secret, provider := m.broadcastConfig()
	if provider == nil {
		return []grpc.ClientConnInterface{m.local}, nil
	}
	addrs, err := provider(ctx)
	if err != nil {
		return nil, err
	}
	conns := make([]grpc.ClientConnInterface, 0, len(addrs)+1)
	conns = append(conns, m.local)
	// When services bypass startrpc, Register is never called, so
	// serviceTarget falls back to the SetLocalTarget address. The provider already
	// excludes this instance; this filter is only defense in depth.
	self := m.serviceTarget(serviceName)
	seen := make(map[string]struct{}, len(addrs)+1)
	if self != "" {
		seen[self] = struct{}{}
	}
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		conns = append(conns, newRemoteClientConn(addr, serviceName, secret))
	}
	return conns, nil
}

func (m *connManager) IsSelfNode(cc grpc.ClientConnInterface) bool {
	return cc == m.local
}
