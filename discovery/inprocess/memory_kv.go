package inprocess

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/openimsdk/tools/discovery"
)

type memoryKV struct {
	mu               sync.RWMutex
	items            map[string]memoryEntry
	subscribers      map[uint64]*watchSubscriber
	nextSubscriberID uint64
}

type memoryEntry struct {
	value []byte
	lease *valueLease
}

type valueLease struct {
	cancel context.CancelFunc
}

type watchSubscriber struct {
	prefix string
	cancel context.CancelFunc

	queueMu sync.Mutex
	queue   []*discovery.WatchKey
	notify  chan struct{}
}

// enqueue appends the event and wakes the watcher. The queue is unbounded so a
// stalled handler never blocks writers; it is called with memoryKV.mu held so
// events are queued in the same order the store was mutated.
func (w *watchSubscriber) enqueue(event *discovery.WatchKey) {
	w.queueMu.Lock()
	w.queue = append(w.queue, event)
	w.queueMu.Unlock()
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

func (w *watchSubscriber) take() []*discovery.WatchKey {
	w.queueMu.Lock()
	events := w.queue
	w.queue = nil
	w.queueMu.Unlock()
	return events
}

func (s *memoryKV) SetKey(ctx context.Context, key string, value []byte) error {
	return s.setValue(ctx, key, value, 0)
}

func (s *memoryKV) SetWithLease(ctx context.Context, key string, value []byte, ttl int64) error {
	if ttl <= 0 {
		return fmt.Errorf("inprocess set with lease invalid ttl %d", ttl)
	}
	return s.setValue(ctx, key, value, time.Duration(ttl)*time.Second)
}

func (s *memoryKV) GetKey(ctx context.Context, key string) ([]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.items[key]
	if !ok {
		return nil, nil
	}
	return cloneBytes(entry.value), nil
}

func (s *memoryKV) GetKeyWithPrefix(ctx context.Context, prefix string) ([][]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.items) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(s.items))
	for key := range s.items {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}
	sort.Strings(keys)

	values := make([][]byte, 0, len(keys))
	for _, key := range keys {
		values = append(values, cloneBytes(s.items[key].value))
	}
	return values, nil
}

func (s *memoryKV) DelData(ctx context.Context, key string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	s.deleteKey(key, true)
	return nil
}

func (s *memoryKV) WatchKey(ctx context.Context, prefix string, fn discovery.WatchKeyHandler) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	if fn == nil {
		return fmt.Errorf("watch handler is nil")
	}

	watchCtx, cancel := context.WithCancel(ctx)
	subscriber := &watchSubscriber{
		prefix: prefix,
		cancel: cancel,
		notify: make(chan struct{}, 1),
	}

	id := s.addSubscriber(subscriber)
	defer s.removeSubscriber(id, subscriber)

	for {
		select {
		case <-watchCtx.Done():
			// ctx.Err() when the caller cancelled, nil when the registry closed.
			return ctx.Err()
		case <-subscriber.notify:
			for _, event := range subscriber.take() {
				if err := fn(event); err != nil {
					return err
				}
			}
		}
	}
}

func (s *memoryKV) setValue(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := checkContext(ctx); err != nil {
		return err
	}

	var lease *valueLease
	var leaseCtx context.Context
	if ttl > 0 {
		var cancel context.CancelFunc
		leaseCtx, cancel = context.WithCancel(context.Background())
		lease = &valueLease{cancel: cancel}
	}

	entry := memoryEntry{value: cloneBytes(value), lease: lease}

	s.mu.Lock()
	if s.items == nil {
		s.items = make(map[string]memoryEntry)
	}
	var oldLease *valueLease
	if old, ok := s.items[key]; ok {
		oldLease = old.lease
	}
	s.items[key] = entry
	s.publishWatchEventLocked(key, entry.value, discovery.WatchTypePut)
	s.mu.Unlock()

	if oldLease != nil && oldLease.cancel != nil {
		oldLease.cancel()
	}
	if lease != nil {
		go s.expireAfter(leaseCtx, key, lease, ttl)
	}
	return nil
}

func (s *memoryKV) expireAfter(ctx context.Context, key string, lease *valueLease, ttl time.Duration) {
	timer := time.NewTimer(ttl)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}

	s.deleteIfLeaseMatches(key, lease)
}

func (s *memoryKV) deleteIfLeaseMatches(key string, lease *valueLease) {
	s.mu.Lock()
	entry, ok := s.items[key]
	if ok && entry.lease == lease {
		delete(s.items, key)
		s.publishWatchEventLocked(key, nil, discovery.WatchTypeDelete)
	}
	s.mu.Unlock()
}

func (s *memoryKV) deleteKey(key string, notify bool) {
	s.mu.Lock()
	entry, ok := s.items[key]
	if ok {
		delete(s.items, key)
		if notify {
			s.publishWatchEventLocked(key, nil, discovery.WatchTypeDelete)
		}
	}
	s.mu.Unlock()

	if ok && entry.lease != nil && entry.lease.cancel != nil {
		entry.lease.cancel()
	}
}

func (s *memoryKV) addSubscriber(subscriber *watchSubscriber) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subscribers == nil {
		s.subscribers = make(map[uint64]*watchSubscriber)
	}
	s.nextSubscriberID++
	id := s.nextSubscriberID
	s.subscribers[id] = subscriber
	return id
}

func (s *memoryKV) removeSubscriber(id uint64, subscriber *watchSubscriber) {
	subscriber.cancel()

	s.mu.Lock()
	if current, ok := s.subscribers[id]; ok && current == subscriber {
		delete(s.subscribers, id)
	}
	s.mu.Unlock()
}

// publishWatchEventLocked queues the event for matching subscribers. The caller
// must hold s.mu (write lock): queuing inside the same critical section as the
// map mutation is what guarantees subscribers see events in mutation order.
func (s *memoryKV) publishWatchEventLocked(key string, value []byte, typ discovery.WatchType) {
	for _, subscriber := range s.subscribers {
		if strings.HasPrefix(key, subscriber.prefix) {
			subscriber.enqueue(&discovery.WatchKey{
				Key:   []byte(key),
				Value: cloneBytes(value),
				Type:  typ,
			})
		}
	}
}

func (s *memoryKV) close() {
	s.mu.Lock()
	items := make([]memoryEntry, 0, len(s.items))
	for _, entry := range s.items {
		items = append(items, entry)
	}
	subscribers := make([]*watchSubscriber, 0, len(s.subscribers))
	for _, subscriber := range s.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	s.items = nil
	s.subscribers = nil
	s.mu.Unlock()

	for _, entry := range items {
		if entry.lease != nil && entry.lease.cancel != nil {
			entry.lease.cancel()
		}
	}
	for _, subscriber := range subscribers {
		subscriber.cancel()
	}
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	out := make([]byte, len(value))
	copy(out, value)
	return out
}
