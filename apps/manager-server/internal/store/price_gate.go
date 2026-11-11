package store

import (
	"container/list"
	"context"
	"sync"
)

// priceGate is a cancelable FIFO reader/writer gate. Adjacent readers may run
// together; readers cannot bypass a queued writer, nor can later writers starve
// queued readers. Waiters use the calling goroutine, never a lock-wait helper.
// The zero value is ready for use.
type priceGate struct {
	mu      sync.Mutex
	queue   list.List
	readers int
	writer  bool
}

type priceWaiter struct {
	write   bool
	ready   chan struct{}
	granted bool
}

func (g *priceGate) advanceLocked() {
	for head := g.queue.Front(); head != nil && !g.writer; head = g.queue.Front() {
		waiter := head.Value.(*priceWaiter)
		if waiter.write && g.readers > 0 {
			return
		}
		g.queue.Remove(head)
		waiter.granted = true
		if waiter.write {
			g.writer = true
		} else {
			g.readers++
		}
		close(waiter.ready)
	}
}

func (g *priceGate) releaseLocked(write bool) {
	if write {
		g.writer = false
	} else {
		g.readers--
	}
	g.advanceLocked()
}

func (g *priceGate) acquire(ctx context.Context, write bool) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	waiter := &priceWaiter{write: write, ready: make(chan struct{})}
	g.mu.Lock()
	element := g.queue.PushBack(waiter)
	g.advanceLocked()
	g.mu.Unlock()

	select {
	case <-ctx.Done():
	case <-waiter.ready:
	}
	g.mu.Lock()
	if err := ctx.Err(); err != nil {
		if waiter.granted {
			g.releaseLocked(write)
		} else {
			g.queue.Remove(element)
			g.advanceLocked()
		}
		g.mu.Unlock()
		return nil, err
	}
	g.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			g.releaseLocked(write)
			g.mu.Unlock()
		})
	}, nil
}
