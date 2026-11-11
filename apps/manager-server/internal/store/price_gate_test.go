package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func awaitPriceQueue(t *testing.T, gate *priceGate, size int) {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		gate.mu.Lock()
		queued := gate.queue.Len()
		gate.mu.Unlock()
		if queued == size {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("queued=%d, want %d", queued, size)
		case <-ticker.C:
		}
	}
}

func TestPriceGateCancellationAndFIFO(t *testing.T) {
	var gate priceGate
	ctx := context.Background()
	releaseRead, err := gate.acquire(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseRead()
	writerEntered := make(chan func(), 1)
	go func() { release, _ := gate.acquire(ctx, true); writerEntered <- release }()
	awaitPriceQueue(t, &gate, 1)
	readerEntered := make(chan func(), 1)
	go func() { release, _ := gate.acquire(ctx, false); readerEntered <- release }()
	awaitPriceQueue(t, &gate, 2)
	laterWriter := make(chan func(), 1)
	go func() { release, _ := gate.acquire(ctx, true); laterWriter <- release }()
	awaitPriceQueue(t, &gate, 3)

	releaseRead()
	releaseWriter := <-writerEntered
	select {
	case release := <-readerEntered:
		release()
		t.Fatal("reader passed writer")
	default:
	}
	releaseWriter()
	releaseReader := <-readerEntered
	select {
	case release := <-laterWriter:
		release()
		t.Fatal("later writer bypassed reader")
	default:
	}
	releaseReader()
	(<-laterWriter)()
	awaitPriceQueue(t, &gate, 0)
}

func TestPriceGateCanceledWaitersReleaseAndDoNotRun(t *testing.T) {
	for _, write := range []bool{false, true} {
		var gate priceGate
		release, err := gate.acquire(context.Background(), true)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		acquired, err := gate.acquire(ctx, write)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) || acquired != nil {
			t.Fatalf("acquire = %v, %v", acquired != nil, err)
		}
		awaitPriceQueue(t, &gate, 0)
		release()
		release() // Completion is idempotent.
		acquired, err = gate.acquire(context.Background(), write)
		if err != nil {
			t.Fatal(err)
		}
		acquired()
	}
}

func TestModelPriceSnapshotContextHonorsGateDeadline(t *testing.T) {
	st := &Store{}
	release, err := st.modelPricesMu.acquire(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	called := false
	err = st.WithModelPriceSnapshotContext(ctx, func() error { called = true; return nil })
	if !errors.Is(err, context.DeadlineExceeded) || called {
		t.Fatalf("err=%v called=%v", err, called)
	}
	awaitPriceQueue(t, &st.modelPricesMu, 0)
}
