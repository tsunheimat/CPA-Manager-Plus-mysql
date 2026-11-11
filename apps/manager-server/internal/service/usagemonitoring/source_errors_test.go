package usagemonitoring

import (
	"context"
	"errors"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
)

func TestDerivedReaderSeparatesUnavailableAndTerminalErrors(t *testing.T) {
	r := New(&store.Store{})
	for _, want := range []error{context.Canceled, context.DeadlineExceeded, store.ErrUsagePricingCoverageIncomplete, errors.New("SQL failure")} {
		rows, available, err := readDerived(context.Background(), r, "account_stats", true, func() (int, store.UsageMonitoringState, bool, error) {
			return 1, store.UsageMonitoringState{}, false, want
		})
		if rows != 0 || available || !errors.Is(err, want) {
			t.Fatalf("rows=%d available=%v err=%v", rows, available, err)
		}
	}
	rows, available, err := readDerived(context.Background(), r, "account_stats", true, func() (int, store.UsageMonitoringState, bool, error) {
		return 1, store.UsageMonitoringState{}, false, nil
	})
	if rows != 0 || available || err != nil {
		t.Fatalf("unavailable rows=%d available=%v err=%v", rows, available, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	_, _, err = readDerived(ctx, r, "account_stats", false, func() (int, store.UsageMonitoringState, bool, error) {
		called = true
		return 0, store.UsageMonitoringState{}, true, nil
	})
	if !errors.Is(err, context.Canceled) || called {
		t.Fatal("canceled unsupported read permitted raw fallback")
	}
}
