package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usagemonitoring"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usagepricing"
)

type pricingBacklogFixture struct{ usagepricing.Repository }

func (*pricingBacklogFixture) CatchUp(context.Context, int, int64) (usagepricing.CatchUpResult, error) {
	return usagepricing.CatchUpResult{Processed: 1, Pending: true, Rebuilt: true, CoverageEventID: 1, TargetEventID: 1000000}, nil
}

type projectionRetryFixture struct {
	usagemonitoring.Repository
	attempts int
}

func (r *projectionRetryFixture) CatchUpProjection(ctx context.Context, limit int, nowMS int64) (usagemonitoring.CatchUpResult, error) {
	r.attempts++
	if r.attempts == 1 {
		return usagemonitoring.CatchUpResult{}, errors.New("transient fixture failure")
	}
	return r.Repository.CatchUpProjection(ctx, limit, nowMS)
}

func TestUsagePricingWorkerRetriesFailedTasksDuringBacklogAtRescan(t *testing.T) {
	st := newUsagePricingRollupWorkerStore(t)
	st.UsagePricing = &pricingBacklogFixture{Repository: st.UsagePricing}
	probe := &projectionRetryFixture{Repository: st.UsageMonitoring}
	st.UsageMonitoring = probe
	worker := NewUsagePricingRollupWorker(st)
	worker.maxBatches = 10
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if !worker.catchUp(ctx) {
			t.Fatal("fixture backlog unexpectedly completed")
		}
	}
	if probe.attempts != 1 {
		t.Fatal("unbounded retry during continuation", probe.attempts)
	}
	worker.nextRescan = time.Time{} // Scheduled check elapsed, without ingestion/Wake.
	if !worker.catchUp(ctx) || probe.attempts != 2 {
		t.Fatal("periodic rescan starved failed task", probe.attempts)
	}
}
