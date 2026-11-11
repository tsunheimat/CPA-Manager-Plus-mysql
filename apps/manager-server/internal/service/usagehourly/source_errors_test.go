package usagehourly

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageevent"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

type failedOffsetTimelineRepository struct {
	usageevent.Repository
	err error
}

func (r *failedOffsetTimelineRepository) HourlyTimelineBetween(context.Context, int64, int64) ([]usageevent.TimelinePoint, error) {
	return nil, r.err
}

func TestHourlyReaderPropagatesCancellationBeforeUnavailableFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader := New(nil, false)
	for _, read := range []func() (Snapshot, bool){
		func() (Snapshot, bool) { return reader.Load(ctx, 1, 2) },
		func() (Snapshot, bool) {
			return reader.LoadAnalytics(ctx, store.AnalyticsFilter{SearchQuery: "unsupported"}, "hour", time.UTC, false)
		},
	} {
		snapshot, available := read()
		if available || !errors.Is(snapshot.ReadError, context.Canceled) {
			t.Fatalf("available=%v error=%v", available, snapshot.ReadError)
		}
	}
}

func TestHourlyReaderOffsetTimelineErrorIsTerminal(t *testing.T) {
	st := newReaderTestStore(t)
	from := int64(1_800_000_000_001)
	to := from + 2*hourMS
	if _, err := st.InsertEvents(context.Background(), []usage.Event{readerEvent("offset-error", from+1000, "model", false, 1, 1, nil)}); err != nil {
		t.Fatal(err)
	}
	catchUpReaderRollup(t, context.Background(), st)
	want := errors.New("offset SQL failed")
	st.UsageEvents = &failedOffsetTimelineRepository{Repository: st.UsageEvents, err: want}
	snapshot, available := New(st, true).Load(context.Background(), from, to)
	if available || !errors.Is(snapshot.ReadError, want) {
		t.Fatalf("available=%v error=%v", available, snapshot.ReadError)
	}
}
