package usageevent

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestLatencyOversizedCalendarMapRanksCompleteBuckets(t *testing.T) {
	db, err := testdb.Open(t, filepath.Join(t.TempDir(), "bounded.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	const count = maxAnalyticsBucketMapHours + 3
	tx, err := sqlwrite.BeginTx(ctx, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `insert into usage_events(event_hash,timestamp_ms,timestamp,model,latency_ms,ttft_ms,created_at_ms) values(?,?,'synthetic','model',?,?,1)`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var latencySamples, ttftSamples []float64
	want := map[int64]LatencyPercentiles{}
	for i := 0; i < count; i++ {
		timestamp := base + int64(i)*3_600_000 + 17000
		latency, ttft := int64(i%101+1), int64(i%13)
		if _, err := stmt.ExecContext(ctx, fmt.Sprintf("%064x", i+1), timestamp, latency, ttft); err != nil {
			t.Fatal(err)
		}
		latencySamples = append(latencySamples, float64(latency))
		point := LatencyPercentiles{BucketMS: usage.AnalyticsBucketMS(timestamp, "hour", location), P95LatencyMS: sql.NullFloat64{Float64: float64(latency), Valid: true}}
		if ttft > 0 {
			ttftSamples = append(ttftSamples, float64(ttft))
			point.P95TTFTMS = sql.NullFloat64{Float64: float64(ttft), Valid: true}
		}
		want[point.BucketMS] = point
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	filter := AnalyticsFilter{FromMS: base, ToMS: base + count*3_600_000, IncludeFailed: true}
	if _, ok, err := analyticsSQLBucketPlan(ctx, filter.FromMS, filter.ToMS, "hour", location, false); err != nil || ok {
		t.Fatal("fixture must exceed interval map bound", ok, err)
	}
	got, err := New(db, WithMySQLAnalytics(true)).LatencyMetricsWithFilter(ctx, filter, "hour", location, true, true)
	if err != nil || len(got.Timeline) != len(want) {
		t.Fatal("truncated result", len(got.Timeline), len(want), err)
	}
	p95Latency, _ := percentile95(latencySamples)
	p95TTFT, _ := percentile95(ttftSamples)
	if !got.Summary.P95LatencyMS.Valid || got.Summary.P95LatencyMS.Float64 != p95Latency || !got.Summary.P95TTFTMS.Valid || got.Summary.P95TTFTMS.Float64 != p95TTFT {
		t.Fatal("chunk percentiles used for global rank", got.Summary)
	}
	for i, point := range got.Timeline {
		if point != want[point.BucketMS] || i > 0 && point.BucketMS <= got.Timeline[i-1].BucketMS {
			t.Fatal("bucket population split or duplicated", point, want[point.BucketMS])
		}
	}
	// The disabled-MySQL reference strategy must not impose the optimized
	// calendar map's bound on historical coverage, either.
	reference, err := New(db).(*repository).latencyMetricsReference(ctx, filter, "hour", location, true, true)
	if err != nil || !reflect.DeepEqual(reference, got) {
		t.Fatalf("streaming reference lost historical buckets: got %d want %d, summary=%#v want=%#v, err=%v", len(reference.Timeline), len(got.Timeline), reference.Summary, got.Summary, err)
	}
}
