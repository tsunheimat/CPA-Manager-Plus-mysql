package analyticsbench_test

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	monitoring "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/monitoring"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

// BenchmarkUsageAnalyticsHeatmap exercises the workload absent from the core /
// percentile / selector benchmark. The seven-day bounds match the frozen
// acceptance window, but generated events are synthetic, not a production dump.
// Native/reference runs must use the same event count and server configuration.
func BenchmarkUsageAnalyticsHeatmap(b *testing.B) {
	for _, days := range []int{7, 30} {
		b.Run(fmt.Sprintf("%dd", days), func(b *testing.B) {
			for _, combined := range []bool{false, true} {
				profile := "heatmap_only"
				if combined {
					profile = "compact_summary_heatmap"
				}
				b.Run(profile, func(b *testing.B) {
					ctx := context.Background()
					cfg := config.Config{DBDriver: "sqlite", DBPath: filepath.Join(b.TempDir(), "usage.sqlite")}
					if testdb.MySQL() {
						cfg.DBDriver, cfg.DBDSN = "mysql", testdb.DSN(b, cfg.DBPath)
					}
					cfg.MySQLAnalyticsEnabled = enabled("CPA_MYSQL_BENCH_NATIVE_READS")
					cfg.MySQLBulkEnabled = enabled("CPA_MYSQL_BENCH_BULK_WRITES")
					cfg.MySQLHourlyEnabled = enabled("CPA_MYSQL_BENCH_HOURLY")
					db, err := store.OpenConfigured(ctx, cfg)
					if err != nil {
						b.Fatal(err)
					}
					defer db.Close()
					prices := make(map[string]store.ModelPrice)
					for model := 0; model < 12; model++ {
						prices[fmt.Sprintf("test-model-%d", model)] = store.ModelPrice{
							Prompt: 1, Completion: 2,
							ContextTiers: []store.ModelPriceContextTier{
								{ThresholdTokens: 128, Prompt: 2, PromptConfigured: true},
								{ThresholdTokens: 256, Prompt: 3, Completion: 4, PromptConfigured: true, CompletionConfigured: true},
							},
						}
					}
					if err := db.SaveModelPrices(ctx, prices); err != nil {
						b.Fatal(err)
					}
					const toMS int64 = 1794380110560
					fromMS := toMS - int64(days)*int64(24*time.Hour/time.Millisecond)
					count := eventCount()
					var expectedTokens, expectedFailed int64
					for offset := 0; offset < count; offset += 200 {
						batch := make([]usage.Event, 0, min(200, count-offset))
						for i := offset; i < min(offset+200, count); i++ {
							e := event(i, fromMS+int64(i)*(toMS-fromMS)/int64(count))
							// Coprime dimensions avoid the tiny correlated cross-product
							// of the original model/key fixture. A final 168-cell chart
							// must still account for all contributor/pricing groups.
							e.APIKeyHash = hash(fmt.Sprintf("heatmap-key-%d", i%1009))
							e.Provider = fmt.Sprintf("provider-%d", i%5)
							e.AuthProviderSnapshot = e.Provider
							expectedTokens += e.TotalTokens
							if e.Failed {
								expectedFailed++
							}
							batch = append(batch, e)
						}
						if _, err := db.InsertEvents(ctx, batch); err != nil {
							b.Fatal(err)
						}
					}
					catchUp(b, ctx, db, toMS)
					svc := monitoring.New(db, true)
					req := monitoring.Request{FromMS: fromMS, ToMS: toMS, NowMS: toMS, TimeZone: "Asia/Hong_Kong",
						Include: monitoring.Include{Heatmap: true, Summary: combined, SummaryProfile: "compact", Granularity: "day"}}
					warm, err := svc.Analytics(ctx, req)
					if err != nil {
						b.Fatalf("heatmap fixture warmup: %v", err)
					}
					assertHeatmapTotals(b, warm, int64(count), expectedTokens, expectedFailed, combined)
					b.Logf("engine=%s days=%d events=%d native_reads=%t bulk=%t hourly=%t timezone=Asia/Hong_Kong; warmed synthetic fixture, key_limit=1009 provider_limit=5 model_limit=12; service deadline enforced; not a production speedup or browser measurement",
						cfg.DBDriver, days, count, cfg.MySQLAnalyticsEnabled, cfg.MySQLBulkEnabled, cfg.MySQLHourlyEnabled)
					before := db.DatabasePoolStats()
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						result, err := svc.Analytics(ctx, req)
						if err != nil {
							b.Fatalf("heatmap request: %v", err)
						}
						assertHeatmapTotals(b, result, int64(count), expectedTokens, expectedFailed, combined)
					}
					b.StopTimer()
					b.ReportMetric(float64(db.DatabasePoolStats().WaitCount-before.WaitCount), "pool_waits_total")
				})
			}
		})
	}
}

func assertHeatmapTotals(b *testing.B, result monitoring.Response, calls, tokens, failed int64, combined bool) {
	b.Helper()
	if len(result.Heatmap) == 0 || len(result.Heatmap) > 7*24 {
		b.Fatalf("invalid heatmap cell count: %d", len(result.Heatmap))
	}
	var gotCalls, gotTokens, gotFailed, gotSuccess int64
	var cost float64
	for _, cell := range result.Heatmap {
		gotCalls += cell.Calls
		gotTokens += cell.Tokens
		gotFailed += cell.Failure
		gotSuccess += cell.Success
		cost += cell.Cost
	}
	if gotCalls != calls || gotTokens != tokens || gotFailed != failed || gotSuccess != calls-failed {
		b.Fatalf("heatmap totals calls/tokens/failed/success=%d/%d/%d/%d, expected %d/%d/%d/%d", gotCalls, gotTokens, gotFailed, gotSuccess, calls, tokens, failed, calls-failed)
	}
	if math.IsNaN(cost) || math.IsInf(cost, 0) || cost <= 0 {
		b.Fatalf("missing/invalid priced heatmap cost: %v", cost)
	}
	if combined && (result.Summary == nil || result.Summary.TotalCalls != calls || math.Abs(result.Summary.TotalCost-cost) > 1e-9*math.Max(1, math.Abs(cost))) {
		b.Fatalf("summary/heatmap inconsistency: summary=%+v heatmap_cost=%v", result.Summary, cost)
	}
}
