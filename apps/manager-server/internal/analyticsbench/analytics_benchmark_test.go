package analyticsbench_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	monitoring "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/monitoring"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

// BenchmarkUsageAnalyticsRollingWindow keeps non-midnight bounds and varied
// sample populations fixed. Setup/catch-up are excluded from measured requests.
// Without a test DSN this measures SQLite only, never MySQL performance.
func BenchmarkUsageAnalyticsRollingWindow(b *testing.B) {
	for _, profile := range []string{"core", "percentiles_only", "core_selectors_pair", "core_selectors_pair_with_ingestion", "overview_v1_sections", "overview_v2_sections"} {
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
			pageProfile := strings.HasPrefix(profile, "overview_")
			if pageProfile {
				book := make(map[string]store.ModelPrice)
				for i := 0; i < 12; i++ {
					book[fmt.Sprintf("test-model-%d", i)] = store.ModelPrice{
						Prompt: 1, Completion: 2, PromptConfigured: true, CompletionConfigured: true,
						RawJSON: `{"catalog_metadata":"` + strings.Repeat("m", 32768) + `"}`,
						ContextTiers: []store.ModelPriceContextTier{
							{ThresholdTokens: 128, Prompt: 3, PromptConfigured: true},
							{ThresholdTokens: 256, Completion: 4, CompletionConfigured: true},
						},
					}
				}
				if err := db.SaveModelPrices(ctx, book); err != nil {
					b.Fatal(err)
				}
			}
			const toMS int64 = 1782227112345
			fromMS := toMS - int64(24*time.Hour/time.Millisecond)
			count := eventCount()
			for offset := 0; offset < count; offset += 200 {
				end := min(offset+200, count)
				events := make([]usage.Event, 0, end-offset)
				for index := offset; index < end; index++ {
					e := event(index, fromMS+int64(index)*(toMS-fromMS)/int64(count))
					if pageProfile {
						e.RawJSON = `{"diagnostic":"` + strings.Repeat("d", 1024) + `"}`
						if e.Failed {
							e.FailBody = `{"error":{"message":"` + strings.Repeat("retry detail ", 315) + `"}}`
						}
					}
					events = append(events, e)
				}
				if _, err := db.InsertEvents(ctx, events); err != nil {
					b.Fatal(err)
				}
			}
			catchUp(b, ctx, db, toMS)
			svc := monitoring.New(db, true)
			core := monitoring.Request{FromMS: fromMS, ToMS: toMS, NowMS: toMS, TimeZone: "UTC",
				Include: monitoring.Include{Summary: true, SummaryProfile: "compact", ModelStats: true,
					Timeline: true, TimelineProfile: "core", Granularity: "hour"}}
			exact := core
			exact.Include = monitoring.Include{Granularity: "hour", Percentiles: &monitoring.PercentilesInclude{Summary: true, Timeline: true}}
			selectors := core
			selectors.Include = monitoring.Include{FilterOptions: true, FilterSelectors: true}
			if result, err := svc.Analytics(ctx, core); err != nil || result.Summary == nil || result.Summary.TotalCalls != int64(count) {
				b.Fatalf("fixture core parity: summary=%+v err=%v", result.Summary, err)
			}
			b.Logf("engine=%s events=%d native_reads=%t bulk=%t hourly=%t; warmed ready fixture; no HTTP/proxy overhead", cfg.DBDriver, count, cfg.MySQLAnalyticsEnabled, cfg.MySQLBulkEnabled, cfg.MySQLHourlyEnabled)
			before := db.DatabasePoolStats()
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if profile == "overview_v1_sections" || profile == "overview_v2_sections" {
					for _, section := range overviewPageRequests(fromMS, toMS, "UTC", profile == "overview_v2_sections") {
						if _, err := svc.Analytics(ctx, section.request); err != nil {
							b.Fatalf("%s: %v", section.name, err)
						}
					}
					continue
				}
				if profile == "core" || profile == "percentiles_only" {
					request := core
					if profile == "percentiles_only" {
						request = exact
					}
					if _, err := svc.Analytics(ctx, request); err != nil {
						b.Fatal(err)
					}
					continue
				}
				var group sync.WaitGroup
				errors := make(chan error, 3)
				for _, request := range []monitoring.Request{core, selectors} {
					group.Add(1)
					go func(request monitoring.Request) {
						defer group.Done()
						_, err := svc.Analytics(ctx, request)
						errors <- err
					}(request)
				}
				if profile == "core_selectors_pair_with_ingestion" {
					group.Add(1)
					go func(sequence int) {
						defer group.Done()
						// New IDs with timestamps outside the frozen range preserve
						// answer comparability while exercising concurrent writes.
						batch := make([]usage.Event, 32)
						for i := range batch {
							batch[i] = event(count+sequence*len(batch)+i, toMS+int64(sequence*len(batch)+i+1))
						}
						_, err := db.InsertEvents(ctx, batch)
						errors <- err
					}(iteration)
				}
				group.Wait()
				close(errors)
				for err := range errors {
					if err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
			stats := db.DatabasePoolStats()
			b.ReportMetric(float64(stats.WaitCount-before.WaitCount), "pool_waits_total")
		})
	}
}

func enabled(name string) bool { return os.Getenv(name) == "1" || os.Getenv(name) == "true" }

func eventCount() int {
	if value, err := strconv.Atoi(os.Getenv("CPA_MANAGER_MONITORING_BENCH_EVENTS")); err == nil && value > 0 {
		return value
	}
	return 100_000
}

func hash(value string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(value))) }

func event(index int, timestamp int64) usage.Event {
	latency, ttft := int64(20+index%1700), int64(1+(index*19)%380)
	return usage.Event{EventHash: hash(fmt.Sprintf("mysql-analytics-benchmark-%d", index)),
		TimestampMS: timestamp, Timestamp: time.UnixMilli(timestamp).UTC().Format(time.RFC3339Nano),
		CreatedAtMS: timestamp, Model: fmt.Sprintf("test-model-%d", index%12), Provider: "openai",
		AuthProviderSnapshot: "openai", AuthIndex: fmt.Sprintf("auth-%d", index%40),
		AuthFileSnapshot: fmt.Sprintf("test-account-%d.json", index%40),
		AccountSnapshot:  fmt.Sprintf("test-account-%d@example.invalid", index%40),
		Source:           "benchmark", SourceHash: hash(fmt.Sprintf("source-%d", index%40)),
		APIKeyHash:  hash(fmt.Sprintf("key-%d", index%20)),
		InputTokens: int64(100 + index%200), OutputTokens: int64(50 + index%100),
		TotalTokens: int64(150 + index%200 + index%100), LatencyMS: &latency, TTFTMS: &ttft,
		Failed: index%23 == 0}
}

func catchUp(b *testing.B, ctx context.Context, db *store.Store, nowMS int64) {
	b.Helper()
	if _, err := db.DiscoverUsageCacheAccounting(ctx); err != nil {
		b.Fatal(err)
	}
	for {
		result, err := db.RunUsageCacheAccountingBatch(ctx, 5000)
		if err != nil {
			b.Fatal(err)
		}
		if result.Completed {
			break
		}
	}
	for {
		result, err := db.CatchUpUsageHourlyAggregate(ctx, 5000, nowMS)
		if err != nil {
			b.Fatal(err)
		}
		if !result.Pending {
			break
		}
	}
	for {
		result, err := db.CatchUpUsagePricing(ctx, 5000, nowMS)
		if err != nil {
			b.Fatal(err)
		}
		if !result.Pending {
			break
		}
	}
	for _, fn := range []func(context.Context, int, int64) (store.UsageMonitoringCatchUpResult, error){
		db.CatchUpUsageMonitoringProjection, db.CatchUpUsageMonitoringMetadata, db.CatchUpUsageMonitoringStats,
		db.CatchUpUsageMonitoringHourlyStats, db.CatchUpUsageMonitoringHourlyMetadata,
	} {
		for {
			result, err := fn(ctx, 5000, nowMS)
			if err != nil {
				b.Fatal(err)
			}
			if !result.Pending {
				break
			}
		}
	}
}
