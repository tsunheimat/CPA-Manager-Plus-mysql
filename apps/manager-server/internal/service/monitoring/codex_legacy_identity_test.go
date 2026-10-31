package monitoring

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

func TestCodexLegacyEvidencePreservesHistoryAndWindowResponses(t *testing.T) {
	db, st, historyRequest, windowRequest := newCodexLegacyServiceFixture(t, 12, "")
	service := New(st)
	ctx := context.Background()
	wantHistory, err := service.AccountHistory(ctx, historyRequest)
	if err != nil {
		t.Fatal(err)
	}
	wantWindow, err := service.AccountWindowUsage(ctx, windowRequest)
	if err != nil {
		t.Fatal(err)
	}
	for index, count := range []int64{3, 5} {
		if wantHistory.Items[index].TotalRequests != count || wantWindow.Items[index].TotalRequests != count {
			t.Fatalf("fixture target %d lost historical usage: history=%+v window=%+v", index, wantHistory.Items[index], wantWindow.Items[index])
		}
	}
	for _, batchSize := range []int{1, 1000} {
		if _, err := st.CatchUpCodexLegacyIdentityEvidence(ctx, batchSize, time.Now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
		history, err := service.AccountHistory(ctx, historyRequest)
		if err != nil || !reflect.DeepEqual(history.Items, wantHistory.Items) || history.Checkpoint != wantHistory.Checkpoint {
			t.Fatalf("history changed with evidence batch %d: response=%+v err=%v", batchSize, history, err)
		}
		window, err := service.AccountWindowUsage(ctx, windowRequest)
		if err != nil || !reflect.DeepEqual(window.Items, wantWindow.Items) {
			t.Fatalf("window changed with evidence batch %d: response=%+v err=%v", batchSize, window, err)
		}
	}
	// The same evidence reader must also serve the raw analytics fallback.
	if _, err := db.Exec("update usage_monitoring_rollup_state set structure_revision = 'unavailable' where rollup_name = 'projection_v1'"); err != nil {
		t.Fatal(err)
	}
	window, err := service.AccountWindowUsage(ctx, windowRequest)
	if err != nil || !reflect.DeepEqual(window.Items, wantWindow.Items) {
		t.Fatalf("raw account-window fallback changed: response=%+v err=%v", window, err)
	}
}

// BenchmarkCodexLegacyIdentityServices reproduces the issue's 154k/296k
// credential histories in a 610k-event database. Existing monitoring benchmark
// environment variables can change row count and raw payload size.
func BenchmarkCodexLegacyIdentityServices(b *testing.B) {
	count := monitoringBenchmarkEventCount(610_000)
	payload := monitoringBenchmarkRawPayload()
	b.Logf("preparing events=%d credential_a=%d credential_b=%d raw_payload_bytes=%d", count, count*154/610, count*296/610, len(payload))
	preparedAt := time.Now()
	db, st, historyRequest, windowRequest := newCodexLegacyServiceFixture(b, count, payload)
	service := New(st)
	ctx := context.Background()
	wantRequests := []int64{int64(count * 154 / 610), int64(count * 296 / 610)}
	var databaseBytes int64
	if err := db.QueryRow(testdb.SQL(db, "select page_count * page_size from pragma_page_count(), pragma_page_size()", "select sum(data_length+index_length) from information_schema.tables where table_schema=database()")).Scan(&databaseBytes); err != nil {
		b.Fatal(err)
	}
	b.Logf("fixture prepared elapsed=%s database_bytes=%d", time.Since(preparedAt), databaseBytes)
	for _, evidence := range []string{"raw_evidence", "stored_evidence"} {
		if evidence == "stored_evidence" {
			started := time.Now()
			var longestBatch time.Duration
			batches := 0
			for {
				batchStarted := time.Now()
				result, err := st.CatchUpCodexLegacyIdentityEvidence(ctx, 1000, time.Now().UnixMilli())
				if err != nil {
					b.Fatal(err)
				}
				longestBatch = max(longestBatch, time.Since(batchStarted))
				batches++
				if !result.Pending {
					break
				}
			}
			var groups int64
			if err := db.QueryRow("select count(*) from usage_codex_legacy_identity_evidence_v1").Scan(&groups); err != nil {
				b.Fatal(err)
			}
			b.Logf("identity evidence backfill elapsed=%s batches=%d longest_batch=%s groups=%d", time.Since(started), batches, longestBatch, groups)
		}
		for _, cache := range []string{"warm", "fresh_sqlite_connection"} {
			for _, operation := range []string{"history", "window"} {
				b.Run(evidence+"/"+cache+"/"+operation, func(b *testing.B) {
					samples := make([]time.Duration, 0, b.N)
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if cache == "fresh_sqlite_connection" {
							b.StopTimer()
							db.SetMaxIdleConns(0)
							db.SetMaxIdleConns(1)
							b.StartTimer()
						}
						started := time.Now()
						if operation == "history" {
							response, err := service.AccountHistory(ctx, historyRequest)
							if err != nil || len(response.Items) != 2 {
								b.Fatalf("history response=%+v err=%v", response, err)
							}
							for i, item := range response.Items {
								if item.TotalRequests != wantRequests[i] {
									b.Fatalf("history target %d requests=%d, want %d", i, item.TotalRequests, wantRequests[i])
								}
							}
						} else {
							response, err := service.AccountWindowUsage(ctx, windowRequest)
							if err != nil || len(response.Items) != 2 {
								b.Fatalf("window response=%+v err=%v", response, err)
							}
							for i, item := range response.Items {
								if item.TotalRequests != wantRequests[i] {
									b.Fatalf("window target %d requests=%d, want %d", i, item.TotalRequests, wantRequests[i])
								}
							}
						}
						samples = append(samples, time.Since(started))
					}
					b.StopTimer()
					sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
					b.ReportMetric(float64(samples[len(samples)/2])/float64(time.Millisecond), "p50-ms")
					b.ReportMetric(float64(samples[(len(samples)*95-1)/100])/float64(time.Millisecond), "p95-ms")
				})
			}
		}
	}
}

func newCodexLegacyServiceFixture(t testing.TB, count int, payload string) (*sql.DB, *store.Store, AccountHistoryRequest, AccountWindowUsageRequest) {
	t.Helper()
	if count < 12 {
		t.Fatal("identity service fixture requires at least 12 events")
	}
	db, err := testdb.Open(t, filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if err := testdb.PrepareIndexes(ctx, db); err != nil {
		t.Fatal(err)
	}
	const fromMS int64 = 1_700_000_000_000
	toMS := fromMS + int64(count)*1000 + 1
	aCount, bCount := count*154/610, count*296/610
	const batchSize = 512
	for start := 1; start <= count; start += batchSize {
		var values []any
		var rows []string
		for id := start; id <= count && id < start+batchSize; id++ {
			credential, boundary := 0, aCount/2
			if id > aCount {
				credential, boundary = 1, aCount+bCount/2
			}
			if id > aCount+bCount {
				credential, boundary = 2, aCount+bCount+(count-aCount-bCount)/2
			}
			account := ""
			if id > boundary {
				account = fmt.Sprintf("workspace-%d", credential)
			}
			ts := fromMS + int64(id)*1000
			file := fmt.Sprintf("codex-%d.json", credential)
			values = append(values, id, fmt.Sprintf("identity-%d", id), ts, fmt.Sprint(ts), "gpt-test", "gpt-test", ts, "codex", "codex", file, file, fmt.Sprintf("auth-%d", credential), account, fmt.Sprintf("member-%d@example.com", credential), 100, 20, 120, payload)
			rows = append(rows, "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		}
		if _, err := db.Exec(`insert into usage_events(id,event_hash,timestamp_ms,timestamp,model,requested_model,created_at_ms,provider,auth_provider_snapshot,auth_file_snapshot,source,auth_index,auth_account_id_snapshot,account_snapshot,input_tokens,output_tokens,total_tokens,raw_json) values `+strings.Join(rows, ","), values...); err != nil {
			t.Fatal(err)
		}
	}

	st := store.New(db)
	if err := st.SaveModelPrices(ctx, map[string]store.ModelPrice{"gpt-test": {Prompt: 1, Completion: 2}}); err != nil {
		t.Fatal(err)
	}
	for {
		result, err := st.CatchUpAccountHistoryRollups(ctx, 5000, toMS)
		if err != nil {
			t.Fatal(err)
		}
		if result.Processed == 0 {
			break
		}
	}
	for {
		result, err := st.CatchUpUsagePricing(ctx, 5000, toMS)
		if err != nil {
			t.Fatal(err)
		}
		if !result.Pending {
			break
		}
	}
	for _, catchUp := range []func(context.Context, int, int64) (store.UsageMonitoringCatchUpResult, error){
		st.CatchUpUsageMonitoringProjection, st.CatchUpUsageMonitoringStats,
	} {
		for {
			result, err := catchUp(ctx, 5000, toMS)
			if err != nil {
				t.Fatal(err)
			}
			if !result.Pending {
				break
			}
		}
	}
	// Bulk fixture creation can leave a multi-GB WAL. Measure steady-state
	// queries after checkpointing that setup work, with the same database for
	// both evidence readers. Reopening a connection does not clear OS caches.
	var busy, logFrames, checkpointed int
	if err := db.QueryRow(testdb.SQL(db, "pragma wal_checkpoint(truncate)", "select 0,0,0")).Scan(&busy, &logFrames, &checkpointed); err != nil || busy != 0 {
		t.Fatalf("checkpoint identity fixture: busy=%d err=%v", busy, err)
	}
	history := AccountHistoryRequest{}
	windows := AccountWindowUsageRequest{}
	for i := 0; i < 2; i++ {
		history.Accounts = append(history.Accounts, AccountHistoryTarget{
			RowKey: fmt.Sprint(i), AuthFileSnapshot: fmt.Sprintf("codex-%d.json", i),
			AuthIndex: fmt.Sprintf("auth-%d", i), AuthProviderSnapshot: "codex",
			AuthAccountIDSnapshot: fmt.Sprintf("workspace-%d", i), AccountSnapshot: fmt.Sprintf("member-%d@example.com", i),
		})
		windows.Windows = append(windows.Windows, AccountWindowUsageTarget{
			RowKey: fmt.Sprint(i), WindowKey: "window", FromMS: fromMS, ToMS: toMS,
			AuthFileSnapshot: fmt.Sprintf("codex-%d.json", i), AuthIndex: fmt.Sprintf("auth-%d", i),
			AuthProviderSnapshot: "codex", AuthAccountIDSnapshot: fmt.Sprintf("workspace-%d", i),
			AccountSnapshot: fmt.Sprintf("member-%d@example.com", i),
		})
	}
	return db, st, history, windows
}
