package usageevent

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
)

func TestPricingEveryRawCallerPushesCompleteFilter(t *testing.T) {
	ctx := context.Background()
	filter := AnalyticsFilter{FromMS: 123, ToMS: 7200123, IncludeFailed: true, SearchQuery: "needle", SearchAPIKeyHash: "hash", Models: []string{"model"}, Providers: []string{"provider"}, Accounts: []string{"account"}, CredentialIDs: []string{"credential"}, AuthFiles: []string{"file"}, AuthIndices: []string{"index"}, APIKeyHashes: []string{"key"}, SourceHashes: []string{"source"}, ProjectIDs: []string{"project"}, RequestTypes: []string{"request"}, MinLatencyMS: 13, CacheStatus: "hit", HeaderErrorKinds: []string{"kind"}, HeaderErrorCodes: []string{"code"}, HeaderQuotaPlans: []string{"quota"}, HeaderTraceIDs: []string{"trace"}}
	where, want := analyticsWhere(filter)
	for _, name := range []string{"model", "limited-model", "timeline", "api-timeline", "heatmap", "channel", "account", "api", "credential", "credential-timeline", "model-between", "top-between", "account-window"} {
		t.Run(name, func(t *testing.T) {
			repo, conn := newLatencyRoutingRepository(t)
			conn.query = func(_ context.Context, _ int, query string, args []driver.NamedValue) (*latencyRoutingRows, error) {
				baseEnd := strings.Index(query, "), pricing_resolved_events")
				if baseEnd < 0 {
					t.Fatal("caller did not use explicit pricing pipeline", query)
				}
				if strings.Contains(query, ".*") || strings.Contains(query, "select *") || strings.Contains(query, "select max(tier.") {
					t.Fatal("old pricing shape", query)
				}
				if name != "model-between" && name != "top-between" && name != "account-window" {
					// Split credential queries use adjusted time bounds, but the complete
					// predicate and all non-time bindings must remain unchanged.
					if !strings.Contains(query[:baseEnd], where) {
						t.Fatal("filter is not on raw source", query)
					}
					for i := 2; i < len(want); i++ {
						if args[i].Value != want[i] {
							t.Fatalf("binding %d changed: %v want %v", i, args[i].Value, want[i])
						}
					}
				}
				bindings := make([]any, len(args))
				for i, a := range args {
					bindings[i] = a.Value
				}
				compiled, compiledArgs, err := mysql.CompileSQL(query, bindings...)
				if err != nil || !reflect.DeepEqual(compiledArgs, bindings) {
					t.Fatalf("caller compile/bindings: %v", err)
				}
				if !strings.Contains(compiled, "lead(") {
					t.Fatal("interval missing after compile")
				}
				if name == "account-window" {
					if !strings.HasPrefix(query, "with window_targets") || !strings.Contains(query[:baseEnd], "usage_events join window_targets") || !strings.Contains(query[:baseEnd], "in (w.account_key, w.legacy_account_key)") {
						t.Fatal("account windows must scope before pricing", query)
					}
					if strings.Contains(query[baseEnd:], "auth_file_snapshot") {
						t.Fatal("account identity evidence leaked into pricing projection")
					}
					if args[0].Value != int64(4) || args[1].Value != filter.FromMS || args[2].Value != filter.ToMS || args[3].Value != "bound-account" {
						t.Fatal("target bindings changed", args)
					}
				}
				return &latencyRoutingRows{columns: []string{"unused"}}, nil
			}
			var err error
			switch name {
			case "model":
				_, err = repo.ModelStatsWithFilter(ctx, filter, 0)
			case "limited-model":
				_, err = repo.ModelStatsWithFilter(ctx, filter, 2)
			case "timeline":
				_, err = repo.TimelineWithFilter(ctx, filter, "hour", time.UTC)
			case "api-timeline":
				_, err = repo.APIKeyTimelineWithFilter(ctx, filter, "hour", time.UTC)
			case "heatmap":
				_, err = repo.HeatmapWithFilter(ctx, filter, time.UTC)
			case "channel":
				_, err = repo.ChannelModelStatsWithFilter(ctx, filter)
			case "account":
				_, err = repo.AccountModelStatsWithFilter(ctx, filter)
			case "api":
				_, err = repo.APIKeyModelStatsWithFilter(ctx, filter)
			case "credential":
				_, err = repo.CredentialModelStatsWithFilter(ctx, filter)
			case "credential-timeline":
				_, err = repo.CredentialTimelineWithFilter(ctx, filter, "hour", time.UTC)
			case "model-between":
				_, err = repo.ModelStatsBetween(ctx, filter.FromMS, filter.ToMS)
			case "top-between":
				_, err = repo.TopModelsBetween(ctx, filter.FromMS, filter.ToMS, 2)
			case "account-window":
				_, err = repo.AccountWindowModelStats(ctx, []AccountWindowUsageQuery{{RequestIndex: 4, FromMS: filter.FromMS, ToMS: filter.ToMS, AccountKey: "bound-account"}})
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(conn.queries) == 0 {
				t.Fatal("caller did not query")
			}
		})
	}
}

func TestPricingSourceFilterSemanticsIndependentFixtures(t *testing.T) {
	db := pricingTestDB(t)
	for i := int64(1); i <= 3; i++ {
		insertPricingEvent(t, db, i, "display", "requested(high)", "billing", "priority", 100, nil)
	}
	pricingFixtureExec(t, db, `update usage_events set provider='raw-provider',auth_provider_snapshot='snapshot-provider',account_snapshot='account',auth_label_snapshot='label',auth_index='index',source='source',source_hash='source-hash',auth_file_snapshot='file',auth_project_id_snapshot='project',api_key_hash='key',executor_type='request',header_error_kind='kind',header_error_code='code',header_quota_plan_type='quota',header_trace_id='trace',latency_ms=20,cache_read_tokens=5,cache_creation_tokens=3,fail_summary='needle' where timestamp_ms=1`)
	pricingFixtureExec(t, db, `update usage_events set api_key_hash='other-key',failed=1,auth_provider_snapshot='codex',auth_project_id_snapshot='codex-account-id:v1:hidden-marker',latency_ms=-3 where timestamp_ms=2`)
	pricingFixtureExec(t, db, `update usage_events set failed=2,auth_index='fallback-index',source='last-source' where timestamp_ms=3`)
	type testCase struct {
		name   string
		change func(*AnalyticsFilter)
		want   []int64
	}
	cases := []testCase{
		{"model", func(f *AnalyticsFilter) { f.Models = []string{"requested(high)"} }, []int64{1, 2, 3}},
		{"raw-provider", func(f *AnalyticsFilter) { f.Providers = []string{"RAW-PROVIDER"} }, []int64{1}},
		{"snapshot-provider", func(f *AnalyticsFilter) { f.Providers = []string{"snapshot-provider"} }, []int64{1}},
		{"credential-file", func(f *AnalyticsFilter) { f.CredentialIDs = []string{"file"} }, []int64{1}},
		{"credential-index", func(f *AnalyticsFilter) { f.CredentialIDs = []string{"fallback-index"} }, []int64{3}},
		{"auth-file", func(f *AnalyticsFilter) { f.AuthFiles = []string{"file"} }, []int64{1}},
		{"auth-index", func(f *AnalyticsFilter) { f.AuthIndices = []string{"index"} }, []int64{1}},
		{"key", func(f *AnalyticsFilter) { f.APIKeyHashes = []string{"key"} }, []int64{1}},
		{"source-hash", func(f *AnalyticsFilter) { f.SourceHashes = []string{"source-hash"} }, []int64{1}},
		{"project", func(f *AnalyticsFilter) { f.ProjectIDs = []string{"project"} }, []int64{1}},
		{"hidden-project", func(f *AnalyticsFilter) { f.ProjectIDs = []string{"codex-account-id:v1:hidden-marker"} }, nil},
		{"request", func(f *AnalyticsFilter) { f.RequestTypes = []string{"request"} }, []int64{1}},
		{"header-kind", func(f *AnalyticsFilter) { f.HeaderErrorKinds = []string{"kind"} }, []int64{1}},
		{"header-code", func(f *AnalyticsFilter) { f.HeaderErrorCodes = []string{"code"} }, []int64{1}},
		{"header-quota", func(f *AnalyticsFilter) { f.HeaderQuotaPlans = []string{"quota"} }, []int64{1}},
		{"header-trace", func(f *AnalyticsFilter) { f.HeaderTraceIDs = []string{"trace"} }, []int64{1}},
		{"success", func(f *AnalyticsFilter) { f.IncludeFailed = false }, []int64{1}},
		{"failed", func(f *AnalyticsFilter) { f.FailedOnly = true }, []int64{2}},
		{"contradictory", func(f *AnalyticsFilter) { f.IncludeFailed = false; f.FailedOnly = true }, nil},
		{"latency", func(f *AnalyticsFilter) { f.MinLatencyMS = 10 }, []int64{1}},
		{"search-or-key", func(f *AnalyticsFilter) { f.SearchQuery = "NEEDLE"; f.SearchAPIKeyHash = "OTHER-KEY" }, []int64{1, 2}},
		{"wildcard", func(f *AnalyticsFilter) { f.SearchQuery = "need_e" }, []int64{1}},
		{"raw-marker-search", func(f *AnalyticsFilter) { f.SearchQuery = "hidden-marker" }, []int64{2}},
		{"empty", func(f *AnalyticsFilter) { f.Models = []string{"absent"} }, nil},
	}
	for _, account := range []string{"account", "label", "source", "index"} {
		value := account
		cases = append(cases, testCase{"account-" + value, func(f *AnalyticsFilter) { f.Accounts = []string{value} }, []int64{1}})
	}
	for _, status := range []string{"hit", "read", "creation", "miss"} {
		value := status
		want := []int64{1}
		if status == "miss" {
			want = []int64{2, 3}
		}
		cases = append(cases, testCase{"cache-" + status, func(f *AnalyticsFilter) { f.CacheStatus = value }, want})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			filter := AnalyticsFilter{FromMS: 1, ToMS: 4, IncludeFailed: true}
			tc.change(&filter)
			where, args := analyticsWhere(filter)
			rows, err := db.Query(pricingBandedUsageEventsCTEWithBaseFilter(where, pricingHeatmapFields)+` select timestamp_ms from banded_usage_events order by timestamp_ms`, args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var got []int64
			for rows.Next() {
				var ts int64
				if err := rows.Scan(&ts); err != nil {
					t.Fatal(err)
				}
				got = append(got, ts)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestCredentialPricingPartsSnapshotAndTerminalErrors(t *testing.T) {
	filter := AnalyticsFilter{FromMS: 123, ToMS: 7200123, IncludeFailed: true}
	for _, mode := range []string{"success", "sqlite", "begin", "commit", "query", "scan", "rows", "cancel"} {
		stages := []int{0}
		if mode == "query" || mode == "scan" || mode == "rows" || mode == "cancel" {
			stages = []int{0, 1, 2}
		}
		for _, failStage := range stages {
			t.Run(fmt.Sprintf("%s/%d", mode, failStage), func(t *testing.T) {
				repo, conn := newLatencyRoutingRepository(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				sentinel := errors.New("injected credential failure")
				if mode == "sqlite" {
					repo.mysqlBackend = false
				}
				if mode == "begin" {
					conn.beginError = sentinel
				}
				if mode == "commit" {
					conn.commitError = sentinel
				}
				conn.query = func(_ context.Context, stage int, _ string, _ []driver.NamedValue) (*latencyRoutingRows, error) {
					if mode != "sqlite" && (!conn.active.Load() || !conn.options.ReadOnly || conn.options.Isolation != driver.IsolationLevel(sql.LevelRepeatableRead)) {
						t.Fatal("parts escaped repeatable-read transaction")
					}
					rows := &latencyRoutingRows{columns: []string{"unused"}}
					if stage == failStage {
						switch mode {
						case "query":
							return nil, sentinel
						case "scan":
							rows.values = [][]driver.Value{{"invalid-row-shape"}}
						case "rows":
							rows.next = func(int, []driver.Value) error { return sentinel }
						case "cancel":
							cancel()
							return nil, ctx.Err()
						}
					}
					return rows, nil
				}
				got, err := repo.CredentialTimelineWithFilter(ctx, filter, "hour", time.UTC)
				if mode == "success" || mode == "sqlite" {
					if err != nil || len(got) != 0 || len(conn.queries) != 3 {
						t.Fatal(got, err, len(conn.queries))
					}
				} else {
					if err == nil || got != nil {
						t.Fatal("partial result or lost error", got, err)
					}
				}
				if mode == "sqlite" {
					if conn.begins.Load() != 0 {
						t.Fatal("SQLite transaction semantics changed")
					}
					return
				}
				if conn.begins.Load() != 1 || conn.admissions.Load() != 0 || conn.openRows.Load() != 0 {
					t.Fatal("invalid transaction/admission/rows accounting")
				}
				if mode == "query" || mode == "scan" || mode == "rows" || mode == "cancel" {
					if len(conn.queries) != failStage+1 || conn.commits.Load() != 0 {
						t.Fatal("retried or committed after attempted operation failed")
					}
				}
			})
		}
	}
}
