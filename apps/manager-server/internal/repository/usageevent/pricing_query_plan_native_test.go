package usageevent

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/modelprice"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

// This is an acceptance test, not evidence of a native speedup. It only uses
// testdb's isolated schema and Admin connection, never runtime EXPLAIN admission
// or the DSN's original database. The larger page sentinel belongs to analyticsbench.
func TestPricingNativeIntervalPlansAndExecution(t *testing.T) {
	if !testdb.MySQL() {
		t.Skip("CPA_TEST_MYSQL_DSN is not set; native acceptance remains pending")
	}
	db := pricingTestDB(t)
	admin := testdb.Admin(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	book := map[string]model.ModelPrice{"billed": {Prompt: 1, ContextTiers: []model.ModelPriceContextTier{{ThresholdTokens: 100, Prompt: 2, PromptConfigured: true}, {ThresholdTokens: 200, Prompt: 3, PromptConfigured: true}}}}
	if err := modelprice.New(db).ReplaceAll(ctx, book); err != nil {
		t.Fatal(err)
	}
	repo := New(db).(*repository)
	const from = int64(1800000000000)
	events := make([]usage.Event, 0, 4160)
	for i := 0; i < 4160; i++ {
		ts := from - 86400000 - int64(i)*1000
		if i < 64 {
			ts = from + int64(i)*1000
		}
		events = append(events, usage.Event{EventHash: canonicalTestHash(fmt.Sprintf("native-interval-%d", i)), TimestampMS: ts, Timestamp: "fixture", Model: "display", RequestedModel: "display(high)", ResolvedModel: "billed", InputTokens: int64(100 + i%3*100), OutputTokens: 2, TotalTokens: int64(102 + i%3*100), AuthFileSnapshot: "fixture.json", APIKeyHash: "fixture-key", Provider: "fixture-provider", CreatedAtMS: 1})
	}
	if _, err := repo.InsertBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	// Verify actual persisted width rather than assuming ingestion retained a
	// requested payload. These columns must not enter pricing materialization.
	payload := strings.Repeat("x", 4096)
	pricingFixtureExec(t, db, `update usage_events set raw_json=?,fail_body=?,response_metadata_json=?`, payload, payload, payload)
	var stored int64
	if err := admin.QueryRowContext(ctx, `select min(octet_length(raw_json)) from usage_events`).Scan(&stored); err != nil || stored != 4096 {
		t.Fatal("persisted fixture width", stored, err)
	}
	filter := AnalyticsFilter{FromMS: from, ToMS: from + 64000, IncludeFailed: true, Models: []string{"display"}, SearchQuery: "fixture.json", APIKeyHashes: []string{usage.NormalizeOpaqueHashForPersistence("fixture-key")}}
	plan, ok, err := analyticsSQLBucketPlan(ctx, filter.FromMS, filter.ToMS, "hour", time.UTC, true)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	heatmap, heatmapArgs := groupedAnalyticsSQL(filter, plan, "heatmap")
	where, args := analyticsWhere(filter)
	queries := []struct {
		name, sql      string
		args           []any
		aggregateReuse bool
		rawScans       int
	}{
		{"heatmap", heatmap, heatmapArgs, false, 1},
		{"models", pricingBandedUsageEventsCTEWithBaseFilter(where, pricingModelFields) + "\n" + modelPricingBandsSQL, args, false, 1},
		{"top", topModelPricingBandsSQL(where), topModelPricingArgs(args, 2), true, 2},
	}
	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			compiled, bindings, err := mysql.CompileSQL(q.sql, q.args...)
			if err != nil {
				t.Fatal(err)
			}
			// The helper pins EXPLAIN ANALYZE and its before/after session
			// counters to one admin connection. Never subtract pooled/global stats.
			evidence := testdb.ExplainMySQL(t, ctx, db, true, q.sql, q.args...)
			var document any
			if err := json.Unmarshal(evidence.Explain, &document); err != nil {
				t.Fatal(err)
			}
			assertPricingNativePlan(t, document, q.aggregateReuse, q.rawScans)
			t.Logf("EXPLAIN FORMAT=JSON %s: %s", q.name, evidence.Explain)
			t.Logf("EXPLAIN ANALYZE %s: %s; session work=%v", q.name, evidence.Analyze, evidence.Work)
			if q.name == "models" {
				shared := readPricingModelStats(t, db, q.sql, q.args...)
				native := readPricingModelStats(t, db, mysql.Native(compiled), bindings...)
				oracle := readPricingModelStats(t, db, frozenScalarPricingCTE+frozenModelPricingAggregate, from, from+64000)
				if !reflect.DeepEqual(sortedAnalyticsJSON(shared), sortedAnalyticsJSON(native)) || !reflect.DeepEqual(sortedAnalyticsJSON(shared), sortedAnalyticsJSON(oracle)) {
					t.Fatal("shared/native/scalar execution differs")
				}
			}
			if q.name == "top" {
				shared := readPricingModelStats(t, db, q.sql, q.args...)
				native := readPricingModelStats(t, db, mysql.Native(compiled), bindings...)
				oracle := readPricingModelStats(t, db, frozenTopModelPricingSQL, from, from+64000, 2)
				if !reflect.DeepEqual(sortedAnalyticsJSON(shared), sortedAnalyticsJSON(native)) || !reflect.DeepEqual(sortedAnalyticsJSON(shared), sortedAnalyticsJSON(oracle)) {
					t.Fatal("shared/native/frozen top execution differs")
				}
			}
		})
	}
	// Prepared compatibility execution and CompileSQL+Native use the exact same
	// generated projection/filter and bindings, not a hand-written substitute.
	countSQL := pricingBandedUsageEventsCTEWithBaseFilter(where, pricingModelFields) + ` select count(*) from banded_usage_events`
	stmt, err := db.PrepareContext(ctx, countSQL)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var count int64
	if err := stmt.QueryRowContext(ctx, args...).Scan(&count); err != nil || count != 64 {
		t.Fatal("prepared event cardinality", count, err)
	}
	compiled, bindings, err := mysql.CompileSQL(countSQL, args...)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, mysql.Native(compiled), bindings...).Scan(&count); err != nil || count != 64 {
		t.Fatal("native event cardinality", count, err)
	}
}

func assertPricingNativePlan(t *testing.T, document any, aggregateReuse bool, wantRawScans int) {
	t.Helper()
	eventScans := 0
	var containsEvents func(any) bool
	containsEvents = func(node any) bool {
		switch n := node.(type) {
		case map[string]any:
			if n["table_name"] == "usage_events" {
				return true
			}
			for _, child := range n {
				if containsEvents(child) {
					return true
				}
			}
		case []any:
			for _, child := range n {
				if containsEvents(child) {
					return true
				}
			}
		}
		return false
	}
	var walk func(any)
	walk = func(node any) {
		switch n := node.(type) {
		case map[string]any:
			if n["table_name"] == "usage_events" {
				eventScans++
				if n["access_type"] != "range" {
					t.Errorf("selective raw source is not range-scoped: %v", n)
				}
			}
			if materialized, ok := n["materialized_from_subquery"]; ok && containsEvents(materialized) {
				encoded, _ := json.Marshal(materialized)
				if !aggregateReuse || !strings.Contains(string(encoded), "grouping_operation") {
					t.Errorf("event-grain pricing materialization: %s", encoded)
				}
			}
			if n["dependent"] == true {
				encoded, _ := json.Marshal(n)
				if strings.Contains(string(encoded), "model_price_context_tiers") {
					t.Error("dependent tier subquery", string(encoded))
				}
			}
			if window, ok := n["windowing"]; ok && containsEvents(window) {
				t.Error("pricing window includes event rows")
			}
			for _, child := range n {
				walk(child)
			}
		case []any:
			for _, child := range n {
				walk(child)
			}
		}
	}
	walk(document)
	if eventScans != wantRawScans {
		t.Errorf("contributing raw scans=%d, want %d (top limits count before pricing)", eventScans, wantRawScans)
	}
	encoded, _ := json.Marshal(document)
	for _, payload := range []string{"raw_json", "fail_body", "response_metadata_json"} {
		if strings.Contains(string(encoded), payload) {
			t.Errorf("plan carries unused payload %s", payload)
		}
	}
}
