package usagepricing

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	pricingservice "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/pricing"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func derivedPricingTokens(band usage.PricingBand, long usage.LongContextTokens, input, output, cached, read, creation int64) pricingservice.ModelTokens {
	return pricingservice.ModelTokens{
		PricingModel: band.PricingModel, ContextThresholdTokens: band.ContextThresholdTokens,
		InputTokens: input, OutputTokens: output, CachedTokens: cached, CacheReadTokens: read, CacheCreationTokens: creation,
		LongInputTokens: long.LongInputTokens, LongOutputTokens: long.LongOutputTokens, LongCachedTokens: long.LongCachedTokens,
		LongCacheReadTokens: long.LongCacheReadTokens, LongCacheCreationTokens: long.LongCacheCreationTokens,
	}
}

func assertDerivedPhysicalPricingGrain(t *testing.T, tx *sql.Tx, query string, args []any) {
	t.Helper()
	rows, err := tx.QueryContext(context.Background(), query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	values, destinations := make([]any, len(columns)), make([]any, len(columns))
	for i := range destinations {
		destinations[i] = &values[i]
	}
	count := 0
	for rows.Next() {
		if err := rows.Scan(destinations...); err != nil {
			t.Fatal(err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("database emitted %d rows before Go merging; want exactly one final pricing grain", count)
	}
}

func TestDerivedPricingRecoveryFinalGrainAndLongMeasures(t *testing.T) {
	for _, tc := range []struct {
		name                                              string
		leftInput, rightInput, leftOutput, rightOutput    int64
		leftCached, leftCache, leftRead, leftCreation     int64
		rightCached, rightCache, rightRead, rightCreation int64
		want                                              pricingservice.ModelTokens
		total, zeroCalls                                  int64
		cost                                              float64
	}{
		{
			name: "monetary-clamp", leftCached: 100, rightInput: 100,
			want:  pricingservice.ModelTokens{PricingModel: "bill", ContextThresholdTokens: -1, InputTokens: 100, CachedTokens: 100},
			total: 100, zeroCalls: 4, cost: 0.00005,
		},
		{
			name: "below", leftInput: 271999, rightInput: 271999, leftOutput: 19, rightOutput: 23,
			leftCached: 60, leftCache: 100, leftRead: 7, leftCreation: 11, rightCached: 8, rightCache: 10, rightRead: 13, rightCreation: 17,
			want:  pricingservice.ModelTokens{PricingModel: "bill", ContextThresholdTokens: -1, InputTokens: 543998, OutputTokens: 42, CachedTokens: 82, CacheReadTokens: 20, CacheCreationTokens: 28},
			total: 544040, zeroCalls: 3, cost: 1.088020,
		},
		{
			name: "equal", leftInput: 272000, rightInput: 272000, leftOutput: 19, rightOutput: 23,
			leftCached: 60, leftCache: 100, leftRead: 7, leftCreation: 11, rightCached: 8, rightCache: 10, rightRead: 13, rightCreation: 17,
			want:  pricingservice.ModelTokens{PricingModel: "bill", ContextThresholdTokens: -1, InputTokens: 544000, OutputTokens: 42, CachedTokens: 82, CacheReadTokens: 20, CacheCreationTokens: 28},
			total: 544042, zeroCalls: 3, cost: 1.088024,
		},
		{
			name: "above", leftInput: 272001, rightInput: 272001, leftOutput: 19, rightOutput: 23,
			leftCached: 60, leftCache: 100, leftRead: 7, leftCreation: 11, rightCached: 8, rightCache: 10, rightRead: 13, rightCreation: 17,
			want: pricingservice.ModelTokens{PricingModel: "bill", ContextThresholdTokens: -1, InputTokens: 544002, OutputTokens: 42, CachedTokens: 82, CacheReadTokens: 20, CacheCreationTokens: 28,
				LongInputTokens: 544002, LongOutputTokens: 42, LongCachedTokens: 82, LongCacheReadTokens: 20, LongCacheCreationTokens: 28},
			total: 544044, zeroCalls: 3, cost: 1.088028,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := pricingSQLFixture(t)
			for _, table := range []string{"usage_events", "usage_monitoring_event_projection_v1"} {
				for _, column := range []string{"provider", "auth_index", "source", "source_hash", "account_snapshot", "auth_label_snapshot", "auth_file_snapshot", "auth_provider_snapshot", "auth_project_id_snapshot", "auth_account_id_snapshot"} {
					pricingSQLExec(t, db, "alter table "+table+" add column "+column+" text default ''")
				}
			}
			pricingSQLExec(t, db, "alter table usage_monitoring_event_projection_v1 add column account_key text")
			pricingSQLExec(t, db, "insert into model_prices values('bill')")
			const key = "usage-account-history:3:file-index:612E6A736F6E:69"
			// Same complete pricing/account grain across live and retained arms.
			// Retained latency=10+20 over two samples; live=90 over one.
			// NULL and zero latency do not enter the denominator.
			for _, event := range []struct {
				retained                                             bool
				id, at, input, output, cached, cache, read, creation int64
				latency                                              any
			}{
				{true, 1, 10, tc.leftInput, tc.leftOutput, tc.leftCached, tc.leftCache, tc.leftRead, tc.leftCreation, int64(10)},
				{true, 2, 11, 0, 0, 0, 0, 0, 0, int64(20)},
				{false, 3, 12, tc.rightInput, tc.rightOutput, tc.rightCached, tc.rightCache, tc.rightRead, tc.rightCreation, int64(90)},
				{false, 4, 13, 0, 0, 0, 0, 0, 0, nil},
				{true, 5, 14, 0, 0, 0, 0, 0, 0, int64(0)},
			} {
				table, idColumn := "usage_events", "id"
				if event.retained {
					table, idColumn = "usage_monitoring_event_projection_v1", "event_id"
				}
				pricingSQLExec(t, db, "insert into "+table+"("+idColumn+`,timestamp_ms,model,requested_model,resolved_model,service_tier,input_tokens,normalized_total_input_tokens,output_tokens,cached_tokens,cache_tokens,cache_read_tokens,cache_creation_tokens,total_tokens,latency_ms,auth_file_snapshot,auth_index,account_snapshot)
					values(?,?,'display(high)','display(high)','bill','default',999999,?,?,?,?,?,?,?,?,'a.json','i','account')`,
					event.id, event.at, event.input, event.output, event.cached, event.cache, event.read, event.creation, event.input+event.output, event.latency)
				if event.retained {
					pricingSQLExec(t, db, `update usage_monitoring_event_projection_v1 set account_key=? where event_id=?`, key, event.id)
					pricingSQLExec(t, db, `insert into usage_archive_event_refs values(?,?,?,1)`, fmt.Sprint("archived-", event.id), event.id, event.at)
				}
			}
			ctx := context.Background()
			tx, err := sqlwrite.BeginTx(ctx, db, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			repo := New(db)
			filter := HourlyFilter{FromMS: 1, ToMS: 100, IncludeFailed: true, CollapseBuckets: true}
			hourly, err := repo.LoadHourlyRowsFromEventsTx(ctx, tx, filter)
			if err != nil || len(hourly) != 1 {
				t.Fatalf("hourly=%+v err=%v", hourly, err)
			}
			h := hourly[0]
			gotHourly := derivedPricingTokens(h.PricingBand, h.LongContextTokens, h.InputTokens, h.OutputTokens, h.CachedTokens, h.CacheReadTokens, h.CacheCreationTokens)
			if gotHourly != tc.want || h.Calls != 5 || h.TotalTokens != tc.total || h.LatencySumMS != 120 || h.LatencySamples != 3 || h.ZeroTokenCalls != tc.zeroCalls || h.Failed || h.Model != "display" || h.BillingModel != "bill" || h.ServiceTier != "default" || h.BucketMS != 0 {
				t.Fatalf("hourly=%+v tokens=%+v want=%+v", h, gotHourly, tc.want)
			}
			accounts, err := repo.LoadAccountRowsFromEventsTx(ctx, tx, []string{key})
			if err != nil || len(accounts) != 1 {
				t.Fatalf("accounts=%+v err=%v", accounts, err)
			}
			a := accounts[0]
			gotAccount := derivedPricingTokens(a.PricingBand, a.LongContextTokens, a.InputTokens, a.OutputTokens, a.CachedTokens, a.CacheReadTokens, a.CacheCreationTokens)
			if gotAccount != tc.want || a.Calls != 5 || a.SuccessCalls != 5 || a.FailureCalls != 0 || a.TotalTokens != tc.total || a.FirstSeenMS != 10 || a.LastSeenMS != 14 || a.AccountKey != key || a.Model != "display" || a.BillingModel != "bill" || a.ServiceTier != "default" {
				t.Fatalf("account=%+v tokens=%+v want=%+v", a, gotAccount, tc.want)
			}
			book := map[string]model.ModelPrice{"bill": {Prompt: 2, Completion: 3, Cache: 0.5, CacheRead: 0.25, CacheCreation: 4}}
			for _, tokens := range []pricingservice.ModelTokens{gotHourly, gotAccount} {
				cost := pricingservice.CostForModelCandidatesWithServiceTier([]string{"bill", "display"}, "default", tokens, book)
				// The oracle is a hand-computed constant. In the clamp case,
				// per-arm costing would incorrectly charge 0.00025, not 0.00005.
				if math.Abs(cost-tc.cost) > 1e-12 {
					t.Fatalf("cost=%g want=%g", cost, tc.cost)
				}
			}
			arms, err := retainedEventSourceArmsTx(ctx, tx)
			if err != nil {
				t.Fatal(err)
			}
			query, args := hourlyStatementFromArms(filter, 1, 100, 0, false, arms)
			assertDerivedPhysicalPricingGrain(t, tx, query, args)
			arms, err = retainedAccountEventSourceArmsTx(ctx, tx, []string{key})
			if err != nil {
				t.Fatal(err)
			}
			query, args = accountStatementFromArms(0, []string{key}, arms)
			assertDerivedPhysicalPricingGrain(t, tx, query, args)
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
