package usageevent

import (
	"context"
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func orderedModelStatsFixture() []ModelStat {
	stat := func(calls int64, model, billing, pricing string, threshold int64, tier string) ModelStat {
		return ModelStat{
			Model: model, BillingModel: billing, Calls: calls, ServiceTier: tier,
			PricingBand: usage.PricingBand{PricingModel: pricing, ContextThresholdTokens: threshold},
		}
	}
	// Expected order is explicit, not derived from the production comparator.
	// Each long name differs beyond MySQL's usual TEXT sort prefix. Embedded NULs
	// also make distinct tuples collide when concatenated with a NUL delimiter.
	prefix := strings.Repeat("界", 2048)
	stats := []ModelStat{
		stat(3, "z-busy", "b", "p", -1, ""),
		stat(2, "A", "b", "p", -1, ""),
		stat(2, "a", "b", "c", -1, "z"),
		stat(2, "a", "b", "c", 0, "z"),
		stat(2, "a", "b", "c", 2, ""),
		stat(2, "a", "b", "c", 2, "priority"),
		stat(2, "a", "b", "c", 10, ""),
		stat(2, "a", "b", "c", math.MaxInt64, ""),
		stat(2, "a", "b", "c\x00d", -1, ""),
		stat(2, "a", "b", "d", -1, ""),
		stat(2, "a", "b\x00c", "d", -1, ""),
		stat(2, "a", "long-"+prefix+"a", "p", -1, ""),
		stat(2, "a", "long-"+prefix+"b", "p", -1, ""),
		stat(2, "a", "pricing", prefix+"a", -1, ""),
		stat(2, "a", "pricing", prefix+"b", -1, ""),
		stat(2, "a", "tier", "p", -1, prefix+"a"),
		stat(2, "a", "tier", "p", -1, prefix+"b"),
		stat(2, "a\x00", "a", "a", -1, ""),
		stat(2, "é", "b", "p", -1, ""),
		stat(2, prefix+"a", "b", "p", -1, ""),
		stat(2, prefix+"b", "b", "p", -1, ""),
		stat(1, "0-quiet", "b", "p", -1, ""),
	}
	for i := range stats {
		n := int64(i + 1)
		stats[i].SuccessCalls = stats[i].Calls - 1
		stats[i].InputTokens = n
		stats[i].OutputTokens = 2 * n
		stats[i].ReasoningTokens = 3 * n
		stats[i].CachedTokens = 4 * n
		stats[i].CacheReadTokens = 5 * n
		stats[i].CacheCreationTokens = 6 * n
		stats[i].TotalTokens = 7 * n
		stats[i].LongContextTokens = usage.LongContextTokens{
			LongInputTokens: 8 * n, LongOutputTokens: 9 * n, LongCachedTokens: 10 * n,
			LongCacheReadTokens: 11 * n, LongCacheCreationTokens: 12 * n,
		}
	}
	return stats
}

func TestSortModelStatsFullGrainOrder(t *testing.T) {
	want := orderedModelStatsFixture()
	for shift := range want {
		for _, reverse := range []bool{false, true} {
			got := append(slices.Clone(want[shift:]), want[:shift]...)
			if reverse {
				slices.Reverse(got)
			}
			SortModelStats(got)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("shift=%d reverse=%v: full-grain order or metric values changed", shift, reverse)
			}
		}
	}
	var empty []ModelStat
	SortModelStats(empty)
	if empty != nil {
		t.Fatal("nil slice changed")
	}
}

func TestModelStatsReadersOnlySortUnlimitedResults(t *testing.T) {
	ctx := context.Background()
	filter := AnalyticsFilter{FromMS: 1, ToMS: 100, IncludeFailed: true}
	canonical := orderedModelStatsFixture()
	for _, reverse := range []bool{false, true} {
		input := append(slices.Clone(canonical[7:]), canonical[:7]...)
		if reverse {
			slices.Reverse(input)
		}
		for _, reader := range []struct {
			name    string
			limited bool
			read    func(*repository) ([]ModelStat, error)
		}{
			{"filtered-zero", false, func(r *repository) ([]ModelStat, error) { return r.ModelStatsWithFilter(ctx, filter, 0) }},
			{"filtered-negative", false, func(r *repository) ([]ModelStat, error) { return r.ModelStatsWithFilter(ctx, filter, -1) }},
			{"between", false, func(r *repository) ([]ModelStat, error) { return r.ModelStatsBetween(ctx, 1, 100) }},
			{"filtered-limited", true, func(r *repository) ([]ModelStat, error) { return r.ModelStatsWithFilter(ctx, filter, 2) }},
			{"top-between", true, func(r *repository) ([]ModelStat, error) { return r.TopModelsBetween(ctx, 1, 100, 2) }},
			{"top-default", true, func(r *repository) ([]ModelStat, error) { return r.TopModelsBetween(ctx, 1, 100, 0) }},
		} {
			t.Run(fmt.Sprintf("%s/reverse=%v", reader.name, reverse), func(t *testing.T) {
				repo, conn := newLatencyRoutingRepository(t)
				conn.query = func(context.Context, int, string, []driver.NamedValue) (*latencyRoutingRows, error) {
					rows := &latencyRoutingRows{columns: make([]string, 19)}
					for _, s := range input {
						rows.values = append(rows.values, []driver.Value{
							s.Model, s.BillingModel, s.PricingModel, s.ContextThresholdTokens, s.ServiceTier,
							s.Calls, s.SuccessCalls, s.InputTokens, s.OutputTokens, s.ReasoningTokens,
							s.CachedTokens, s.CacheReadTokens, s.CacheCreationTokens,
							s.LongInputTokens, s.LongOutputTokens, s.LongCachedTokens,
							s.LongCacheReadTokens, s.LongCacheCreationTokens, s.TotalTokens,
						})
					}
					return rows, nil
				}
				want := canonical
				if reader.limited {
					// Limited SQL owns model ranking and cutoff ties. Never apply the
					// unlimited per-band ordering to the rows it returns.
					want = input
				}
				got, err := reader.read(repo)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("reader changed full-grain order or metric values: err=%v", err)
				}
				if len(conn.queries) != 1 || conn.openRows.Load() != 0 {
					t.Fatalf("queries=%d openRows=%d", len(conn.queries), conn.openRows.Load())
				}
			})
		}
	}
}

func TestModelStatsUnlimitedOrderIndependentOfInsertion(t *testing.T) {
	stat := func(calls int64, name, billing, pricing string, threshold int64, tier string, input int64) ModelStat {
		return ModelStat{
			Model: name, BillingModel: billing, ServiceTier: tier, Calls: calls,
			SuccessCalls: calls, InputTokens: input,
			PricingBand: usage.PricingBand{PricingModel: pricing, ContextThresholdTokens: threshold},
		}
	}
	// The two display-priced suffixes share the same analytics/billing models.
	// Threshold must precede tier, and all fields must break equal-call ties.
	want := []ModelStat{
		stat(3, "z-busy", "z-busy", "z-busy", -1, "", 30),
		stat(1, "A", "A", "A", -1, "", 10),
		stat(1, "tie", "a-billing", "tie(high)", -1, "priority", 10),
		stat(1, "tie", "a-billing", "tie(high)", 100, "", 101),
		stat(1, "tie", "a-billing", "tie(high)", 100, "priority", 101),
		stat(1, "tie", "a-billing", "tie(high)", 200, "", 201),
		stat(1, "tie", "a-billing", "tie(low)", -1, "", 10),
		stat(1, "tie", "z-billing", "tie(high)", -1, "", 10),
		stat(1, "界a", "界a", "界a", -1, "", 10),
		stat(1, "界b", "界b", "界b", -1, "", 10),
	}
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%v", reverse), func(t *testing.T) {
			db := pricingTestDB(t)
			for _, name := range []string{"tie(high)", "tie(low)"} {
				pricingFixtureExec(t, db, `insert into model_prices(model,prompt_per_1m,completion_per_1m,cache_per_1m,updated_at_ms) values(?,1,0,0,1)`, name)
			}
			for _, threshold := range []int64{100, 200} {
				pricingFixtureExec(t, db, `insert into model_price_context_tiers(model,threshold_tokens) values('tie(high)',?)`, threshold)
			}
			input := slices.Clone(want)
			if reverse {
				slices.Reverse(input)
			}
			var timestamp int64
			for _, s := range input {
				requested := s.Model
				if s.Model == "tie" {
					requested = s.PricingModel
				}
				for call := int64(0); call < s.Calls; call++ {
					timestamp++
					insertPricingEvent(t, db, timestamp, "ignored-display", requested, s.BillingModel, s.ServiceTier, s.InputTokens/s.Calls, nil)
				}
			}
			ctx := context.Background()
			repo := New(db)
			for _, limit := range []int{0, -1} {
				got, err := repo.ModelStatsWithFilter(ctx, AnalyticsFilter{FromMS: 1, ToMS: timestamp + 1, IncludeFailed: true}, limit)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("filtered limit %d: got=%+v want=%+v err=%v", limit, got, want, err)
				}
			}
			got, err := repo.ModelStatsBetween(ctx, 1, timestamp+1)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("between: got=%+v want=%+v err=%v", got, want, err)
			}
		})
	}
}
