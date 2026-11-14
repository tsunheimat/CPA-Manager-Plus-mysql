package usagemonitoring

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestSortedDailyModelStatsFullGrainOrder(t *testing.T) {
	stat := func(calls int64, model, billing, pricing string, threshold int64, tier string) ModelStat {
		return ModelStat{
			Model: model, BillingModel: billing, Calls: calls, ServiceTier: tier,
			PricingBand: usage.PricingBand{PricingModel: pricing, ContextThresholdTokens: threshold},
		}
	}
	prefix := strings.Repeat("界", 2048)
	// Hand-ordered full tuples; do not sort an expected value with production code.
	want := []ModelStat{
		stat(2, "z", "z", "z", 10, "priority"),
		stat(1, "a", "b", "c", -1, "z"),
		stat(1, "a", "b", "c", 0, "z"),
		stat(1, "a", "b", "c", 2, "a"),
		stat(1, "a", "b", "c", 2, "z"),
		stat(1, "a", "b", "c", 10, ""),
		stat(1, "a", "b", "c\x00d", -1, ""),
		stat(1, "a", "b\x00c", "d", -1, ""),
		stat(1, "a", "billing-"+prefix+"a", "p", -1, ""),
		stat(1, "a", "billing-"+prefix+"b", "p", -1, ""),
		stat(1, "a", "pricing", prefix+"a", -1, ""),
		stat(1, "a", "pricing", prefix+"b", -1, ""),
		stat(1, "a", "tier", "p", -1, prefix+"a"),
		stat(1, "a", "tier", "p", -1, prefix+"b"),
		stat(1, "a\x00", "a", "a", -1, ""),
		stat(1, prefix+"a", "b", "p", -1, ""),
		stat(1, prefix+"b", "b", "p", -1, ""),
	}
	for i := range want {
		want[i].InputTokens = int64(i + 1)
		want[i].OutputTokens = int64(2 * (i + 1))
		want[i].LongInputTokens = int64(3 * (i + 1))
	}
	for shift := range want {
		t.Run(fmt.Sprint(shift), func(t *testing.T) {
			input := append(slices.Clone(want[shift:]), want[:shift]...)
			slices.Reverse(input)
			grouped := make(map[dailyModelStatKey]*ModelStat, len(input))
			for _, row := range input {
				key := dailyModelStatKey{
					model: row.Model, billingModel: row.BillingModel, pricingModel: row.PricingModel,
					contextThreshold: row.ContextThresholdTokens, serviceTier: row.ServiceTier,
				}
				grouped[key] = &row
			}
			if got := sortedDailyModelStats(grouped); !reflect.DeepEqual(got, want) {
				t.Fatal("projected stats changed full-grain order or metric values")
			}
		})
	}
	if got := sortedDailyModelStats(nil); got == nil || len(got) != 0 {
		t.Fatalf("empty result = %#v, want non-nil empty slice", got)
	}
}
