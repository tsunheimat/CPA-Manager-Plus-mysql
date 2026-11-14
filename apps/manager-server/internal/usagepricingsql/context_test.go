package usagepricingsql

import (
	"strings"
	"testing"
)

func TestContextIntervalFragmentContract(t *testing.T) {
	cte := ContextIntervalsCTE("catalog_intervals")
	if !strings.HasPrefix(cte, "catalog_intervals as (") || strings.Contains(cte, "usage_events") {
		t.Fatal("fragment must be one catalog-only definition", cte)
	}
	for _, required := range []string{"select model, threshold_tokens", "lead(threshold_tokens) over (partition by model order by threshold_tokens)", "as next_threshold_tokens", "from model_price_context_tiers"} {
		if !strings.Contains(cte, required) {
			t.Fatal("missing catalog interval contract", required, cte)
		}
	}
	join := ContextBandJoin("catalog_intervals", "band", "event.pricing_model", "event.normalized_input")
	for _, required := range []string{" left join catalog_intervals band", "band.model = event.pricing_model", "event.normalized_input > band.threshold_tokens", "event.normalized_input <= band.next_threshold_tokens or band.next_threshold_tokens is null"} {
		if !strings.Contains(join, required) {
			t.Fatal("missing strict-lower/inclusive-upper join", required, join)
		}
	}
	if !strings.HasPrefix(join, " ") || strings.Contains(join, "+ 1") || strings.Contains(join, "coalesce(") {
		t.Fatal("join must not increment thresholds or select the caller's base band", join)
	}
}
