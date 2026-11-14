package usageevent

import (
	"fmt"
	"strings"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usagepricingsql"
)

// pricedEventFields is an allowlist of consumer projections, not a list of
// request-supplied identifiers. Filters run on the source and do not widen these
// projections: in particular search, headers and latency filters add no payload
// or latency columns to heatmap pricing rows.
type pricedEventFields uint8

const (
	pricingModelFields pricedEventFields = iota
	pricingTimelineFields
	pricingAPIKeyTimelineFields
	pricingHeatmapFields
	pricingChannelFields
	pricingAccountFields
	pricingAPIKeyFields
	pricingCredentialFields
	pricingCredentialTimelineFields
	pricingAccountWindowFields
)

func (fields pricedEventFields) columns() []string {
	columns := []string{
		"normalized_total_input_tokens", "input_tokens", "output_tokens",
		"cached_tokens", "cache_tokens", "cache_read_tokens", "cache_creation_tokens",
		"total_tokens", "failed", "service_tier",
	}
	channel := []string{"auth_index", "source", "account_snapshot", "auth_label_snapshot", "auth_provider_snapshot", "provider", "auth_account_id_snapshot", "latency_ms"}
	credential := []string{"timestamp_ms", "latency_ms", "auth_file_snapshot", "auth_index", "source", "source_hash", "account_snapshot", "auth_label_snapshot", "auth_provider_snapshot", "provider", "auth_account_id_snapshot", "auth_project_id_snapshot"}
	switch fields {
	case pricingModelFields:
		return append(columns, "reasoning_tokens")
	case pricingTimelineFields:
		return append(columns, "timestamp_ms", "reasoning_tokens", "latency_ms")
	case pricingAPIKeyTimelineFields:
		return append(columns, "timestamp_ms", "reasoning_tokens", "latency_ms", "api_key_hash")
	case pricingHeatmapFields:
		return append(columns, "timestamp_ms", "api_key_hash", "provider", "auth_provider_snapshot")
	case pricingChannelFields:
		return append(columns, channel...)
	case pricingAccountFields:
		return append(append(columns, channel...), "timestamp_ms", "source_hash")
	case pricingAPIKeyFields:
		return append(append(columns, channel...), "timestamp_ms", "source_hash", "api_key_hash")
	case pricingCredentialFields:
		return append(columns, credential...)
	case pricingCredentialTimelineFields:
		return append(append(columns, credential...), "reasoning_tokens")
	case pricingAccountWindowFields:
		return append(columns, "timestamp_ms", "request_index")
	default:
		panic("unknown priced event projection")
	}
}

func pricingBandedUsageEventsCTEWithBaseFilter(baseFilter string, fields pricedEventFields) string {
	return "with " + pricingBandedUsageEventsCTEs("usage_events", baseFilter, fields)
}

// pricingBandedUsageEventsCTEs requires the complete source predicate and an
// explicit consumer projection. baseFrom/baseFilter are internal SQL only; all
// filter values stay bound. The account-window caller supplies its target join
// here so only matching target-event pairs reach pricing (overlaps are intended).
// The event chain is single-use, projection/filter/join-only. Only the catalog
// relation needs a window; whether the engine merges the event CTEs is plan-tested.
func pricingBandedUsageEventsCTEs(baseFrom, baseFilter string, fields pricedEventFields) string {
	baseFilter = strings.TrimPrefix(baseFilter, "where ")
	if strings.TrimSpace(baseFilter) == "" {
		panic("priced events require an explicit source predicate")
	}
	columns := fields.columns()
	project := func(alias string) string {
		qualified := make([]string, len(columns))
		for i, column := range columns {
			qualifier := alias
			if alias == "usage_events" && column == "request_index" {
				qualifier = "w"
			}
			qualified[i] = qualifier + "." + column
		}
		return strings.Join(qualified, ", ")
	}
	return fmt.Sprintf(`pricing_base_events as (
	select %s,
		%s as requested_model_value,
		%s as analytics_model_value,
		coalesce(nullif(resolved_model, ''), %s) as billing_model_value,
		coalesce(normalized_total_input_tokens, input_tokens, 0) as normalized_input_tokens_value
	from %s
	where %s
), pricing_resolved_events as (
	select %s,
		pricing_base_events.analytics_model_value,
		pricing_base_events.billing_model_value,
		pricing_base_events.normalized_input_tokens_value,
		case
			when billing_price.model is not null then pricing_base_events.billing_model_value
			when analytics_price.model is not null then pricing_base_events.analytics_model_value
			when display_price.model is not null then pricing_base_events.requested_model_value
			else pricing_base_events.billing_model_value
		end as pricing_model_value
	from pricing_base_events
	left join model_prices billing_price on billing_price.model = pricing_base_events.billing_model_value
	left join model_prices analytics_price on billing_price.model is null
		and analytics_price.model = pricing_base_events.analytics_model_value
	left join model_prices display_price on billing_price.model is null and analytics_price.model is null
		and display_price.model = pricing_base_events.requested_model_value
), %s, banded_usage_events as (
	select %s,
		pricing_resolved_events.analytics_model_value,
		pricing_resolved_events.billing_model_value,
		pricing_resolved_events.pricing_model_value,
		coalesce(tier.threshold_tokens, %d) as context_threshold_tokens_value
	from pricing_resolved_events%s
)`, project("usage_events"), requestedModelExpr, analyticsModelExpr, analyticsModelExpr,
		baseFrom, baseFilter, project("pricing_base_events"),
		usagepricingsql.ContextIntervalsCTE("pricing_context_intervals"), project("pricing_resolved_events"),
		model.ModelPriceBaseContextThreshold,
		usagepricingsql.ContextBandJoin("pricing_context_intervals", "tier", "pricing_resolved_events.pricing_model_value", "pricing_resolved_events.normalized_input_tokens_value"))
}

// modelPricingBandsSQL aggregates once at the complete monetary grain. Limited
// readers select models before this step so excluded token sums cannot overflow.
var modelPricingBandsSQL = `select
	analytics_model_value as model, billing_model_value as billing_model,
	pricing_model_value, context_threshold_tokens_value, coalesce(service_tier, '') as service_tier,
	count(*) as calls, sum(case when failed = 0 then 1 else 0 end) as success,
	coalesce(sum(` + normalizedInputExpr + `), 0) as input_tokens,
	coalesce(sum(output_tokens), 0) as output_tokens,
	coalesce(sum(reasoning_tokens), 0) as reasoning_tokens,
	coalesce(sum(` + compatCachedExpr + `), 0) as cached_tokens,
	coalesce(sum(cache_read_tokens), 0) as cache_read_tokens,
	coalesce(sum(cache_creation_tokens), 0) as cache_creation_tokens,
	coalesce(sum(` + longInputExpr + `), 0) as long_input_tokens,
	coalesce(sum(` + longOutputExpr + `), 0) as long_output_tokens,
	coalesce(sum(` + longCachedExpr + `), 0) as long_cached_tokens,
	coalesce(sum(` + longCacheReadExpr + `), 0) as long_cache_read_tokens,
	coalesce(sum(` + longCacheCreationExpr + `), 0) as long_cache_creation_tokens,
	coalesce(sum(total_tokens), 0) as total_tokens
from banded_usage_events
group by analytics_model_value, billing_model_value, pricing_model_value, context_threshold_tokens_value, coalesce(service_tier, '')`

func topModelPricingBandsSQL(baseFilter string) string {
	baseFilter = strings.TrimPrefix(baseFilter, "where ")
	// Count on a narrow, filtered raw scan before pricing or token aggregation.
	// In particular, an excluded model's SUM(MaxInt64+1) must not fail a valid
	// limited result. Reuse only the count relation, not event-grain pricing.
	// Both raw scans use the complete filter and share one statement snapshot.
	return `with top_models as (
	select ` + analyticsModelExpr + ` as selected_model, count(*) as model_calls
	from usage_events
	where ` + baseFilter + `
	group by ` + analyticsModelExpr + `
	order by model_calls desc
	limit ?
), ` + pricingBandedUsageEventsCTEs("usage_events join top_models on top_models.selected_model = "+analyticsModelExpr, baseFilter, pricingModelFields) + `, model_bands as (
` + modelPricingBandsSQL + `
)
select b.model, b.billing_model, b.pricing_model_value, b.context_threshold_tokens_value, b.service_tier,
	b.calls, b.success, b.input_tokens, b.output_tokens, b.reasoning_tokens,
	b.cached_tokens, b.cache_read_tokens, b.cache_creation_tokens,
	b.long_input_tokens, b.long_output_tokens, b.long_cached_tokens, b.long_cache_read_tokens, b.long_cache_creation_tokens,
	b.total_tokens
from model_bands b join top_models t on t.selected_model = b.model
order by t.model_calls desc, b.model, b.calls desc`
}

func topModelPricingArgs(filterArgs []any, limit int) []any {
	args := make([]any, 0, 2*len(filterArgs)+1)
	args = append(args, filterArgs...)
	args = append(args, limit)
	return append(args, filterArgs...)
}
