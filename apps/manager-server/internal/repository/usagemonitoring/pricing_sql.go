package usagemonitoring

import (
	"fmt"
	"strings"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageprojection"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usagepricingsql"
)

// These are internal projection profiles, never request-supplied identifiers.
var monitoringPricingSourceFields = []string{
	"model", "analytics_model", "resolved_model", "service_tier", "failed",
	"normalized_total_input_tokens", "output_tokens", "cached_tokens", "cache_tokens",
	"cache_read_tokens", "cache_creation_tokens", "total_tokens",
}

var monitoringAccountFields = []string{
	"timestamp_ms", "account_snapshot", "auth_label_snapshot", "provider",
	"auth_provider_snapshot", "auth_account_id_snapshot", "auth_index", "source", "source_hash", "latency_ms",
}

func monitoringPricingSourceSelects(extra []string) (string, string) {
	fields := append(append([]string{}, monitoringPricingSourceFields...), extra...)
	projected, raw := make([]string, 0, len(fields)), make([]string, 0, len(fields))
	for _, field := range fields {
		p, e := "p."+field, "e."+field
		switch field {
		case "model":
			p = "p.requested_model"
			e = usageidentity.SQLEffectiveRequestedModelExpression("e.model", "e.requested_model")
		case "analytics_model":
			e = usageidentity.SQLRequestAnalyticsModelExpression("e.model", "e.requested_model")
		case "normalized_total_input_tokens":
			e = "coalesce(e.normalized_total_input_tokens, e.input_tokens, 0)"
		case "failed", "output_tokens", "reasoning_tokens", "cached_tokens", "cache_tokens", "cache_read_tokens", "cache_creation_tokens", "total_tokens":
			e = "coalesce(" + e + ", 0)"
		case "timestamp_ms", "latency_ms":
		default:
			e = "coalesce(" + e + ", '')"
		}
		projected = append(projected, p+" as "+field)
		raw = append(raw, e+" as "+field)
	}
	return strings.Join(projected, ", "), strings.Join(raw, ", ")
}

func monitoringBandedProjectedEventsCTE(source string, extra ...string) string {
	fields := append([]string{
		"analytics_model", "service_tier", "failed", "normalized_total_input_tokens",
		"output_tokens", "cache_read_tokens", "cache_creation_tokens", "total_tokens",
	}, extra...)
	baseFields := "base_events." + strings.Join(fields, ", base_events.")
	pricedFields := "priced_events." + strings.Join(fields, ", priced_events.")
	return fmt.Sprintf(`with %s, base_events as (%s), priced_events as (
		select %s,
			coalesce(nullif(base_events.resolved_model, ''), base_events.analytics_model) as billing_model_value,
			case
				when billing_price.model is not null then coalesce(nullif(base_events.resolved_model, ''), base_events.analytics_model)
				when analytics_price.model is not null then base_events.analytics_model
				when display_price.model is not null then base_events.model
				else coalesce(nullif(base_events.resolved_model, ''), base_events.analytics_model)
			end as pricing_model_value,
			max(max(coalesce(cached_tokens, 0), coalesce(cache_tokens, 0)) - max(coalesce(cache_read_tokens, 0), 0) - max(coalesce(cache_creation_tokens, 0), 0), 0) as compatible_cached_tokens_value,
			normalized_total_input_tokens as normalized_input_tokens_value
		from base_events
		left join model_prices billing_price on billing_price.model = coalesce(nullif(base_events.resolved_model, ''), base_events.analytics_model)
		left join model_prices analytics_price on billing_price.model is null and analytics_price.model = base_events.analytics_model
		left join model_prices display_price on billing_price.model is null and analytics_price.model is null and display_price.model = base_events.model
	), banded_events as (
		select %s, priced_events.billing_model_value, priced_events.pricing_model_value,
			priced_events.compatible_cached_tokens_value, priced_events.normalized_input_tokens_value,
			coalesce(tier.threshold_tokens, %d) as context_threshold_tokens_value
		from priced_events%s
	)`, usagepricingsql.ContextIntervalsCTE("pricing_context_intervals"), source, baseFields, pricedFields,
		model.ModelPriceBaseContextThreshold,
		usagepricingsql.ContextBandJoin("pricing_context_intervals", "tier", "priced_events.pricing_model_value", "priced_events.normalized_input_tokens_value"))
}

// Each disjoint watermark arm is classified and aggregated before UNION ALL.
// Only aggregate rows are materialized; their SQL merge preserves the complete
// pricing/provenance grain, weighted latency, and representative MAX values.
func monitoringPricingArmsQuery(arms []eventSourceArm, extra []string, aggregate string, aggregateArgs []any, columns []usageprojection.AggregateColumn) (string, []any) {
	queries := make([]string, 0, len(arms))
	var args []any
	for _, arm := range arms {
		queries = append(queries, monitoringBandedProjectedEventsCTE(arm.SQL, extra...)+aggregate)
		args = append(args, arm.Args...)
		args = append(args, aggregateArgs...)
	}
	return usageprojection.MergeAggregateQueries(queries, columns), args
}

func monitoringAggregateColumns(dimensions []usageprojection.AggregateColumn, metrics string, lastSeen, latency bool) []usageprojection.AggregateColumn {
	columns := append([]usageprojection.AggregateColumn{}, dimensions...)
	for _, name := range strings.Fields(metrics) {
		columns = append(columns, usageprojection.AggregateColumn{Name: name, Merge: "sum"})
	}
	if lastSeen {
		columns = append(columns, usageprojection.AggregateColumn{Name: "last_seen_ms", Merge: "max"})
	}
	if latency {
		columns = append(columns, usageprojection.AggregateColumn{Name: "latency_sum_ms", Merge: "sum"}, usageprojection.AggregateColumn{Name: "latency_samples", Merge: "sum"})
	}
	return columns
}

const monitoringDimensionMeasures = "calls success_calls failure_calls input_tokens output_tokens cached_tokens cache_read_tokens cache_creation_tokens long_input_tokens long_output_tokens long_cached_tokens long_cache_read_tokens long_cache_creation_tokens total_tokens"

var monitoringModelAggregateColumns = monitoringAggregateColumns([]usageprojection.AggregateColumn{
	{Name: "model"}, {Name: "billing_model"}, {Name: "pricing_model"}, {Name: "context_threshold"}, {Name: "service_tier"},
}, "calls success_calls input_tokens output_tokens reasoning_tokens cached_tokens cache_read_tokens cache_creation_tokens long_input_tokens long_output_tokens long_cached_tokens long_cache_read_tokens long_cache_creation_tokens total_tokens", false, false)

var monitoringAccountAggregateColumns = monitoringAggregateColumns([]usageprojection.AggregateColumn{
	{Name: "account_snapshot"}, {Name: "auth_label_snapshot"}, {Name: "preferred_provider"},
	{Name: "provider", Merge: "max"}, {Name: "auth_provider_snapshot", Merge: "max"}, {Name: "auth_account_id_snapshot"},
	{Name: "auth_index"}, {Name: "source", Merge: "max"}, {Name: "source_hash"},
	{Name: "model"}, {Name: "billing_model"}, {Name: "pricing_model"}, {Name: "context_threshold"}, {Name: "service_tier"},
}, monitoringDimensionMeasures, true, true)

var monitoringAPIKeyAggregateColumns = monitoringAggregateColumns([]usageprojection.AggregateColumn{
	{Name: "api_key_hash"}, {Name: "account_snapshot"}, {Name: "auth_label_snapshot"}, {Name: "preferred_provider"},
	{Name: "auth_account_id_snapshot"}, {Name: "auth_index"}, {Name: "source", Merge: "max"}, {Name: "source_hash"},
	{Name: "model"}, {Name: "billing_model"}, {Name: "pricing_model"}, {Name: "context_threshold"}, {Name: "service_tier"},
}, monitoringDimensionMeasures, true, true)

var monitoringWindowAggregateColumns = monitoringAggregateColumns([]usageprojection.AggregateColumn{
	{Name: "request_index"}, {Name: "model"}, {Name: "billing_model"}, {Name: "pricing_model"}, {Name: "context_threshold"}, {Name: "service_tier"},
}, monitoringDimensionMeasures, true, false)
