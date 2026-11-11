package usageevent

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

const groupedPricingDimensions = `analytics_model_value, billing_model_value, pricing_model_value, context_threshold_tokens_value, coalesce(service_tier, '')`

// All sample-to-additive conversion happens before transfer. Pricing still uses
// each event's band and normalized tokens, never a band inferred from a sum.
func groupedAnalyticsSQL(filter AnalyticsFilter, plan analyticsSQLBuckets, mode string) (string, []any) {
	where, args := analyticsWhere(filter)
	dimensions := plan.bucket + ", " + groupedPricingDimensions
	switch mode {
	case "api":
		dimensions = "coalesce(api_key_hash, ''), " + dimensions
	case "heatmap":
		dimensions = plan.weekday + ", " + plan.hour + ", coalesce(api_key_hash, ''), coalesce(nullif(auth_provider_snapshot, ''), provider, ''), " + dimensions
	case "credential":
		dimensions = credentialIDExpr + `, coalesce(auth_file_snapshot,''), coalesce(auth_index,''), coalesce(source,''), coalesce(source_hash,''), coalesce(account_snapshot,''), coalesce(auth_label_snapshot,''), coalesce(nullif(auth_provider_snapshot,''),provider,''), coalesce(auth_account_id_snapshot,''), coalesce(auth_project_id_snapshot,''), ` + dimensions
	}
	query := pricingBandedUsageEventsCTEWithBaseFilter(strings.TrimPrefix(where, "where ")) + plan.cte + ` select ` + dimensions + `,
 count(*), coalesce(sum(total_tokens),0), sum(case when failed = 0 then 1 else 0 end), sum(case when failed != 0 then 1 else 0 end),
 coalesce(sum(` + normalizedInputExpr + `),0), coalesce(sum(output_tokens),0), coalesce(sum(reasoning_tokens),0),
 coalesce(sum(` + compatCachedExpr + `),0), coalesce(sum(cache_read_tokens),0), coalesce(sum(cache_creation_tokens),0),
 coalesce(sum(` + longInputExpr + `),0), coalesce(sum(` + longOutputExpr + `),0), coalesce(sum(` + longCachedExpr + `),0), coalesce(sum(` + longCacheReadExpr + `),0), coalesce(sum(` + longCacheCreationExpr + `),0),
 sum(case when latency_ms > 0 then latency_ms end), count(case when latency_ms > 0 then 1 end)
 from banded_usage_events` + plan.join + ` group by ` + dimensions + ` order by min(timestamp_ms), analytics_model_value, billing_model_value, pricing_model_value, context_threshold_tokens_value, coalesce(service_tier, '')`
	return query, append(args, plan.args...)
}

func timelineScanTargets(p *TimelinePoint) []any {
	return []any{&p.BucketMS, &p.Model, &p.BillingModel, &p.PricingModel, &p.ContextThresholdTokens, &p.ServiceTier, &p.Calls, &p.Tokens, &p.Success, &p.Failure, &p.InputTokens, &p.OutputTokens, &p.ReasoningTokens, &p.CachedTokens, &p.CacheReadTokens, &p.CacheCreationTokens, &p.LongInputTokens, &p.LongOutputTokens, &p.LongCachedTokens, &p.LongCacheReadTokens, &p.LongCacheCreationTokens, &p.AvgLatencyMS, &p.LatencySamples}
}

// MySQL AVG(integer) rounds a DECIMAL result to its configured division scale.
// Transfer the additive sum and sample count, then divide exactly as the Go
// reference does; hourly/dimension averages must not silently lose precision.
func scanGroupedTimeline(rows *sql.Rows, point *TimelinePoint, prefix ...any) error {
	if err := rows.Scan(append(prefix, timelineScanTargets(point)...)...); err != nil {
		return err
	}
	if point.LatencySamples > 0 {
		point.AvgLatencyMS.Float64 /= float64(point.LatencySamples)
	}
	return nil
}

func (r *repository) groupedAnalyticsRows(ctx context.Context, filter AnalyticsFilter, granularity string, location *time.Location, mode string) (*sql.Rows, bool, error) {
	if !r.mysqlAnalytics || !mysql.IsMySQL(r.db) {
		return nil, false, nil
	}
	plan, available, err := analyticsSQLBucketPlan(ctx, filter.FromMS, filter.ToMS, granularity, location, mode == "heatmap")
	if err != nil || !available {
		return nil, available, err
	}
	query, args := groupedAnalyticsSQL(filter, plan, mode)
	query, args, err = mysql.CompileSQL(query, args...)
	if err != nil {
		return nil, true, err
	}
	rows, err := r.db.QueryContext(ctx, mysql.Native(query), args...)
	return rows, true, err
}

func (r *repository) groupedTimeline(ctx context.Context, filter AnalyticsFilter, granularity string, location *time.Location) ([]TimelinePoint, bool, error) {
	rows, available, err := r.groupedAnalyticsRows(ctx, filter, granularity, location, "timeline")
	if err != nil || !available {
		return nil, available, err
	}
	defer rows.Close()
	result := []TimelinePoint{}
	for rows.Next() {
		var p TimelinePoint
		if err := scanGroupedTimeline(rows, &p); err != nil {
			return nil, true, err
		}
		result = append(result, p)
	}
	return result, true, rows.Err()
}

func (r *repository) groupedAPIKeyTimeline(ctx context.Context, filter AnalyticsFilter, granularity string, location *time.Location) ([]APIKeyTimelinePoint, bool, error) {
	rows, available, err := r.groupedAnalyticsRows(ctx, filter, granularity, location, "api")
	if err != nil || !available {
		return nil, available, err
	}
	result, err := readGroupedAPIKeyTimeline(rows)
	return result, true, err
}

func readGroupedAPIKeyTimeline(rows *sql.Rows) ([]APIKeyTimelinePoint, error) {
	defer rows.Close()
	result := []APIKeyTimelinePoint{}
	for rows.Next() {
		var p TimelinePoint
		var key string
		if err := scanGroupedTimeline(rows, &p, &key); err != nil {
			return nil, err
		}
		result = append(result, APIKeyTimelinePoint{APIKeyHash: key, LongContextTokens: p.LongContextTokens, PricingBand: p.PricingBand, BucketMS: p.BucketMS, Model: p.Model, BillingModel: p.BillingModel, ServiceTier: p.ServiceTier, Calls: p.Calls, Tokens: p.Tokens, Success: p.Success, Failure: p.Failure, InputTokens: p.InputTokens, OutputTokens: p.OutputTokens, ReasoningTokens: p.ReasoningTokens, CachedTokens: p.CachedTokens, CacheReadTokens: p.CacheReadTokens, CacheCreationTokens: p.CacheCreationTokens, AvgLatencyMS: p.AvgLatencyMS, LatencySamples: p.LatencySamples})
	}
	return result, rows.Err()
}

func (r *repository) groupedHeatmap(ctx context.Context, filter AnalyticsFilter, location *time.Location) ([]HeatmapPoint, bool, error) {
	rows, available, err := r.groupedAnalyticsRows(ctx, filter, "hour", location, "heatmap")
	if err != nil || !available {
		return nil, available, err
	}
	result, err := readGroupedHeatmap(rows)
	return result, true, err
}

func readGroupedHeatmap(rows *sql.Rows) ([]HeatmapPoint, error) {
	defer rows.Close()
	// The SQL grain includes the exact bucket for sharing the timeline scanner.
	// Merge different dates with the same weekday/hour into their final heatmap.
	type key struct {
		weekday, hour                                int
		api, provider, model, billing, pricing, tier string
		threshold                                    int64
	}
	index := map[key]int{}
	result := []HeatmapPoint{}
	for rows.Next() {
		var p TimelinePoint
		var h HeatmapPoint
		if err := scanGroupedTimeline(rows, &p, &h.Weekday, &h.Hour, &h.APIKeyHash, &h.Provider); err != nil {
			return nil, err
		}
		k := key{h.Weekday, h.Hour, h.APIKeyHash, h.Provider, p.Model, p.BillingModel, p.PricingModel, p.ServiceTier, p.ContextThresholdTokens}
		i, ok := index[k]
		if !ok {
			i = len(result)
			index[k] = i
			h.Model = p.Model
			h.BillingModel = p.BillingModel
			h.ServiceTier = p.ServiceTier
			h.PricingBand = p.PricingBand
			result = append(result, h)
		}
		out := &result[i]
		out.Calls += p.Calls
		out.SuccessCalls += p.Success
		out.FailureCalls += p.Failure
		out.InputTokens += p.InputTokens
		out.OutputTokens += p.OutputTokens
		out.CachedTokens += p.CachedTokens
		out.CacheReadTokens += p.CacheReadTokens
		out.CacheCreationTokens += p.CacheCreationTokens
		out.TotalTokens += p.Tokens
		out.LongInputTokens += p.LongInputTokens
		out.LongOutputTokens += p.LongOutputTokens
		out.LongCachedTokens += p.LongCachedTokens
		out.LongCacheReadTokens += p.LongCacheReadTokens
		out.LongCacheCreationTokens += p.LongCacheCreationTokens
	}
	return result, rows.Err()
}

func hourlyDistributionSQL(filter AnalyticsFilter, plan analyticsSQLBuckets) (string, []any) {
	where, args := analyticsWhere(filter)
	query := `with filtered_events as (select timestamp_ms,total_tokens from usage_events ` + where + `)` + plan.cte + ` select ` + plan.hour + `,count(*),coalesce(sum(total_tokens),0) from filtered_events` + plan.join + ` group by ` + plan.hour + ` order by ` + plan.hour
	return query, append(args, plan.args...)
}

func (r *repository) groupedHourlyDistribution(ctx context.Context, filter AnalyticsFilter, location *time.Location) ([]HourlyPoint, bool, error) {
	if !r.mysqlAnalytics || !mysql.IsMySQL(r.db) {
		return nil, false, nil
	}
	plan, available, err := analyticsSQLBucketPlan(ctx, filter.FromMS, filter.ToMS, "hour", location, true)
	if err != nil || !available {
		return nil, available, err
	}
	query, args := hourlyDistributionSQL(filter, plan)
	query, args, err = mysql.CompileSQL(query, args...)
	if err != nil {
		return nil, true, err
	}
	rows, err := r.db.QueryContext(ctx, mysql.Native(query), args...)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	result := []HourlyPoint{}
	for rows.Next() {
		var p HourlyPoint
		if err := rows.Scan(&p.Hour, &p.Calls, &p.Tokens); err != nil {
			return nil, true, err
		}
		result = append(result, p)
	}
	return result, true, rows.Err()
}

func (r *repository) groupedCredentialTimeline(ctx context.Context, filter AnalyticsFilter, granularity string, location *time.Location) ([]CredentialTimelinePoint, bool, error) {
	rows, available, err := r.groupedAnalyticsRows(ctx, filter, granularity, location, "credential")
	if err != nil || !available {
		return nil, available, err
	}
	result, err := readGroupedCredentialTimeline(rows)
	return result, true, err
}

func readGroupedCredentialTimeline(rows *sql.Rows) ([]CredentialTimelinePoint, error) {
	defer rows.Close()
	result := []CredentialTimelinePoint{}
	for rows.Next() {
		var p TimelinePoint
		var c CredentialTimelinePoint
		targets := []any{&c.ID, &c.AuthFileSnapshot, &c.AuthIndex, &c.Source, &c.SourceHash, &c.AccountSnapshot, &c.AuthLabelSnapshot, &c.AuthProviderSnapshot, &c.AuthAccountIDSnapshot, &c.AuthProjectIDSnapshot}
		if err := scanGroupedTimeline(rows, &p, targets...); err != nil {
			return nil, err
		}
		c.LongContextTokens = p.LongContextTokens
		c.PricingBand = p.PricingBand
		c.BucketMS = p.BucketMS
		c.Model = p.Model
		c.BillingModel = p.BillingModel
		c.ServiceTier = p.ServiceTier
		c.Calls = p.Calls
		c.Tokens = p.Tokens
		c.Success = p.Success
		c.Failure = p.Failure
		c.InputTokens = p.InputTokens
		c.OutputTokens = p.OutputTokens
		c.ReasoningTokens = p.ReasoningTokens
		c.CachedTokens = p.CachedTokens
		c.CacheReadTokens = p.CacheReadTokens
		c.CacheCreationTokens = p.CacheCreationTokens
		c.AvgLatencyMS = p.AvgLatencyMS
		c.LatencySamples = p.LatencySamples
		c.AuthProjectIDSnapshot = usageidentity.ProjectIDSnapshot(c.AuthProviderSnapshot, c.AuthProjectIDSnapshot)
		result = append(result, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// SQL retains the full provenance grain. Fold to the legacy credential grain,
	// keeping the first event's snapshots even when they are empty.
	first := make(map[credentialNativeKey]CredentialTimelinePoint)
	for _, point := range result {
		key := credentialNativeIdentity(point)
		if _, ok := first[key]; !ok {
			first[key] = point
		}
	}
	result = mergeCredentialTimelineParts([][]CredentialTimelinePoint{result})
	for i := range result {
		point := &result[i]
		original := first[credentialNativeIdentity(*point)]
		point.Source = original.Source
		point.AccountSnapshot = original.AccountSnapshot
		point.AuthLabelSnapshot = original.AuthLabelSnapshot
		point.AuthProviderSnapshot = original.AuthProviderSnapshot
		point.AuthAccountIDSnapshot = original.AuthAccountIDSnapshot
		point.AuthProjectIDSnapshot = original.AuthProjectIDSnapshot
	}
	return result, nil
}

type credentialNativeKey struct {
	id, file, index, sourceHash, model, billing, pricing, tier string
	bucket, threshold                                          int64
}

func credentialNativeIdentity(p CredentialTimelinePoint) credentialNativeKey {
	return credentialNativeKey{p.ID, p.AuthFileSnapshot, p.AuthIndex, p.SourceHash, p.Model, p.BillingModel, p.PricingModel, p.ServiceTier, p.BucketMS, p.ContextThresholdTokens}
}
