package monitoring

import (
	"errors"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
)

type PercentilesInclude struct {
	Summary  bool `json:"summary"`
	Timeline bool `json:"timeline"`
}

type PercentileSummary struct {
	P95LatencyMS *float64 `json:"p95_latency_ms"`
	P95TTFTMS    *float64 `json:"p95_ttft_ms"`
}

type PercentileTimelinePoint struct {
	BucketMS     int64    `json:"bucket_ms"`
	P95LatencyMS *float64 `json:"p95_latency_ms"`
	P95TTFTMS    *float64 `json:"p95_ttft_ms"`
}

type PercentilesResponse struct {
	Summary *PercentileSummary `json:"summary,omitempty"`
	// Pointer preserves a requested empty timeline as [] rather than omitting it.
	Timeline *[]PercentileTimelinePoint `json:"timeline,omitempty"`
}

type analyticsPlan struct {
	modelPrices               bool
	latencySummary            bool
	latencyTimeline           bool
	legacySummaryPercentiles  bool
	legacyTimelinePercentiles bool
}

func planAnalytics(include Include) analyticsPlan {
	plan := analyticsPlan{
		legacySummaryPercentiles:  include.Summary && (include.SummaryProfile != "compact" || include.SummaryPercentiles),
		legacyTimelinePercentiles: include.Timeline && include.TimelineProfile != "core",
	}
	// Standalone raw percentiles do not read pricing or pricing generations.
	// Keep the existing snapshot contract for every other section. In particular
	// anomalies also build a priced timeline, even without include.timeline.
	standalonePercentiles := include.Percentiles != nil && (include.Percentiles.Summary || include.Percentiles.Timeline) &&
		!include.Summary && !include.Timeline && !include.HourlyDistribution && !include.ModelShare &&
		!include.ChannelShare && !include.ModelStats && !include.FailureSources && !include.AccountStats &&
		!include.CredentialStats && !include.CredentialTimeline && !include.APIKeyTimeline && !include.APIKeyStats &&
		!include.FilterOptions && !include.FilterSelectors && !include.Heatmap && !include.AnomalyPoints &&
		!include.TaskBuckets && include.RecentFailures <= 0 && include.EventsPage == nil && include.DrilldownPreview == nil
	plan.modelPrices = !standalonePercentiles
	plan.latencySummary = plan.legacySummaryPercentiles || (include.Percentiles != nil && include.Percentiles.Summary)
	plan.latencyTimeline = plan.legacyTimelinePercentiles || include.AnomalyPoints || (include.Percentiles != nil && include.Percentiles.Timeline)
	return plan
}

func ValidateInclude(include Include) error {
	if include.TimelineProfile != "" && include.TimelineProfile != "core" && include.TimelineProfile != "full" {
		return errors.New("include.timeline_profile must be core or full")
	}
	return nil
}

func buildPercentiles(include *PercentilesInclude, summary store.LatencySummary, timeline []store.LatencyPercentiles) *PercentilesResponse {
	if include == nil || (!include.Summary && !include.Timeline) {
		return nil
	}
	result := &PercentilesResponse{}
	if include.Summary {
		result.Summary = &PercentileSummary{
			P95LatencyMS: nullableFloat(summary.P95LatencyMS.Valid, summary.P95LatencyMS.Float64),
			P95TTFTMS:    nullableFloat(summary.P95TTFTMS.Valid, summary.P95TTFTMS.Float64),
		}
	}
	if include.Timeline {
		points := make([]PercentileTimelinePoint, 0, len(timeline))
		for _, point := range timeline {
			points = append(points, PercentileTimelinePoint{BucketMS: point.BucketMS,
				P95LatencyMS: nullableFloat(point.P95LatencyMS.Valid, point.P95LatencyMS.Float64),
				P95TTFTMS:    nullableFloat(point.P95TTFTMS.Valid, point.P95TTFTMS.Float64)})
		}
		result.Timeline = &points
	}
	return result
}
