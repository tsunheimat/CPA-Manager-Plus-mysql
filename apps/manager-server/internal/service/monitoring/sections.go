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
	priceBook                 bool
	priceSnapshot             bool
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
	// Match execution precedence: selectors replace legacy priced filter options.
	// Legacy anomalies build a priced timeline even without include.timeline.
	plan.priceBook = include.Summary || include.Timeline || include.ModelShare || include.ModelStats ||
		include.ChannelShare || include.AccountStats || include.CredentialStats || include.CredentialTimeline ||
		include.APIKeyStats || include.APIKeyTimeline || include.Heatmap || include.AnomalyPoints ||
		(include.FilterOptions && !include.FilterSelectors)
	// Full event counts can use price-structure-dependent derived aggregates;
	// bounded previews cannot. Hold this gate for the entire dependent request.
	plan.priceSnapshot = plan.priceBook || include.EventsPage != nil
	plan.latencySummary = plan.legacySummaryPercentiles || (include.Percentiles != nil && include.Percentiles.Summary)
	plan.latencyTimeline = plan.legacyTimelinePercentiles || include.AnomalyPoints || (include.Percentiles != nil && include.Percentiles.Timeline)
	return plan
}

func ValidateInclude(include Include) error {
	if include.TimelineProfile != "" && include.TimelineProfile != "core" && include.TimelineProfile != "full" {
		return errors.New("include.timeline_profile must be core or full")
	}
	if include.AnomalyBasisVersion != nil {
		if *include.AnomalyBasisVersion != 1 {
			return errors.New("include.anomaly_basis_version must be 1")
		}
		if !include.Timeline || include.TimelineProfile != "core" || include.AnomalyPoints {
			return errors.New("include.anomaly_basis_version requires timeline with core profile and without anomaly_points")
		}
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
