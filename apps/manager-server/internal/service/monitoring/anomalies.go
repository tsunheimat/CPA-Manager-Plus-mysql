package monitoring

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
)

const anomalyBasisSemantics = "core_additive_exact_latency"

// AnomalyBasis freezes additive changes from this core response. CoreID is an
// opaque response identity, NOT a database snapshot shared with the later exact
// request. Every adjacent pair of existing buckets is retained, even when only
// its later latency values can make it anomalous.
type AnomalyBasis struct {
	Version    int                     `json:"version"`
	Semantics  string                  `json:"semantics"`
	CoreID     string                  `json:"core_id"`
	Candidates []AnomalyBasisCandidate `json:"candidates"`
}

type AnomalyBasisCandidate struct {
	PreviousBucketMS       int64   `json:"previous_bucket_ms"`
	BucketMS               int64   `json:"bucket_ms"`
	RequestChange          float64 `json:"request_change"`
	CostChange             float64 `json:"cost_change"`
	TokensPerRequestChange float64 `json:"tokens_per_request_change"`
	CacheHitRateChange     float64 `json:"cache_hit_rate_change"`
	FailureRateChange      float64 `json:"failure_rate_change"`
}

func prepareAnomalyCandidate(previous, current TimelinePoint) AnomalyBasisCandidate {
	return AnomalyBasisCandidate{
		PreviousBucketMS:       previous.BucketMS,
		BucketMS:               current.BucketMS,
		RequestChange:          percentChange(float64(current.Calls), float64(previous.Calls)),
		CostChange:             percentChange(current.Cost, previous.Cost),
		TokensPerRequestChange: percentChange(averageTokensPerRequest(current), averageTokensPerRequest(previous)),
		CacheHitRateChange:     cacheHitRate(current) - cacheHitRate(previous),
		FailureRateChange:      current.FailureRate - previous.FailureRate,
	}
}

func buildAnomalyBasis(ctx context.Context, timeline []TimelinePoint) (*AnomalyBasis, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	basis := &AnomalyBasis{Version: 1, Semantics: anomalyBasisSemantics, CoreID: hex.EncodeToString(id[:]),
		Candidates: make([]AnomalyBasisCandidate, 0, max(0, len(timeline)-1))}
	for i := 1; i < len(timeline); i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		basis.Candidates = append(basis.Candidates, prepareAnomalyCandidate(timeline[i-1], timeline[i]))
	}
	return basis, nil
}

func finalizeAnomalyCandidate(candidate AnomalyBasisCandidate, current TimelinePoint, latencyChange float64) (AnomalyPoint, bool) {
	keys := make([]string, 0, 6)
	if candidate.RequestChange > 1 {
		keys = append(keys, "request_spike")
	}
	if candidate.CostChange > 1 {
		keys = append(keys, "cost_spike")
	}
	if candidate.TokensPerRequestChange > 0.5 {
		keys = append(keys, "tokens_per_request_spike")
	}
	if candidate.CacheHitRateChange < -0.2 {
		keys = append(keys, "cache_hit_drop")
	}
	if candidate.FailureRateChange > 0.2 {
		keys = append(keys, "failure_rate_spike")
	}
	if latencyChange > 0.5 {
		keys = append(keys, "latency_spike")
	}
	return AnomalyPoint{
		BucketMS: current.BucketMS, BucketEndMS: current.BucketEndMS,
		BucketIntervals: current.BucketIntervals, BucketRangeExact: current.BucketRangeExact,
		Label: current.Label, Severity: anomalySeverity(len(keys)), MetricKeys: keys,
		Calls: current.Calls, TotalTokens: current.TotalTokens, Cost: current.Cost, FailureRate: current.FailureRate,
		RequestChange: candidate.RequestChange, CostChange: candidate.CostChange,
		TokensPerRequestChange: candidate.TokensPerRequestChange, CacheHitRateChange: candidate.CacheHitRateChange,
		FailureRateChange: candidate.FailureRateChange, LatencyP95Change: latencyChange,
	}, len(keys) > 0
}

// Only negotiated v2 cores promise an explicit empty timeline. Legacy responses
// retain their existing omitempty behavior and wire shape.
func (r Response) MarshalJSON() ([]byte, error) {
	type response Response
	if r.AnomalyBasis == nil {
		return json.Marshal(response(r))
	}
	timeline := r.Timeline
	if timeline == nil {
		timeline = []TimelinePoint{}
	}
	return json.Marshal(struct {
		response
		Timeline []TimelinePoint `json:"timeline"`
	}{response: response(r), Timeline: timeline})
}
