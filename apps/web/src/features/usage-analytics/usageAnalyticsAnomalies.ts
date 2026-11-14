import type {
  MonitoringAnalyticsAnomalyPoint,
  MonitoringAnalyticsInclude,
  MonitoringAnalyticsResponse,
} from '@/services/api/usageService';

const finite = (value: unknown): value is number =>
  typeof value === 'number' && Number.isFinite(value);
const nullableFinite = (value: unknown) => value === null || finite(value);

/** A missing acknowledgment is not an acknowledged empty population. */
export function hasAnomalyBasis(core: MonitoringAnalyticsResponse | null): boolean {
  const basis = core?.anomaly_basis;
  const timeline = core?.timeline;
  if (
    !basis ||
    basis.version !== 1 ||
    basis.semantics !== 'core_additive_exact_latency' ||
    typeof basis.core_id !== 'string' ||
    !basis.core_id.trim() ||
    !Array.isArray(basis.candidates) ||
    !Array.isArray(timeline) ||
    basis.candidates.length !== Math.max(0, timeline.length - 1)
  )
    return false;
  const ids = new Set<number>();
  for (const point of timeline) {
    if (
      !point ||
      !finite(point.bucket_ms) ||
      ids.has(point.bucket_ms) ||
      !finite(point.bucket_end_ms) ||
      typeof point.bucket_range_exact !== 'boolean' ||
      typeof point.label !== 'string' ||
      !finite(point.calls) ||
      !finite(point.total_tokens) ||
      !finite(point.cost) ||
      !finite(point.failure_rate)
    )
      return false;
    if (
      point.bucket_intervals !== undefined &&
      (!Array.isArray(point.bucket_intervals) ||
        !point.bucket_intervals.every(
          (interval) =>
            interval &&
            finite(interval.from_ms) &&
            finite(interval.to_ms) &&
            interval.from_ms < interval.to_ms
        ))
    )
      return false;
    ids.add(point.bucket_ms);
  }
  return basis.candidates.every(
    (candidate, index) =>
      candidate &&
      candidate.previous_bucket_ms === timeline[index].bucket_ms &&
      candidate.bucket_ms === timeline[index + 1].bucket_ms &&
      [
        candidate.request_change,
        candidate.cost_change,
        candidate.tokens_per_request_change,
        candidate.cache_hit_rate_change,
        candidate.failure_rate_change,
      ].every(finite)
  );
}

export function hasRequestedPercentiles(
  exact: MonitoringAnalyticsResponse | null,
  requested: MonitoringAnalyticsInclude['percentiles']
): boolean {
  if (!requested) return true;
  const values = (point: unknown) => {
    if (!point || typeof point !== 'object') return false;
    const value = point as { p95_latency_ms?: unknown; p95_ttft_ms?: unknown };
    return nullableFinite(value.p95_latency_ms) && nullableFinite(value.p95_ttft_ms);
  };
  if (requested.summary && !values(exact?.percentiles?.summary)) return false;
  if (requested.timeline) {
    const timeline = exact?.percentiles?.timeline;
    if (!Array.isArray(timeline)) return false;
    const ids = new Set<number>();
    for (const point of timeline) {
      if (!values(point) || !finite(point.bucket_ms) || ids.has(point.bucket_ms)) return false;
      ids.add(point.bucket_ms);
    }
  }
  return true;
}

function percentChange(current: number, previous: number) {
  return previous <= 0 ? (current > 0 ? 1 : 0) : (current - previous) / previous;
}

function score(point: MonitoringAnalyticsAnomalyPoint) {
  // Keep Go's addition order; thresholds and score inclusion are different.
  let value = point.metric_keys.length * 10;
  value += Math.max(0, point.request_change);
  value += Math.max(0, point.cost_change);
  value += Math.max(0, point.tokens_per_request_change);
  value += Math.max(0, -point.cache_hit_rate_change);
  value += Math.max(0, point.failure_rate_change);
  value += Math.max(0, point.latency_p95_change);
  return value;
}

/** Call only with generation + core_id fenced responses. Additive values remain
 * frozen at core time, while exact latency may reflect later writes/archives.
 * Undefined means incomplete protocol, not zero anomalies. */
export function completeUsageAnomalies(
  core: MonitoringAnalyticsResponse | null,
  exact: MonitoringAnalyticsResponse | null
): MonitoringAnalyticsAnomalyPoint[] | undefined {
  if (!hasAnomalyBasis(core) || !hasRequestedPercentiles(exact, { timeline: true }))
    return undefined;
  const timeline = core!.timeline!;
  const latency = new Map(
    exact!.percentiles!.timeline!.map((point) => [point.bucket_ms, point.p95_latency_ms ?? 0])
  );
  const result: MonitoringAnalyticsAnomalyPoint[] = [];
  core!.anomaly_basis!.candidates.forEach((candidate, index) => {
    const current = timeline[index + 1];
    const latencyChange = percentChange(
      latency.get(candidate.bucket_ms) ?? 0,
      latency.get(candidate.previous_bucket_ms) ?? 0
    );
    const keys: string[] = [];
    if (candidate.request_change > 1) keys.push('request_spike');
    if (candidate.cost_change > 1) keys.push('cost_spike');
    if (candidate.tokens_per_request_change > 0.5) keys.push('tokens_per_request_spike');
    if (candidate.cache_hit_rate_change < -0.2) keys.push('cache_hit_drop');
    if (candidate.failure_rate_change > 0.2) keys.push('failure_rate_spike');
    if (latencyChange > 0.5) keys.push('latency_spike');
    if (!keys.length) return;
    result.push({
      bucket_ms: current.bucket_ms,
      bucket_end_ms: current.bucket_end_ms!,
      bucket_range_exact: current.bucket_range_exact,
      ...(current.bucket_intervals ? { bucket_intervals: current.bucket_intervals } : {}),
      label: current.label,
      calls: current.calls,
      total_tokens: current.total_tokens!,
      cost: current.cost!,
      failure_rate: current.failure_rate!,
      request_change: candidate.request_change,
      cost_change: candidate.cost_change,
      tokens_per_request_change: candidate.tokens_per_request_change,
      cache_hit_rate_change: candidate.cache_hit_rate_change,
      failure_rate_change: candidate.failure_rate_change,
      latency_p95_change: latencyChange,
      severity: keys.length >= 3 ? 'high' : keys.length >= 2 ? 'medium' : 'low',
      metric_keys: keys,
    });
  });
  return result.sort((a, b) => score(b) - score(a) || b.bucket_ms - a.bucket_ms).slice(0, 50);
}
