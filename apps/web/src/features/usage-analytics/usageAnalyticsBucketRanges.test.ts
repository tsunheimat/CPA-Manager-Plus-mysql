import { afterEach, describe, expect, it, vi } from 'vitest';
import type {
  MonitoringAnalyticsAnomalyPoint,
  MonitoringAnalyticsTimelinePoint,
} from '@/services/api/usageService';
import {
  adaptUsageAnalyticsData,
  analyzeUsageBucket,
  buildMonitoringDetailUrl,
  buildUsageTimeline,
  canDrilldownUsageBucket,
  fillUsageTimelineBuckets,
  summarizeAnomalies,
} from './usageAnalyticsModel';
import { mergeUsageAnalyticsSections } from './usageAnalyticsSections';

// Same historical repeated local day as the service's Casey regression. The intervening
// 2010-03-05 00:00–02:00 (+11) belongs to a different bucket, not this day's requests.
const bucketMs = Date.parse('2010-03-04T00:00:00+11:00');
const bucketEndMs = Date.parse('2010-03-05T00:00:00+08:00');
const intervals = [
  { from_ms: bucketMs, to_ms: Date.parse('2010-03-05T00:00:00+11:00') },
  { from_ms: Date.parse('2010-03-04T23:00:00+08:00'), to_ms: bucketEndMs },
];
const point: MonitoringAnalyticsTimelinePoint = {
  bucket_ms: bucketMs,
  bucket_end_ms: bucketEndMs,
  bucket_range_exact: false,
  bucket_intervals: intervals,
  label: '03/04',
  calls: 30,
  tokens: 900,
  cost: 3,
  success: 29,
  failure: 1,
};
const anomaly: MonitoringAnalyticsAnomalyPoint = {
  ...point,
  bucket_end_ms: bucketEndMs,
  severity: 'high',
  metric_keys: ['request_spike'],
  total_tokens: 900,
  cost: 3,
  failure_rate: 1 / 30,
  request_change: 2,
  cost_change: 2,
  tokens_per_request_change: 0,
  cache_hit_rate_change: 0,
  failure_rate_change: 0,
  latency_p95_change: 0,
};
const expectedMetadata = {
  bucketMs,
  bucketEndMs,
  bucketRangeExact: false,
  bucketIntervals: intervals.map(({ from_ms, to_ms }) => ({ fromMs: from_ms, toMs: to_ms })),
};

afterEach(() => vi.unstubAllEnvs());

describe('analytics bucket exact-membership contract', () => {
  it('preserves timeline and anomaly metadata through section merging and display adapters', () => {
    vi.stubEnv('TZ', 'Antarctica/Casey');
    const merged = mergeUsageAnalyticsSections(
      { generated_at_ms: 1, granularity: 'day', timeline: [point] },
      null,
      {
        generated_at_ms: 1,
        granularity: 'day',
        percentiles: { timeline: [{ bucket_ms: bucketMs, p95_latency_ms: 50, p95_ttft_ms: null }] },
        anomaly_points: [anomaly],
      },
      null
    );
    const adapted = adaptUsageAnalyticsData(merged, 'day');
    expect(adapted.timeline[0]).toMatchObject({
      ...expectedMetadata,
      requestCount: 30,
      totalTokens: 900,
      estimatedCost: 3,
      p95LatencyMs: 50,
    });
    expect(adapted.anomalyPoints[0]).toMatchObject({
      ...expectedMetadata,
      requestCount: 30,
      totalTokens: 900,
      estimatedCost: 3,
    });
    expect(analyzeUsageBucket(adapted.timeline, bucketMs)?.point).toMatchObject(expectedMetadata);
    expect(summarizeAnomalies(adapted.anomalyPoints)[0]).toMatchObject(expectedMetadata);
    expect(buildMonitoringDetailUrl(adapted.timeline[0], {})).toBe('');
    expect(buildMonitoringDetailUrl(adapted.anomalyPoints[0], {})).toBe('');
  });

  it('never constructs an envelope URL for an explicit non-exact bucket, even without intervals', () => {
    expect(buildMonitoringDetailUrl({ bucketMs, bucketEndMs, bucketRangeExact: false }, {})).toBe(
      ''
    );
  });

  it.each([undefined, true])('keeps legacy and exact ranges navigable (exact=%s)', (exact) => {
    const adapted = buildUsageTimeline(
      [{ ...point, bucket_range_exact: exact, bucket_intervals: undefined }],
      'day'
    )[0];
    expect(adapted.bucketRangeExact).toBe(exact);
    expect(canDrilldownUsageBucket(adapted)).toBe(true);
    const url = new URL(
      buildMonitoringDetailUrl(adapted, { model: 'model-a' }),
      'https://panel.test'
    );
    expect(url.pathname).toBe('/monitoring');
    expect(url.searchParams.get('from_ms')).toBe(String(bucketMs));
    expect(url.searchParams.get('to_ms')).toBe(String(bucketEndMs));
    expect(url.searchParams.get('model')).toBe('model-a');
  });

  it('retains totals and metadata while withholding unverified synthetic gap drilldowns', () => {
    vi.stubEnv('TZ', 'Antarctica/Casey');
    const real = buildUsageTimeline([point], 'day')[0];
    const filled = fillUsageTimelineBuckets(
      [real],
      {
        fromMs: Date.parse('2010-03-03T00:00:00+11:00'),
        toMs: Date.parse('2010-03-06T00:00:00+08:00'),
      },
      'day'
    );
    expect(filled).toHaveLength(3);
    expect(filled.find((entry) => entry.bucketMs === bucketMs)).toBe(real);
    expect(real).toMatchObject(expectedMetadata);
    expect(filled.reduce((total, entry) => total + entry.requestCount, 0)).toBe(30);
    expect(filled.reduce((total, entry) => total + entry.estimatedCost, 0)).toBe(3);
    const synthetic = filled.filter((entry) => entry !== real);
    expect(synthetic).toHaveLength(2);
    for (const entry of synthetic) {
      expect(entry.requestCount).toBe(0);
      expect(entry.bucketIntervals).toBeUndefined(); // Never invent a union from neighboring buckets.
      expect(canDrilldownUsageBucket(entry)).toBe(false);
      expect(buildMonitoringDetailUrl(entry, {})).toBe('');
    }
  });

  it('retains a returned bucket whose canonical ID precedes the selected calendar range', () => {
    vi.stubEnv('TZ', 'Antarctica/Casey');
    const real = buildUsageTimeline([point], 'day')[0];
    const filled = fillUsageTimelineBuckets(
      [real],
      {
        fromMs: Date.parse('2010-03-05T00:30:00+11:00'),
        toMs: Date.parse('2010-03-05T00:00:00+08:00'),
      },
      'day'
    );
    expect(filled).toContain(real);
    expect(filled.reduce((total, entry) => total + entry.requestCount, 0)).toBe(30);
    expect(filled.every((entry) => !canDrilldownUsageBucket(entry))).toBe(true);
  });

  it.each([
    [
      'America/New_York',
      '2026-03-07T00:00:00-05:00',
      '2026-03-08T00:00:00-05:00',
      '2026-03-09T00:00:00-04:00',
    ],
    [
      'America/New_York',
      '2026-10-31T00:00:00-04:00',
      '2026-11-01T00:00:00-04:00',
      '2026-11-02T00:00:00-05:00',
    ],
    [
      'Australia/Lord_Howe',
      '2026-04-04T00:00:00+11:00',
      '2026-04-05T00:00:00+11:00',
      '2026-04-06T00:00:00+10:30',
    ],
  ])('keeps ordinary DST synthetic buckets unchanged in %s (%s)', (zone, before, start, end) => {
    vi.stubEnv('TZ', zone);
    const fromMs = Date.parse(before);
    const startMs = Date.parse(start);
    const endMs = Date.parse(end);
    const real = buildUsageTimeline(
      [
        {
          ...point,
          bucket_ms: fromMs,
          bucket_end_ms: startMs,
          bucket_range_exact: true,
          bucket_intervals: undefined,
        },
      ],
      'day'
    );
    const filled = fillUsageTimelineBuckets(real, { fromMs, toMs: endMs }, 'day');
    expect(filled).toHaveLength(2);
    expect(filled[1]).toMatchObject({ bucketMs: startMs, bucketEndMs: endMs, requestCount: 0 });
    expect(canDrilldownUsageBucket(filled[0])).toBe(true);
    expect(canDrilldownUsageBucket(filled[1])).toBe(true);
    expect(buildMonitoringDetailUrl(filled[1], {})).toContain(`to_ms=${endMs}`);
  });
});
