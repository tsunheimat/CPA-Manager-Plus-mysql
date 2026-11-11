import { describe, expect, it, vi } from 'vitest';
import {
  createMonitoringAnalyticsScheduler,
  type UseMonitoringAnalyticsReturn,
} from '@/features/monitoring/hooks/useMonitoringAnalytics';
import type {
  MonitoringAnalyticsEventRow,
  MonitoringAnalyticsResponse,
  MonitoringAnalyticsSummary,
} from '@/services/api/usageService';
import {
  analyticsSectionState,
  buildUsageAnalyticsSections,
  currentAnalyticsData,
  mergeUsageAnalyticsSections,
} from './usageAnalyticsSections';
import {
  buildDrilldownPreview,
  buildUsageTimeline,
  buildUsageSummary,
  fillUsageTimelineBuckets,
} from './usageAnalyticsModel';

const response = { generated_at_ms: 1, granularity: 'hour' };
describe('analytics section semantics', () => {
  it('never includes exact metrics, anomalies, or dimensions in core for any tab', () => {
    for (const tab of [
      'overview',
      'trends',
      'models',
      'apiKeys',
      'credentials',
      'heatmap',
    ] as const) {
      const { core, dimensions } = buildUsageAnalyticsSections(tab, 'hour');
      expect(core.summary_profile).toBe('compact');
      expect(core).not.toHaveProperty('summary_percentiles');
      expect(core).not.toHaveProperty('percentiles');
      expect(core).not.toHaveProperty('anomaly_points');
      expect(core).not.toHaveProperty('api_key_stats');
      expect(core).not.toHaveProperty('channel_share');
      expect(core).not.toHaveProperty('credential_stats');
      expect(dimensions).not.toHaveProperty('summary');
      if (core.timeline) expect(core.timeline_profile).toBe('core');
    }
  });
  it('distinguishes empty success, missing explicit metrics, failure, and stale data', () => {
    const analytics: UseMonitoringAnalyticsReturn = {
      enabled: true,
      loading: false,
      error: '',
      data: response,
      dataStale: false,
      lastRefreshedAt: null,
      serviceBase: 'test',
      unavailableReason: '',
      refresh: vi.fn(),
    };
    expect(analyticsSectionState(analytics, true)).toMatchObject({
      loaded: true,
      loading: false,
      error: '',
    });
    expect(analyticsSectionState(analytics, true, false)).toMatchObject({
      loaded: false,
      loading: false,
      error: 'usage_analytics.section_missing',
    });
    expect(
      analyticsSectionState({ ...analytics, data: null, error: 'failed' }, true)
    ).toMatchObject({ loaded: false, loading: false, error: 'failed' });
    expect(currentAnalyticsData({ ...analytics, dataStale: true })).toBeNull();
    expect(
      analyticsSectionState(
        { ...analytics, enabled: false, data: null, unavailableReason: 'service_not_configured' },
        true
      )
    ).toMatchObject({ loaded: false, loading: false });
  });
  it('only overlays exact values on core buckets, preserving core coverage and bucket ends', () => {
    const core: MonitoringAnalyticsResponse = {
      ...response,
      summary: { total_calls: 4 } as MonitoringAnalyticsSummary,
      timeline: [
        {
          bucket_ms: 100,
          bucket_end_ms: 333,
          label: '',
          calls: 4,
          tokens: 4,
          success: 4,
          failure: 0,
        },
      ],
    };
    const exact: MonitoringAnalyticsResponse = {
      ...response,
      percentiles: {
        summary: { p95_latency_ms: null, p95_ttft_ms: 9 },
        timeline: [
          { bucket_ms: 100, p95_latency_ms: null, p95_ttft_ms: 2 },
          { bucket_ms: 200, p95_latency_ms: 999, p95_ttft_ms: 999 },
        ],
      },
      coverage: { scope: 'exact', raw_complete: false } as MonitoringAnalyticsResponse['coverage'],
    };
    const merged = mergeUsageAnalyticsSections(core, null, exact, null)!;
    expect(merged.timeline).toHaveLength(1);
    expect(merged.timeline![0]).toMatchObject({
      calls: 4,
      bucket_end_ms: 333,
      p95_latency_ms: null,
      p95_ttft_ms: 2,
    });
    expect(merged.summary).toEqual({ total_calls: 4, p95_latency_ms: null, p95_ttft_ms: 9 });
    expect(merged.coverage).toBeUndefined();
    expect(mergeUsageAnalyticsSections(null, null, exact, null)).toBeNull();
  });
  it('does not turn invalid or absent exact samples or missing pricing context into zero', () => {
    const metrics = buildUsageSummary({
      p95_latency_ms: NaN,
      p95_ttft_ms: Infinity,
      average_latency_ms: null,
    } as MonitoringAnalyticsSummary);
    expect(metrics).toMatchObject({ p95LatencyMs: null, p95TtftMs: null, averageLatencyMs: null });
    const event = {
      event_hash: 'event',
      model: 'unknown',
      total_tokens: 100,
    } as MonitoringAnalyticsEventRow;
    expect(buildDrilldownPreview([event], [])[0].estimatedCost).toBeNull();
  });
  it.each([
    ['2026-03-07T05:00:00Z', '2026-03-10T04:00:00Z', 23],
    ['2026-10-31T04:00:00Z', '2026-11-03T05:00:00Z', 25],
  ])('fills calendar day ends across DST from %s', (from, to, hours) => {
    vi.stubEnv('TZ', 'America/New_York');
    try {
      const fromMs = Date.parse(from);
      const timeline = buildUsageTimeline(
        [
          {
            bucket_ms: fromMs,
            bucket_end_ms: fromMs + 500,
            label: '',
            calls: 1,
            tokens: 1,
            success: 1,
            failure: 0,
          },
        ],
        'day'
      );
      const filled = fillUsageTimelineBuckets(timeline, { fromMs, toMs: Date.parse(to) }, 'day');
      expect(filled[0].bucketEndMs).toBe(fromMs + 500);
      expect(filled[1].bucketEndMs - filled[1].bucketMs).toBe(Number(hours) * 3_600_000);
      expect(filled[1].bucketEndMs).toBe(filled[2].bucketMs);
    } finally {
      vi.unstubAllEnvs();
    }
  });
});

describe('page-local analytics admission', () => {
  it('prioritizes queued work, removes canceled work and holds running slots until settled', async () => {
    const scheduler = createMonitoringAnalyticsScheduler(1);
    const started: string[] = [];
    const firstController = new AbortController();
    const canceledController = new AbortController();
    let finish!: () => void;
    const first = scheduler.run(() => {
      started.push('first');
      return new Promise<void>((resolve) => {
        finish = resolve;
      });
    }, firstController.signal);
    await Promise.resolve();
    await Promise.resolve();
    const canceled = scheduler.run(async () => {
      started.push('canceled');
    }, canceledController.signal);
    const canceledResult = canceled.catch((error) => error);
    const low = scheduler.run(
      async () => {
        started.push('low');
      },
      new AbortController().signal,
      5
    );
    const high = scheduler.run(
      async () => {
        started.push('high');
      },
      new AbortController().signal,
      0
    );
    firstController.abort();
    canceledController.abort();
    expect((await canceledResult).name).toBe('AbortError');
    expect(started).toEqual(['first']);
    finish();
    await Promise.all([first, high, low]);
    expect(started).toEqual(['first', 'high', 'low']);
  });
  it('releases a slot after a synchronous work error', async () => {
    const scheduler = createMonitoringAnalyticsScheduler(1);
    await expect(
      scheduler.run(() => {
        throw new Error('failure');
      }, new AbortController().signal)
    ).rejects.toThrow('failure');
    await expect(scheduler.run(async () => 42, new AbortController().signal)).resolves.toBe(42);
  });
});
