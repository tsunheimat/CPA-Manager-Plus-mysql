import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  adaptUsageAnalyticsData,
  buildSelectedCredentialTrendSeries,
} from '@/features/usage-analytics/usageAnalyticsModel';
import { getDemoMonitoringAnalytics, getDemoUsageServiceInfo } from './demoFixtures';

describe('demo analytics section contract', () => {
  const now = Date.UTC(2026, 10, 6, 12);
  const scope = { from_ms: now - 86_400_000, to_ms: now, now_ms: now };
  beforeEach(() => {
    vi.stubEnv('TZ', 'UTC');
  });
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it.each([
    { days: 1, granularity: 'hour' as const, buckets: 24 },
    { days: 7, granularity: 'hour' as const, buckets: 168 },
    { days: 14, granularity: 'day' as const, buckets: 14 },
  ])(
    'covers the full $days-day credential trend at $granularity granularity',
    ({ days, granularity, buckets }) => {
      const to = Date.UTC(2026, 10, 6);
      const from = to - days * 86_400_000;
      const request = {
        from_ms: from,
        to_ms: to,
        now_ms: to,
        include: { timeline: true, credential_stats: true, credential_timeline: true, granularity },
      };
      const response = getDemoMonitoringAnalytics(request);
      const expectedStarts = Array.from(
        { length: buckets },
        (_, index) => from + index * (granularity === 'hour' ? 3_600_000 : 86_400_000)
      );
      expect(response.timeline?.map((point) => point.bucket_ms)).toEqual(expectedStarts);
      const credentialIDs = [...new Set(response.credential_timeline?.map((point) => point.id))];
      expect(credentialIDs.length).toBeGreaterThan(0);
      for (const id of credentialIDs) {
        expect(
          response.credential_timeline
            ?.filter((point) => point.id === id)
            .map((point) => point.bucket_ms)
        ).toEqual(expectedStarts);
      }

      const selectedID = credentialIDs[0];
      const scopedResponse = getDemoMonitoringAnalytics({
        ...request,
        filters: { credential_ids: [selectedID] },
      });
      const adapted = adaptUsageAnalyticsData(scopedResponse, granularity);
      const trend = buildSelectedCredentialTrendSeries(
        adapted.credentialRows[0],
        adapted.credentialTimeline,
        'requestCount'
      );
      expect(trend).toHaveLength(1);
      expect(trend[0].points.map((point) => point.bucketMs)).toEqual(expectedStarts);
      expect(trend[0].points.every((point) => point.value > 0)).toBe(true);
    }
  );

  it('resumes calendar hour starts and ends after the Lord Howe fractional spring transition', () => {
    vi.stubEnv('TZ', 'Australia/Lord_Howe');
    const expectedBoundaries = [
      '2026-10-04T00:00:00+10:30',
      '2026-10-04T01:00:00+10:30',
      '2026-10-04T02:30:00+11:00',
      '2026-10-04T03:00:00+11:00',
      '2026-10-04T04:00:00+11:00',
      '2026-10-04T05:00:00+11:00',
    ].map((value) => Date.parse(value));
    const from = expectedBoundaries[0];
    const to = expectedBoundaries[expectedBoundaries.length - 1];
    const request = {
      from_ms: from,
      to_ms: to,
      now_ms: to,
      time_zone: 'Australia/Lord_Howe',
      include: { timeline: true, anomaly_points: true, granularity: 'hour' as const },
    };
    const response = getDemoMonitoringAnalytics(request);
    expect(response.timeline?.map((point) => point.bucket_ms)).toEqual(
      expectedBoundaries.slice(0, -1)
    );
    expect(response.timeline?.map((point) => point.bucket_end_ms)).toEqual(
      expectedBoundaries.slice(1)
    );
    const adapted = adaptUsageAnalyticsData(response, 'hour', '', undefined, undefined, {
      fromMs: from,
      toMs: to,
    });
    expect(adapted.timeline.map((point) => [point.bucketMs, point.bucketEndMs])).toEqual(
      expectedBoundaries.slice(0, -1).map((start, index) => [start, expectedBoundaries[index + 1]])
    );
    expect(adapted.timeline.every((point) => point.requestCount > 0)).toBe(true);
    for (const anomaly of response.anomaly_points ?? []) {
      expect(anomaly.bucket_end_ms).toBe(
        expectedBoundaries[expectedBoundaries.indexOf(anomaly.bucket_ms) + 1]
      );
    }
  });

  it('keeps every partial-range overview hour populated rather than filling daily fixture gaps', () => {
    const to = Date.UTC(2026, 10, 6, 18, 27);
    const from = to - 86_400_000;
    const response = getDemoMonitoringAnalytics({
      from_ms: from,
      to_ms: to,
      now_ms: to,
      include: { summary: true, timeline: true, timeline_profile: 'core', granularity: 'hour' },
    });
    const adapted = adaptUsageAnalyticsData(response, 'hour', '', undefined, undefined, {
      fromMs: from,
      toMs: to,
    });
    expect(response.timeline).toHaveLength(25);
    expect(adapted.timeline).toHaveLength(25);
    expect(adapted.timeline.every((point) => point.requestCount > 0)).toBe(true);
  });

  it('advertises sections, returns a cheap core profile and standalone exact metrics', () => {
    expect(getDemoUsageServiceInfo().analyticsSectionsVersion).toBe(1);
    const core = getDemoMonitoringAnalytics({
      ...scope,
      include: {
        summary: true,
        summary_profile: 'compact',
        timeline: true,
        timeline_profile: 'core',
        model_stats: true,
        granularity: 'hour',
      },
    });
    expect(core.granularity).toBe('hour');
    expect(core.timeline).toHaveLength(24);
    expect(core.summary).not.toHaveProperty('p95_latency_ms');
    expect(core.timeline![0]).not.toHaveProperty('p95_latency_ms');
    expect(core).not.toHaveProperty('api_key_stats');
    expect(core).not.toHaveProperty('anomaly_points');
    expect(core).not.toHaveProperty('events');
    const exact = getDemoMonitoringAnalytics({
      ...scope,
      include: {
        granularity: 'hour',
        percentiles: { summary: true, timeline: true },
        anomaly_points: true,
      },
    });
    expect(exact).not.toHaveProperty('summary');
    expect(exact).not.toHaveProperty('timeline');
    expect(exact.percentiles?.summary?.p95_latency_ms).toBeGreaterThan(0);
    expect(exact.percentiles?.timeline?.map((point) => point.bucket_ms)).toEqual(
      core.timeline?.map((point) => point.bucket_ms)
    );
    expect(exact.anomaly_points?.length).toBeGreaterThan(0);
  });
  it('keeps legacy full percentiles and isolates preview bounds', () => {
    const legacy = getDemoMonitoringAnalytics({
      ...scope,
      include: { summary: true, timeline: true },
    });
    expect(legacy.summary?.p95_latency_ms).toBeGreaterThan(0);
    expect(legacy.timeline?.[0].p95_latency_ms).toBeGreaterThan(0);
    const preview = getDemoMonitoringAnalytics({
      ...scope,
      include: {
        drilldown_preview: {
          from_ms: now - 60_000,
          to_ms: now,
          limit: 12,
        },
      },
    });
    expect(preview).not.toHaveProperty('summary');
    expect(preview).not.toHaveProperty('timeline');
    expect(
      preview.drilldown_preview?.items.every(
        (item) => item.timestamp_ms >= now - 60_000 && item.timestamp_ms < now
      )
    ).toBe(true);
    const tiny = getDemoMonitoringAnalytics({
      from_ms: now - 10_000,
      to_ms: now,
      now_ms: now,
      include: { granularity: 'hour', timeline: true, anomaly_points: true },
    });
    expect(tiny.timeline).toHaveLength(1);
  });
});
