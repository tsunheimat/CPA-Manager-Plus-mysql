import fixtureData from '../../../../manager-server/internal/service/monitoring/testdata/anomaly_basis_v1.json';
import { describe, expect, it } from 'vitest';
import type {
  MonitoringAnalyticsAnomalyBasisCandidate,
  MonitoringAnalyticsAnomalyPoint,
  MonitoringAnalyticsResponse,
  MonitoringAnalyticsTimelinePoint,
} from '@/services/api/usageService';
import {
  completeUsageAnomalies,
  hasAnomalyBasis,
  hasRequestedPercentiles,
} from './usageAnalyticsAnomalies';
import { buildUsageAnalyticsSections, mergeUsageAnalyticsSections } from './usageAnalyticsSections';

const fixture = fixtureData as {
  cases: Array<{
    name: string;
    timeline: MonitoringAnalyticsTimelinePoint[];
    candidates: MonitoringAnalyticsAnomalyBasisCandidate[];
    exact: NonNullable<MonitoringAnalyticsResponse['percentiles']>['timeline'];
    expected: MonitoringAnalyticsAnomalyPoint[];
  }>;
  ranking: { bucket_count: number; alternating_latency_ms: number[]; expected_bucket_ms: number[] };
};
const response = { generated_at_ms: 1, granularity: 'hour' };
const basis = {
  version: 1,
  semantics: 'core_additive_exact_latency',
  core_id: 'core-one',
} as const;

describe('anomaly v2 shared independent oracle', () => {
  it.each(fixture.cases)('$name', (test) => {
    const core = {
      ...response,
      timeline: test.timeline,
      anomaly_basis: { ...basis, candidates: test.candidates },
    };
    const exact = { ...response, percentiles: { timeline: test.exact } };
    expect(hasAnomalyBasis(core)).toBe(true);
    expect(completeUsageAnomalies(core, exact)).toEqual(test.expected);
  });

  it('retains all latency-only candidates then stably orders ties and caps at 50', () => {
    const timeline = Array.from({ length: fixture.ranking.bucket_count }, (_, i) => ({
      bucket_ms: i,
      bucket_end_ms: i + 1,
      bucket_range_exact: true,
      label: '',
      calls: 1,
      success: 1,
      failure: 0,
      tokens: 0,
      total_tokens: 0,
      cost: 0,
      failure_rate: 0,
    }));
    const candidates = timeline.slice(1).map((point) => ({
      previous_bucket_ms: point.bucket_ms - 1,
      bucket_ms: point.bucket_ms,
      request_change: 0,
      cost_change: 0,
      tokens_per_request_change: 0,
      cache_hit_rate_change: 0,
      failure_rate_change: 0,
    }));
    const exact = {
      ...response,
      percentiles: {
        timeline: timeline.map((point) => ({
          bucket_ms: point.bucket_ms,
          p95_latency_ms: fixture.ranking.alternating_latency_ms[point.bucket_ms % 2],
          p95_ttft_ms: null,
        })),
      },
    };
    expect(
      completeUsageAnomalies(
        { ...response, timeline, anomaly_basis: { ...basis, candidates } },
        exact
      )?.map((point) => point.bucket_ms)
    ).toEqual(fixture.ranking.expected_bucket_ms);
  });

  it('preserves strict score-addition order including below-threshold positive changes', () => {
    const timeline = [0, 1, 2, 3].map((id) => ({
      bucket_ms: id,
      bucket_end_ms: id + 1,
      bucket_range_exact: true,
      label: '',
      calls: 1,
      tokens: 0,
      success: 1,
      failure: 0,
      total_tokens: 0,
      cost: 0,
      failure_rate: 0,
    }));
    const candidates = timeline.slice(1).map((point) => ({
      previous_bucket_ms: point.bucket_ms - 1,
      bucket_ms: point.bucket_ms,
      request_change: 0,
      cost_change: point.bucket_ms === 1 ? 1 : 0,
      tokens_per_request_change: 1,
      cache_hit_rate_change: 0,
      failure_rate_change: 0,
    }));
    const result = completeUsageAnomalies(
      { ...response, timeline, anomaly_basis: { ...basis, candidates } },
      { ...response, percentiles: { timeline: [] } }
    );
    expect(result?.map((point) => point.bucket_ms)).toEqual([1, 3, 2]);
  });

  it('rejects missing acknowledgments, truncated candidates, missing nullable fields and duplicate buckets', () => {
    const test = fixture.cases[2];
    const core: MonitoringAnalyticsResponse = {
      ...response,
      timeline: test.timeline,
      anomaly_basis: { ...basis, candidates: test.candidates },
    };
    const exact: MonitoringAnalyticsResponse = {
      ...response,
      percentiles: { timeline: test.exact },
    };
    for (const patch of [
      undefined,
      { ...core.anomaly_basis!, core_id: '' },
      { ...core.anomaly_basis!, candidates: [] },
      { ...core.anomaly_basis!, semantics: 'unknown' },
    ]) {
      expect(
        completeUsageAnomalies(
          { ...core, anomaly_basis: patch } as MonitoringAnalyticsResponse,
          exact
        )
      ).toBeUndefined();
    }
    for (const percentiles of [
      undefined,
      {},
      { timeline: null },
      { timeline: [{ bucket_ms: 100, p95_latency_ms: null }] },
      { timeline: [{ bucket_ms: 100, p95_latency_ms: NaN, p95_ttft_ms: null }] },
      { timeline: [test.exact![0], test.exact![0]] },
    ]) {
      expect(
        completeUsageAnomalies(core, { ...response, percentiles } as MonitoringAnalyticsResponse)
      ).toBeUndefined();
    }
    expect(hasRequestedPercentiles(exact, { summary: true, timeline: true })).toBe(false);
    expect(
      mergeUsageAnalyticsSections(core, null, exact, null, 2, { summary: true, timeline: true })
        ?.anomaly_points
    ).toBeUndefined();
  });

  it('uses frozen additive values under later insert/delete/price changes and does not merge coverage', () => {
    const test = fixture.cases[2];
    const coreCoverage = {
      raw_complete: false,
      core_aggregate_used: true,
    } as MonitoringAnalyticsResponse['coverage'];
    const exactCoverage = {
      raw_complete: false,
      core_aggregate_used: false,
    } as MonitoringAnalyticsResponse['coverage'];
    const core = {
      ...response,
      timeline: test.timeline,
      coverage: coreCoverage,
      anomaly_basis: { ...basis, candidates: test.candidates },
    };
    const exact = {
      ...response,
      coverage: exactCoverage,
      percentiles: { timeline: test.exact },
      // An unrelated new snapshot must not replace core totals or candidates.
      timeline: test.timeline.map((point) => ({ ...point, calls: 999, cost: 999 })),
    };
    const merged = mergeUsageAnalyticsSections(core, null, exact, null, 2, { timeline: true });
    expect(merged?.anomaly_points).toEqual(test.expected);
    expect(merged?.coverage).toEqual(coreCoverage);
    expect(completeUsageAnomalies(core, { ...exact, percentiles: { timeline: [] } })).toEqual([]);
  });

  it.each([1, 2])('negotiates only overview/trends at capability %s', (version) => {
    for (const tab of [
      'overview',
      'trends',
      'models',
      'apiKeys',
      'credentials',
      'heatmap',
    ] as const) {
      const { core, exact } = buildUsageAnalyticsSections(tab, 'hour', version);
      if (tab === 'overview' || tab === 'trends') {
        expect(core.anomaly_basis_version).toBe(version === 2 ? 1 : undefined);
        expect(exact.anomaly_points).toBe(version === 1 ? true : undefined);
        if (version === 2)
          expect(Object.keys(exact).sort()).toEqual(['granularity', 'percentiles']);
      } else {
        expect(core.anomaly_basis_version).toBeUndefined();
        expect(exact.percentiles).toBeUndefined();
      }
    }
  });
});
