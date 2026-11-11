import { useEffect } from 'react';
import { MemoryRouter } from 'react-router-dom';
import { act, create, type ReactTestRenderer } from 'react-test-renderer';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type {
  MonitoringAnalyticsRequest,
  MonitoringAnalyticsResponse,
  MonitoringAnalyticsSummary,
} from '@/services/api/usageService';
import { useUsageAnalytics } from './useUsageAnalytics';

const mocks = vi.hoisted(() => ({
  getAnalytics: vi.fn(),
  version: 1,
  checking: false,
  meta: vi.fn(),
  aliases: vi.fn(),
  auth: { apiBase: 'https://panel.test', managementKey: 'test' },
}));
vi.mock('@/services/api/usageService', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/services/api/usageService')>()),
  monitoringAnalyticsApi: { getAnalytics: mocks.getAnalytics },
}));
vi.mock('@/hooks/usePanelFeatureAvailability', () => ({
  usePanelFeatureAvailability: () => ({
    analyticsSectionsVersion: mocks.version,
    checking: mocks.checking,
  }),
}));
vi.mock('@/hooks/useRequestMonitoringAvailability', () => ({
  useRequestMonitoringAvailability: () => ({
    available: true,
    checking: false,
    serviceBase: 'https://manager.test',
    reason: '',
  }),
}));
vi.mock('@/features/monitoring/hooks/useUsageData', () => ({
  useUsageData: () => ({ apiKeyAliases: [], loadApiKeyAliases: mocks.aliases }),
}));
vi.mock('@/features/monitoring/services/monitoringMetaService', () => ({
  loadMonitoringMetaPayload: mocks.meta,
}));
vi.mock('@/stores', () => ({
  useConfigStore: (select: (state: { config: null }) => unknown) => select({ config: null }),
  useAuthStore: (select: (state: typeof mocks.auth) => unknown) => select(mocks.auth),
}));
(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const summary: MonitoringAnalyticsSummary = {
  total_calls: 10,
  success_calls: 9,
  failure_calls: 1,
  success_rate: 0.9,
  input_tokens: 70,
  output_tokens: 30,
  cached_tokens: 0,
  cache_read_tokens: 0,
  cache_creation_tokens: 0,
  reasoning_tokens: 0,
  total_tokens: 100,
  total_cost: 2,
  average_latency_ms: 30,
  zero_token_calls: 0,
  rpm_30m: 0,
  tpm_30m: 0,
  avg_daily_requests: 10,
  avg_daily_tokens: 100,
  approx_tasks: 0,
  approx_task_failures: 0,
  approx_task_success_rate: 0,
  zero_token_models: [],
};
const coreResponse = (request: MonitoringAnalyticsRequest): MonitoringAnalyticsResponse => ({
  generated_at_ms: request.now_ms!,
  granularity: 'hour',
  summary,
  model_stats: [
    {
      model: 'model-a',
      calls: 10,
      success_calls: 9,
      failure_calls: 1,
      success_rate: 0.9,
      input_tokens: 70,
      output_tokens: 30,
      cached_tokens: 0,
      cache_read_tokens: 0,
      cache_creation_tokens: 0,
      total_tokens: 100,
      cost: 2,
    },
  ],
  timeline: [
    {
      bucket_ms: request.from_ms,
      bucket_end_ms: request.from_ms + 1_800_000,
      label: 'test',
      calls: 10,
      tokens: 100,
      success: 9,
      failure: 1,
    },
  ],
});
type Pending = {
  request: MonitoringAnalyticsRequest;
  signal: AbortSignal;
  settled: boolean;
  resolve: (data: MonitoringAnalyticsResponse) => void;
  reject: (error: Error) => void;
};
const isCore = (p: Pending) => Boolean(p.request.include?.summary);
const isDimensions = (p: Pending) =>
  Boolean(p.request.include?.api_key_stats && !p.request.include.summary);
const isExact = (p: Pending) => Boolean(p.request.include?.percentiles);
const isSelectors = (p: Pending) => Boolean(p.request.include?.filter_selectors);
const isPreview = (p: Pending) => Boolean(p.request.include?.drilldown_preview);
const emptyResponse = { generated_at_ms: 1, granularity: 'hour' };

describe('progressive usage analytics with real request hooks', () => {
  let renderer: ReactTestRenderer;
  let latest: ReturnType<typeof useUsageAnalytics>;
  let pending: Pending[];
  let active: number;
  let peakActive: number;
  function Harness() {
    const result = useUsageAnalytics();
    useEffect(() => {
      latest = result;
    }, [result]);
    return null;
  }
  const render = async () => {
    await act(async () => {
      renderer = create(
        <MemoryRouter>
          <Harness />
        </MemoryRouter>
      );
    });
  };
  const next = (predicate: (p: Pending) => boolean) => {
    const value = pending.find((p) => !p.settled && !p.signal.aborted && predicate(p));
    expect(value).toBeDefined();
    return value!;
  };
  const resolve = async (
    predicate: (p: Pending) => boolean,
    data?: MonitoringAnalyticsResponse
  ) => {
    const p = next(predicate);
    await act(async () => {
      p.resolve(data ?? (isCore(p) ? coreResponse(p.request) : emptyResponse));
    });
  };
  const finishCoreAndDimensions = async () => {
    await resolve(isCore);
    await resolve(isDimensions);
  };
  const finishOptional = async () => {
    await resolve(isSelectors, { ...emptyResponse, filter_options: { models: ['model-a'] } });
    const core = pending.find(isCore)!;
    await resolve(isExact, {
      ...emptyResponse,
      percentiles: {
        summary: { p95_latency_ms: 100, p95_ttft_ms: null },
        timeline: [{ bucket_ms: core.request.from_ms, p95_latency_ms: 100, p95_ttft_ms: null }],
      },
      anomaly_points: [],
    });
  };
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date('2026-11-06T12:00:00Z'));
    mocks.version = 1;
    mocks.checking = false;
    mocks.auth.managementKey = 'test';
    mocks.meta.mockReset().mockResolvedValue({
      authFiles: [],
      authFilesLoaded: true,
      channels: [],
      channelsLoaded: true,
      error: '',
    });
    pending = [];
    active = 0;
    peakActive = 0;
    mocks.getAnalytics.mockReset().mockImplementation(
      (_base, _key, request, signal) =>
        new Promise<MonitoringAnalyticsResponse>((res, rej) => {
          active += 1;
          peakActive = Math.max(peakActive, active);
          const entry: Pending = {
            request,
            signal,
            settled: false,
            resolve: (data) => {
              if (!entry.settled) {
                entry.settled = true;
                active -= 1;
                res(data);
              }
            },
            reject: (error) => {
              if (!entry.settled) {
                entry.settled = true;
                active -= 1;
                rej(error);
              }
            },
          };
          pending.push(entry);
        })
    );
  });
  afterEach(async () => {
    await act(async () => {
      renderer?.unmount();
      pending.forEach((p) => p.resolve(emptyResponse));
    });
    vi.useRealTimers();
  });

  it('renders compact core before exact details, limits all POSTs to two and freezes actual bounds', async () => {
    await render();
    expect(pending).toHaveLength(2);
    expect(pending[0].request.include).toEqual({
      summary: true,
      summary_profile: 'compact',
      summary_comparison: true,
      timeline: true,
      timeline_profile: 'core',
      model_stats: true,
      granularity: 'hour',
    });
    expect(pending.some(isExact)).toBe(false);
    await resolve(isCore);
    expect(latest.summary.requestCount).toBe(10);
    expect(latest.loading).toBe(false);
    expect(latest.summary.p95LatencyMs).toBeNull();
    expect(latest.sections.exact.loading).toBe(true);
    expect(latest.sections.dimensions.loaded).toBe(false);
    await resolve(isDimensions);
    expect(next(isExact).request.include).toEqual({
      granularity: 'hour',
      percentiles: { summary: true, timeline: true },
      anomaly_points: true,
    });
    await finishOptional();
    expect(latest.summary.p95LatencyMs).toBe(100);
    expect(latest.summary.p95TtftMs).toBeNull();
    expect(latest.sections.anomalies.loaded).toBe(true);
    expect(latest.anomalyPoints).toEqual([]);
    expect(peakActive).toBe(2);
    const scope = pending.map(({ request: r }) => [r.from_ms, r.to_ms, r.now_ms, r.time_zone]);
    expect(scope.every((value) => JSON.stringify(value) === JSON.stringify(scope[0]))).toBe(true);
  });

  it('does not infer capability before the probe and retains legacy includes without it', async () => {
    mocks.checking = true;
    await render();
    expect(pending).toHaveLength(0);
    mocks.version = 0;
    mocks.checking = false;
    await act(async () => {
      renderer.update(
        <MemoryRouter>
          <Harness />
        </MemoryRouter>
      );
    });
    expect(pending).toHaveLength(2);
    expect(next(isCore).request.include).toMatchObject({
      summary_percentiles: true,
      channel_share: true,
      api_key_stats: true,
      anomaly_points: true,
    });
    expect(next(isCore).request.include).not.toHaveProperty('timeline_profile');
    await resolve(isCore);
    await resolve(isSelectors);
    expect(pending.some(isExact)).toBe(false);
  });

  it('isolates exact errors, waits for an explicit section retry, and keeps coverage separate', async () => {
    await finishSetup();
    const p = next(isExact);
    await act(async () => {
      p.reject(new Error('analytics deadline'));
    });
    await resolve(isSelectors);
    expect(latest.error).toBe('');
    expect(latest.summary.requestCount).toBe(10);
    expect(latest.sections.exact.error).toBe('analytics deadline');
    expect(latest.sections.anomalies.loaded).toBe(false);
    const count = pending.length;
    await act(async () => {
      latest.setTrendMetric('totalTokens');
    });
    expect(pending).toHaveLength(count);
    await act(async () => {
      latest.sections.exact.retry();
    });
    const coverage = {
      scope: 'time_range',
      mode: 'mixed',
      raw_complete: false,
      core_aggregate_used: false,
      raw_deleted_event_count: 1,
      min_deleted_timestamp_ms: 1,
      max_deleted_timestamp_ms: 2,
      fidelity_limitations: ['percentiles_require_raw_events'],
    };
    await resolve(isExact, {
      ...emptyResponse,
      coverage,
      percentiles: { summary: { p95_latency_ms: null, p95_ttft_ms: null }, timeline: [] },
    });
    expect(latest.sections.exact.loaded).toBe(true);
    expect(latest.sections.exact.coverage).toEqual(coverage);
    expect(latest.coverage).toBeUndefined();
    expect(pending.filter(isCore)).toHaveLength(1);
  });

  async function finishSetup() {
    await render();
    await finishCoreAndDimensions();
  }

  it('fetches only preview on a click, uses the authoritative bucket end and matching model pricing', async () => {
    await finishSetup();
    await finishOptional();
    const point = latest.timeline[0];
    await act(async () => {
      latest.selectBucket(point);
    });
    expect(pending.filter(isCore)).toHaveLength(1);
    expect(next(isPreview).request.include).toEqual({
      granularity: 'hour',
      drilldown_preview: {
        from_ms: point.bucketMs,
        to_ms: point.bucketEndMs,
        limit: 12,
      },
    });
    const event = {
      event_hash: 'event',
      timestamp_ms: point.bucketMs,
      model: 'model-a',
      total_tokens: 10,
    };
    await resolve(isPreview, {
      ...emptyResponse,
      drilldown_preview: { items: [event], has_more: false, next_before_ms: 0 },
    } as MonitoringAnalyticsResponse);
    expect(latest.drilldownPreview[0].estimatedCost).toBe(0.2);
    expect(latest.sections.preview.loaded).toBe(true);
  });

  it.each([0, 1])(
    'never POSTs an envelope preview for non-exact or unverified synthetic buckets (sections v%s)',
    async (version) => {
      mocks.version = version;
      await render();
      const response = coreResponse(next(isCore).request);
      response.timeline![0].bucket_range_exact = false;
      // False alone must block the request: intervals are optional, not a fallback envelope.
      await resolve(isCore, response);
      if (version === 1) {
        await resolve(isDimensions);
        await finishOptional();
      } else {
        await resolve(isSelectors);
      }
      const requestCount = pending.length;
      for (const point of [latest.timeline[0], latest.timeline[1]]) {
        expect(point.bucketRangeExact).toBe(false);
        await act(async () => {
          latest.selectBucket(point);
        });
        expect(latest.selectedBucket).toMatchObject({
          bucketMs: point.bucketMs,
          bucketRangeExact: false,
        });
        expect(latest.sections.preview).toMatchObject({
          enabled: false,
          loading: false,
          loaded: false,
          error: '',
        });
        expect(latest.drilldownPreview).toEqual([]);
        await act(async () => {
          latest.sections.preview.retry();
        });
        expect(pending).toHaveLength(requestCount);
      }
      expect(pending.some(isPreview)).toBe(false);
      expect(latest.timeline.reduce((total, point) => total + point.requestCount, 0)).toBe(10);
    }
  );

  it('aborts a previous preview when selecting a non-exact bucket and ignores its late result', async () => {
    await render();
    const response = coreResponse(next(isCore).request);
    const first = response.timeline![0];
    first.bucket_range_exact = true;
    const bucketMs = first.bucket_ms + 3_600_000;
    const bucketEndMs = bucketMs + 3_600_000;
    const bucketIntervals = [
      { from_ms: bucketMs, to_ms: bucketMs + 600_000 },
      { from_ms: bucketMs + 1_800_000, to_ms: bucketEndMs },
    ];
    response.timeline!.push({
      ...first,
      bucket_ms: bucketMs,
      bucket_end_ms: bucketEndMs,
      bucket_range_exact: false,
      bucket_intervals: bucketIntervals,
    });
    await resolve(isCore, response);
    await resolve(isDimensions);
    await finishOptional();
    await act(async () => {
      latest.selectBucket(latest.timeline[0]);
    });
    const previous = next(isPreview);
    await act(async () => {
      latest.selectBucket(latest.timeline[1]);
    });
    expect(previous.signal.aborted).toBe(true);
    expect(latest.selectedBucket?.bucketIntervals).toEqual(
      bucketIntervals.map(({ from_ms, to_ms }) => ({ fromMs: from_ms, toMs: to_ms }))
    );
    expect(latest.sections.preview.enabled).toBe(false);
    await act(async () => {
      previous.resolve({
        ...emptyResponse,
        drilldown_preview: {
          items: [{ event_hash: 'old-envelope', model: 'model-a', total_tokens: 50 }],
          has_more: false,
          next_before_ms: 0,
        },
      } as MonitoringAnalyticsResponse);
    });
    expect(latest.drilldownPreview).toEqual([]);
    expect(pending.filter(isPreview)).toHaveLength(1);
    await act(async () => {
      latest.selectBucket(latest.timeline[0]);
    });
    expect(next(isPreview).request.include?.drilldown_preview).toMatchObject({
      from_ms: first.bucket_ms,
      to_ms: first.bucket_end_ms,
    });
    expect(pending.filter(isPreview)).toHaveLength(2);
  });

  it('rejects old responses after refresh even at the same clock time and never duplicates core', async () => {
    await finishSetup();
    const old = next(isExact);
    const oldGeneration = latest.fetchGenerationKey;
    await act(async () => {
      latest.refresh();
    });
    expect(latest.fetchGenerationKey).not.toBe(oldGeneration);
    expect(latest.sections.core.loaded).toBe(false);
    expect(old.signal.aborted).toBe(true);
    await act(async () => {
      pending
        .filter((p) => p.signal.aborted)
        .forEach((p) =>
          p.resolve({
            ...emptyResponse,
            percentiles: { summary: { p95_latency_ms: 9999, p95_ttft_ms: 9999 } },
            coverage: {
              scope: 'old',
              mode: 'raw',
              raw_complete: true,
              core_aggregate_used: false,
              raw_deleted_event_count: 999,
              min_deleted_timestamp_ms: 0,
              max_deleted_timestamp_ms: 0,
              fidelity_limitations: [],
            },
          })
        );
    });
    await finishCoreAndDimensions();
    expect(latest.summary.p95LatencyMs).toBeNull();
    expect(latest.coverage).toBeUndefined();
    expect(pending.filter(isCore)).toHaveLength(2);
    expect(peakActive).toBe(2);
    await finishOptional();
    expect(latest.summary.p95LatencyMs).toBe(100);
  });

  it('keeps selectors independent of tabs and filters and drops an in-flight previous preview', async () => {
    await finishSetup();
    await finishOptional();
    await act(async () => {
      latest.selectBucket(latest.timeline[0]);
    });
    const preview = next(isPreview);
    const priorFilters = latest.fetchGenerationKey;
    await act(async () => {
      latest.setFilters({ model: 'another-model' });
    });
    expect(latest.fetchGenerationKey).not.toBe(priorFilters);
    expect(latest.sections.preview.enabled).toBe(false);
    expect(latest.selectedBucket).toBeNull();
    await act(async () => {
      preview.resolve({
        ...emptyResponse,
        drilldown_preview: {
          items: [{ event_hash: 'old', model: 'model-a', total_tokens: 50 }],
          has_more: false,
          next_before_ms: 0,
        },
      } as MonitoringAnalyticsResponse);
    });
    expect(latest.drilldownPreview).toEqual([]);
    await finishCoreAndDimensions();
    const cores = pending.filter(isCore);
    const core = cores[cores.length - 1];
    expect(core.request.filters).toMatchObject({ models: ['another-model'] });
    await resolve(isExact, {
      ...emptyResponse,
      percentiles: { summary: { p95_latency_ms: null, p95_ttft_ms: null }, timeline: [] },
    });
    expect(pending.filter(isSelectors)).toHaveLength(1);
    await act(async () => {
      latest.setActiveTab('models');
    });
    expect(pending.filter(isSelectors)).toHaveLength(1);
    expect(latest.sections.exact.enabled).toBe(false);
    expect(pending.filter(isCore)).toHaveLength(3);
  });

  it('treats a missing explicit percentile response as unavailable, not a completed empty result', async () => {
    await finishSetup();
    await resolve(isSelectors);
    await resolve(isExact);
    expect(latest.sections.exact.loaded).toBe(false);
    expect(latest.sections.exact.error).toBe('usage_analytics.section_missing');
    expect(latest.summary.p95LatencyMs).toBeNull();
    expect(latest.error).toBe('');
    const count = pending.length;
    await act(async () => {
      latest.setTrendMetric('estimatedCost');
    });
    expect(pending).toHaveLength(count);
  });

  it('does not declare failed initial metadata empty or reuse metadata from a prior connection', async () => {
    let finishOld!: (value: unknown) => void;
    mocks.meta.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finishOld = resolve;
        })
    );
    await render();
    mocks.auth.managementKey = 'new-session';
    mocks.meta.mockResolvedValue({
      authFiles: [],
      channels: [],
      authFilesLoaded: false,
      channelsLoaded: false,
      error: 'new metadata unavailable',
    });
    await act(async () => {
      renderer.update(
        <MemoryRouter>
          <Harness />
        </MemoryRouter>
      );
    });
    await act(async () => {
      finishOld({
        authFiles: [],
        channels: [],
        authFilesLoaded: true,
        channelsLoaded: true,
        error: '',
      });
    });
    expect(latest.sections.metadata).toMatchObject({
      loaded: false,
      error: 'new metadata unavailable',
    });
    expect(latest.error).toBe('');
  });

  it('keeps dimensions and metadata errors out of core, with independent retry and empty success', async () => {
    mocks.meta.mockRejectedValue(new Error('metadata offline'));
    await render();
    await resolve(isCore);
    await act(async () => {
      next(isDimensions).reject(new Error('dimensions offline'));
    });
    await finishOptional();
    expect(latest.error).toBe('');
    expect(latest.sections.metadata.error).toBe('metadata offline');
    expect(latest.sections.dimensions.error).toBe('dimensions offline');
    expect(latest.sections.dimensions.loaded).toBe(false);
    await act(async () => {
      latest.sections.dimensions.retry();
    });
    await resolve(isDimensions); // Legacy optional arrays are omitted for successful empty results.
    expect(latest.sections.dimensions.loaded).toBe(true);
    expect(latest.apiKeyRows).toEqual([]);
    expect(pending.filter(isCore)).toHaveLength(1);
    expect(pending.filter(isExact)).toHaveLength(1);
  });
});
