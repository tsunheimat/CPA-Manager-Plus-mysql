import type {
  MonitoringAnalyticsInclude,
  MonitoringAnalyticsResponse,
  MonitoringAnalyticsCoverage,
} from '@/services/api/usageService';
import type { UseMonitoringAnalyticsReturn } from '@/features/monitoring/hooks/useMonitoringAnalytics';
import {
  buildUsageAnalyticsInclude,
  type UsageAnalyticsTab,
  type UsageAnalyticsResolvedGranularity,
} from './usageAnalyticsModel';

export interface UsageAnalyticsSectionState {
  enabled: boolean;
  loading: boolean;
  loaded: boolean;
  error: string;
  stale: boolean;
  coverage?: MonitoringAnalyticsCoverage;
  retry: () => void;
}

export function currentAnalyticsData(analytics: UseMonitoringAnalyticsReturn) {
  return analytics.dataStale || !analytics.enabled ? null : analytics.data;
}

export function analyticsSectionState(
  analytics: UseMonitoringAnalyticsReturn,
  enabled: boolean,
  complete = true
): UsageAnalyticsSectionState {
  const missing = Boolean(analytics.data && !analytics.dataStale && !complete);
  return {
    enabled,
    loading:
      enabled &&
      (analytics.loading ||
        (!analytics.unavailableReason &&
          !analytics.error &&
          (!analytics.data || analytics.dataStale))),
    loaded: enabled && !analytics.dataStale && Boolean(analytics.data) && complete,
    error: enabled ? analytics.error || (missing ? 'usage_analytics.section_missing' : '') : '',
    stale: enabled && analytics.dataStale,
    coverage: enabled && !analytics.dataStale ? analytics.data?.coverage : undefined,
    retry: () => {
      void analytics.refresh({ force: true });
    },
  };
}

export function buildUsageAnalyticsSections(
  tab: UsageAnalyticsTab,
  granularity: UsageAnalyticsResolvedGranularity
) {
  const core = buildUsageAnalyticsInclude(tab, granularity);
  const dimensions: MonitoringAnalyticsInclude = { granularity };
  for (const key of ['channel_share', 'api_key_stats', 'credential_stats'] as const) {
    if (core[key]) {
      dimensions[key] = true;
      delete core[key];
    }
  }
  const exact: MonitoringAnalyticsInclude = { granularity };
  if (core.timeline) core.timeline_profile = 'core';
  if (tab === 'overview' || tab === 'trends') {
    exact.percentiles = { summary: tab === 'overview', timeline: true };
    // The service reuses the same exact bucket percentiles for anomaly detection.
    exact.anomaly_points = true;
  }
  delete core.summary_percentiles;
  delete core.anomaly_points;
  return { core, dimensions, exact };
}

/** Callers pass only current-generation responses. These POSTs are NOT a shared DB snapshot.
 * Coverage is deliberately NOT cross-merged: each section displays its own warning.
 */
export function mergeUsageAnalyticsSections(
  core: MonitoringAnalyticsResponse | null,
  dimensions: MonitoringAnalyticsResponse | null,
  exact: MonitoringAnalyticsResponse | null,
  preview: MonitoringAnalyticsResponse | null
): MonitoringAnalyticsResponse | null {
  if (!core) return null;
  const byBucket = new Map(exact?.percentiles?.timeline?.map((point) => [point.bucket_ms, point]));
  return {
    ...core,
    ...(dimensions
      ? {
          api_key_stats: dimensions.api_key_stats,
          credential_stats: dimensions.credential_stats,
          channel_share: dimensions.channel_share,
        }
      : {}),
    anomaly_points: exact?.anomaly_points,
    percentiles: exact?.percentiles,
    summary: core.summary ? { ...core.summary, ...exact?.percentiles?.summary } : undefined,
    timeline: core.timeline?.map((point) => {
      const percentiles = byBucket.get(point.bucket_ms);
      return percentiles ? { ...point, ...percentiles } : point;
    }),
    drilldown_preview: preview?.drilldown_preview,
  };
}
