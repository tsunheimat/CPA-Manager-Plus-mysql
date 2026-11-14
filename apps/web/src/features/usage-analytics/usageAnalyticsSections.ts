import type {
  MonitoringAnalyticsInclude,
  MonitoringAnalyticsResponse,
  MonitoringAnalyticsCoverage,
} from '@/services/api/usageService';
import type { UseMonitoringAnalyticsReturn } from '@/features/monitoring/hooks/useMonitoringAnalytics';
import { completeUsageAnomalies, hasRequestedPercentiles } from './usageAnalyticsAnomalies';
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
  /** V2 anomalies retain both snapshots' provenance; never merge counts. */
  coreCoverage?: MonitoringAnalyticsCoverage;
  compositionSemantics?: 'core_additive_exact_latency';
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
  granularity: UsageAnalyticsResolvedGranularity,
  sectionsVersion = 1
) {
  const core = buildUsageAnalyticsInclude(tab, granularity);
  const dimensions: MonitoringAnalyticsInclude = { granularity };
  // Heatmap pricing can scan the entire raw range. Keep it out of the compact
  // summary POST, just like the other independently retriable dimensions.
  for (const key of ['channel_share', 'api_key_stats', 'credential_stats', 'heatmap'] as const) {
    if (core[key]) {
      dimensions[key] = true;
      delete core[key];
    }
  }
  const exact: MonitoringAnalyticsInclude = { granularity };
  if (core.timeline) core.timeline_profile = 'core';
  if (tab === 'overview' || tab === 'trends') {
    exact.percentiles = { summary: tab === 'overview', timeline: true };
    if (sectionsVersion >= 2) {
      core.anomaly_basis_version = 1;
    } else {
      // Capability 1 keeps server-computed anomalies and its priced timeline.
      exact.anomaly_points = true;
    }
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
  preview: MonitoringAnalyticsResponse | null,
  sectionsVersion = 1,
  requestedPercentiles?: MonitoringAnalyticsInclude['percentiles']
): MonitoringAnalyticsResponse | null {
  if (!core) return null;
  if (sectionsVersion >= 2 && !hasRequestedPercentiles(exact, requestedPercentiles)) exact = null;
  const byBucket = new Map(exact?.percentiles?.timeline?.map((point) => [point.bucket_ms, point]));
  return {
    ...core,
    ...(dimensions
      ? {
          api_key_stats: dimensions.api_key_stats,
          credential_stats: dimensions.credential_stats,
          channel_share: dimensions.channel_share,
          heatmap: dimensions.heatmap,
        }
      : {}),
    anomaly_points:
      sectionsVersion >= 2 ? completeUsageAnomalies(core, exact) : exact?.anomaly_points,
    percentiles: exact?.percentiles,
    summary: core.summary ? { ...core.summary, ...exact?.percentiles?.summary } : undefined,
    timeline: core.timeline?.map((point) => {
      const percentiles = byBucket.get(point.bucket_ms);
      return percentiles ? { ...point, ...percentiles } : point;
    }),
    drilldown_preview: preview?.drilldown_preview,
  };
}
