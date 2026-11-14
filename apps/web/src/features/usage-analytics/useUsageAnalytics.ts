import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import {
  createMonitoringAnalyticsScheduler,
  useMonitoringAnalytics,
} from '@/features/monitoring/hooks/useMonitoringAnalytics';
import { usePanelFeatureAvailability } from '@/hooks/usePanelFeatureAvailability';
import {
  analyticsSectionState,
  buildUsageAnalyticsSections,
  currentAnalyticsData,
  mergeUsageAnalyticsSections,
} from './usageAnalyticsSections';
import { useApiKeyAliases } from '@/features/monitoring/hooks/useApiKeyAliases';
import { hasAnomalyBasis, hasRequestedPercentiles } from './usageAnalyticsAnomalies';
import { buildApiKeyDisplayMap } from '@/features/monitoring/model/apiKeys';
import { buildMonitoringAuthMetaMap } from '@/features/monitoring/model/authMeta';
import { readString } from '@/features/monitoring/model/base';
import type { MonitoringChannelMeta } from '@/features/monitoring/model/types';
import { loadMonitoringMetaPayload } from '@/features/monitoring/services/monitoringMetaService';
import { useAuthStore, useConfigStore } from '@/stores';
import type { AuthFileItem } from '@/types/authFile';
import type { CredentialInfo } from '@/types/sourceInfo';
import { buildSourceInfoMap } from '@/utils/sourceResolver';
import { normalizeAuthIndex } from '@/utils/usage';
import {
  adaptUsageAnalyticsData,
  analyzeUsageBucket,
  buildApiKeyTrendSeries,
  buildSelectedApiKeyTrendSeries,
  buildSelectedCredentialTrendSeries,
  buildCredentialQuotaRows,
  buildEntityTrendSeries,
  buildKeyAnomalies,
  buildUsageInsights,
  buildUsageHeatmap,
  buildUsageHeatmapCellDetail,
  buildUsageHeatmapCellDateOptions,
  buildUsageHeatmapHighlights,
  buildUsageHeatmapRangeContext,
  buildUsageMatrix,
  buildUsageSummaryDelta,
  buildUsageCredentialTimeline,
  buildUsageApiKeyTimeline,
  buildUsageAnalyticsFilters,
  buildUsageAnalyticsFilterSelectorsInclude,
  buildUsageAnalyticsInclude,
  buildUsageTimeline,
  canDrilldownUsageBucket,
  getSelectableApiKeyHash,
  getUsagePresentationRangeScopeKey,
  getUsageRangeBounds,
  resolveUsageGranularity,
  USAGE_ANALYTICS_DEFAULT_FILTERS,
  type UsageMatrixDimension,
  type UsageMatrixMetricKey,
  type UsageTrendMetricKey,
  type UsageAnalyticsFiltersState,
  type UsageAnalyticsTab,
  type UsageSelectedFilterKey,
  type UsageAnomalyAnalysis,
  type UsageHeatmapCellSelection,
  type UsageHeatmapDateOption,
  type UsageHeatmapMetricKey,
  type UsageHeatmapScaleMode,
  type UsageTimelinePoint,
  type UsageCredentialDisplayContext,
} from './usageAnalyticsModel';
import {
  buildUsageAnalyticsSearchParams,
  buildUsageAnalyticsUiStateFromSearchParams,
  readUsageAnalyticsUiState,
  writeUsageAnalyticsUiState,
  type UsageAnalyticsUiState,
} from './usageAnalyticsUiState';

const USAGE_SEARCH_DEBOUNCE_MS = 350;
const USAGE_HEATMAP_ALL_DATES_KEY = 'all';
const API_KEY_TREND_SERIES_LIMIT = 4;

type UsageAnalyticsMonitoringMeta = {
  authFiles: AuthFileItem[];
  channels: MonitoringChannelMeta[];
  channelsLoaded: boolean;
};

const EMPTY_USAGE_ANALYTICS_MONITORING_META: UsageAnalyticsMonitoringMeta = {
  authFiles: [],
  channels: [],
  channelsLoaded: false,
};

const getBrowserTimeZone = () => {
  if (typeof Intl === 'undefined') return 'UTC';
  return Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
};

function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debouncedValue, setDebouncedValue] = useState(value);

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedValue(value), delayMs);
    return () => clearTimeout(timer);
  }, [delayMs, value]);

  return debouncedValue;
}

export function useUsageAnalytics() {
  const availability = usePanelFeatureAvailability();
  const sectionsVersion = availability.analyticsSectionsVersion ?? 0;
  const progressive = sectionsVersion >= 1;
  const composedAnomalies = sectionsVersion >= 2;
  const scheduler = useMemo(() => createMonitoringAnalyticsScheduler(2), []);
  const [refreshId, setRefreshId] = useState(0);
  const config = useConfigStore((state) => state.config);
  const apiBase = useAuthStore((state) => state.apiBase);
  const managementKey = useAuthStore((state) => state.managementKey);
  const metadataScope = JSON.stringify([apiBase, managementKey]);
  const metadataScopeRef = useRef(metadataScope);
  metadataScopeRef.current = metadataScope;
  const { apiKeyAliases, loadApiKeyAliases } = useApiKeyAliases();
  const [monitoringMeta, setMonitoringMeta] = useState<UsageAnalyticsMonitoringMeta>(
    EMPTY_USAGE_ANALYTICS_MONITORING_META
  );
  const [monitoringMetaError, setMonitoringMetaError] = useState('');
  const [monitoringMetaLoading, setMonitoringMetaLoading] = useState(true);
  const [monitoringMetaLoaded, setMonitoringMetaLoaded] = useState(false);
  const monitoringMetaRequestId = useRef(0);
  const [searchParams, setSearchParams] = useSearchParams();
  const [initialUiState] = useState<UsageAnalyticsUiState>(() =>
    buildUsageAnalyticsUiStateFromSearchParams(searchParams, readUsageAnalyticsUiState())
  );
  const [filters, setFiltersState] = useState<UsageAnalyticsFiltersState>(() => ({
    ...initialUiState.filters,
    apiKeyHash: getSelectableApiKeyHash(initialUiState.filters.apiKeyHash) || 'all',
  }));
  const [activeTabState, setActiveTabState] = useState<UsageAnalyticsTab>(
    () => initialUiState.activeTab
  );
  const [nowMs, setNowMs] = useState(() => Date.now());
  const [selectedBucketMs, setSelectedBucketMs] = useState<number | null>(null);
  const [selectedModelId, setSelectedModelId] = useState('');
  const [selectedApiKeyHash, setSelectedApiKeyHash] = useState('');
  const [selectedCredentialId, setSelectedCredentialId] = useState('');
  const [trendMetric, setTrendMetric] = useState<UsageTrendMetricKey>('requestCount');
  const [matrixDimension, setMatrixDimension] = useState<UsageMatrixDimension>('apiKeyModel');
  const [matrixMetric, setMatrixMetric] = useState<UsageMatrixMetricKey>('requestCount');
  const [heatmapMetric, setHeatmapMetric] = useState<UsageHeatmapMetricKey>('requestCount');
  const [heatmapScaleMode, setHeatmapScaleMode] = useState<UsageHeatmapScaleMode>('absolute');
  const [selectedHeatmapDateKey, setSelectedHeatmapDateKey] = useState(USAGE_HEATMAP_ALL_DATES_KEY);
  const [selectedHeatmapCell, setSelectedHeatmapCell] = useState<UsageHeatmapCellSelection | null>(
    null
  );
  const browserTimeZone = useMemo(() => getBrowserTimeZone(), []);
  const apiKeyDisplayMap = useMemo(
    () => buildApiKeyDisplayMap(config?.apiKeys || [], apiKeyAliases || []),
    [apiKeyAliases, config?.apiKeys]
  );
  const loadMonitoringMeta = useCallback(async () => {
    if (metadataScopeRef.current !== metadataScope) return;
    const requestId = ++monitoringMetaRequestId.current;
    setMonitoringMetaLoading(true);
    setMonitoringMetaError('');
    try {
      const payload = await loadMonitoringMetaPayload(config);
      if (
        requestId !== monitoringMetaRequestId.current ||
        metadataScopeRef.current !== metadataScope
      )
        return;
      setMonitoringMeta((current) => ({
        authFiles: payload.authFilesLoaded ? payload.authFiles : current.authFiles,
        // Use configuration fallback until the first successful channel snapshot.
        // Even an empty successful response is authoritative on later failures.
        channels:
          payload.channelsLoaded || !current.channelsLoaded ? payload.channels : current.channels,
        channelsLoaded: payload.channelsLoaded || current.channelsLoaded,
      }));
      if (payload.authFilesLoaded || payload.channelsLoaded) setMonitoringMetaLoaded(true);
      setMonitoringMetaError(payload.error);
    } catch (error) {
      if (requestId === monitoringMetaRequestId.current) {
        setMonitoringMetaError(error instanceof Error ? error.message : String(error));
      }
    } finally {
      if (requestId === monitoringMetaRequestId.current) setMonitoringMetaLoading(false);
    }
  }, [config, metadataScope]);
  useEffect(() => {
    setMonitoringMeta(EMPTY_USAGE_ANALYTICS_MONITORING_META);
    setMonitoringMetaLoaded(false);
  }, [metadataScope]);
  useEffect(() => {
    void loadMonitoringMeta();
    return () => {
      monitoringMetaRequestId.current += 1;
    };
  }, [loadMonitoringMeta]);
  const authMetaMap = useMemo(
    () => buildMonitoringAuthMetaMap(monitoringMeta.authFiles),
    [monitoringMeta.authFiles]
  );
  const authFileMap = useMemo(() => {
    const map = new Map<string, CredentialInfo>();
    monitoringMeta.authFiles.forEach((entry) => {
      const authIndex = normalizeAuthIndex(entry['auth_index'] ?? entry.authIndex);
      if (!authIndex) return;
      map.set(authIndex, {
        name:
          readString(entry.label) ||
          readString(entry.name) ||
          readString(entry.email) ||
          readString(entry.account) ||
          authIndex,
        type: readString(entry.provider) || readString(entry.type),
      });
    });
    return map;
  }, [monitoringMeta.authFiles]);
  const sourceInfoMap = useMemo(
    () =>
      buildSourceInfoMap({
        geminiApiKeys: config?.geminiApiKeys || [],
        claudeApiKeys: config?.claudeApiKeys || [],
        codexApiKeys: config?.codexApiKeys || [],
        xaiApiKeys: config?.xaiApiKeys || [],
        metaApiKeys: config?.metaApiKeys || [],
        vertexApiKeys: config?.vertexApiKeys || [],
        openaiCompatibility: config?.openaiCompatibility || [],
      }),
    [config]
  );
  const channelByAuthIndex = useMemo(() => {
    const map = new Map<string, MonitoringChannelMeta>();
    monitoringMeta.channels.forEach((channel) => {
      channel.authIndices.forEach((authIndex) => {
        map.set(authIndex, channel);
      });
    });
    return map;
  }, [monitoringMeta.channels]);
  const credentialDisplayContext = useMemo<UsageCredentialDisplayContext>(
    () => ({
      authMetaMap,
      authFileMap,
      sourceInfoMap,
      channelByAuthIndex,
    }),
    [authFileMap, authMetaMap, channelByAuthIndex, sourceInfoMap]
  );
  const setActiveTab = useCallback((tab: UsageAnalyticsTab) => {
    setActiveTabState(tab);
    writeUsageAnalyticsUiState({ activeTab: tab });
  }, []);
  const debouncedSearchQuery = useDebouncedValue(
    filters.searchQuery.trim(),
    USAGE_SEARCH_DEBOUNCE_MS
  );

  const bounds = useMemo(() => getUsageRangeBounds(filters, nowMs), [filters, nowMs]);
  const presentationRangeScopeKey = getUsagePresentationRangeScopeKey(filters, bounds);
  const heatmapRangeContext = useMemo(
    () => buildUsageHeatmapRangeContext(bounds, 'en-US', browserTimeZone),
    [bounds, browserTimeZone]
  );
  const heatmapDateOptions = useMemo(
    () => buildUsageHeatmapCellDateOptions(heatmapRangeContext, selectedHeatmapCell),
    [heatmapRangeContext, selectedHeatmapCell]
  );
  const selectedHeatmapDate = useMemo<UsageHeatmapDateOption | null>(
    () =>
      selectedHeatmapDateKey === USAGE_HEATMAP_ALL_DATES_KEY
        ? null
        : (heatmapDateOptions.find((option) => option.key === selectedHeatmapDateKey) ?? null),
    [heatmapDateOptions, selectedHeatmapDateKey]
  );
  const activeHeatmapDateKey = selectedHeatmapDate
    ? selectedHeatmapDateKey
    : USAGE_HEATMAP_ALL_DATES_KEY;
  const resolvedGranularity = useMemo(
    () => resolveUsageGranularity(filters, nowMs),
    [filters, nowMs]
  );
  const analyticsFilters = useMemo(
    () =>
      buildUsageAnalyticsFilters({
        ...filters,
        apiKeyHash: getSelectableApiKeyHash(filters.apiKeyHash) || 'all',
      }),
    [filters]
  );

  useEffect(() => {
    const nextState = { activeTab: activeTabState, filters };
    writeUsageAnalyticsUiState(nextState);
    const nextParams = buildUsageAnalyticsSearchParams(nextState);
    if (nextParams.toString() !== searchParams.toString()) {
      setSearchParams(nextParams, { replace: true });
    }
  }, [activeTabState, filters, searchParams, setSearchParams]);
  // Bounds, now, timezone and refresh identity are shared by the actual POSTs, not
  // merely a rounded presentation key. Optional responses never cross this fence.
  const fetchGenerationKey = JSON.stringify({
    connection: [apiBase, availability.managerServiceBase, managementKey],
    bounds,
    nowMs,
    refreshId,
    timeZone: browserTimeZone,
    filters: analyticsFilters,
    searchQuery: debouncedSearchQuery,
    granularity: resolvedGranularity,
  });
  const selectorGenerationKey = JSON.stringify({
    connection: [apiBase, availability.managerServiceBase, managementKey],
    bounds,
    nowMs,
    refreshId,
    timeZone: browserTimeZone,
    searchQuery: debouncedSearchQuery,
  });
  const sectionIncludes = useMemo(
    () => buildUsageAnalyticsSections(activeTabState, resolvedGranularity, sectionsVersion),
    [activeTabState, resolvedGranularity, sectionsVersion]
  );
  const include = useMemo(
    () =>
      progressive
        ? sectionIncludes.core
        : buildUsageAnalyticsInclude(activeTabState, resolvedGranularity),
    [activeTabState, progressive, resolvedGranularity, sectionIncludes]
  );
  const dataScopeKey = useMemo(
    () =>
      JSON.stringify({
        activeTab: activeTabState,
        presentationRangeScopeKey,
        filters: analyticsFilters,
        granularity: resolvedGranularity,
        searchQuery: debouncedSearchQuery,
      }),
    [
      activeTabState,
      analyticsFilters,
      presentationRangeScopeKey,
      debouncedSearchQuery,
      resolvedGranularity,
    ]
  );

  const analytics = useMonitoringAnalytics({
    enabled: !availability.checking,
    fetchGenerationKey,
    scheduler,
    priority: 0,
    timeZone: browserTimeZone,
    fromMs: bounds?.fromMs,
    toMs: bounds?.toMs,
    nowMs,
    dataScopeKey,
    searchQuery: debouncedSearchQuery,
    filters: analyticsFilters,
    include,
    throttleMs: 0,
  });

  const coreData = currentAnalyticsData(analytics);
  const hasDimensions = Object.keys(sectionIncludes.dimensions).length > 1;
  const hasExact = Boolean(sectionIncludes.exact.percentiles);
  const basisComplete = !composedAnomalies || !hasExact || hasAnomalyBasis(coreData);
  const basisError = composedAnomalies && hasExact && Boolean(coreData) && !basisComplete;
  // The normal request hook also fences credentials/base and all POST inputs.
  // Adding core_id prevents an exact result from an earlier core in the same
  // generation (including a retry) from ever completing this basis.
  const exactGenerationKey = composedAnomalies
    ? JSON.stringify([fetchGenerationKey, coreData?.anomaly_basis?.core_id ?? null])
    : fetchGenerationKey;
  const dimensionsAnalytics = useMonitoringAnalytics({
    enabled: progressive && !availability.checking && hasDimensions,
    fromMs: bounds?.fromMs,
    toMs: bounds?.toMs,
    nowMs,
    fetchGenerationKey,
    scheduler,
    priority: 2,
    timeZone: browserTimeZone,
    searchQuery: debouncedSearchQuery,
    filters: analyticsFilters,
    include: sectionIncludes.dimensions,
    throttleMs: 0,
  });
  const exactAnalytics = useMonitoringAnalytics({
    enabled: progressive && Boolean(coreData?.summary) && hasExact && basisComplete,
    fromMs: bounds?.fromMs,
    toMs: bounds?.toMs,
    nowMs,
    fetchGenerationKey: exactGenerationKey,
    scheduler,
    priority: 1,
    timeZone: browserTimeZone,
    searchQuery: debouncedSearchQuery,
    filters: analyticsFilters,
    include: sectionIncludes.exact,
    throttleMs: 0,
  });
  const previewBucket = useMemo(() => {
    if (selectedBucketMs === null || (activeTabState !== 'overview' && activeTabState !== 'trends'))
      return null;
    return (
      adaptUsageAnalyticsData(
        coreData,
        resolvedGranularity,
        '',
        undefined,
        undefined,
        bounds
      ).timeline.find((point) => point.bucketMs === selectedBucketMs) ?? null
    );
  }, [activeTabState, bounds, coreData, resolvedGranularity, selectedBucketMs]);
  const previewEnabled = canDrilldownUsageBucket(previewBucket);
  const previewInclude = useMemo(
    () => ({
      granularity: resolvedGranularity,
      ...(previewEnabled && previewBucket
        ? {
            drilldown_preview: {
              from_ms: Math.max(previewBucket.bucketMs, bounds?.fromMs ?? previewBucket.bucketMs),
              to_ms: Math.min(previewBucket.bucketEndMs, bounds?.toMs ?? previewBucket.bucketEndMs),
              limit: 12,
            },
          }
        : {}),
    }),
    [bounds, previewBucket, previewEnabled, resolvedGranularity]
  );
  const previewAnalytics = useMonitoringAnalytics({
    enabled: previewEnabled,
    fromMs: bounds?.fromMs,
    toMs: bounds?.toMs,
    nowMs,
    fetchGenerationKey,
    scheduler,
    priority: 0,
    timeZone: browserTimeZone,
    searchQuery: debouncedSearchQuery,
    filters: analyticsFilters,
    include: previewInclude,
    throttleMs: 0,
  });

  const filterSelectorsInclude = useMemo(() => buildUsageAnalyticsFilterSelectorsInclude(), []);
  const filterSelectorsDataScopeKey = useMemo(
    () =>
      JSON.stringify({
        presentationRangeScopeKey,
        searchQuery: debouncedSearchQuery,
      }),
    [presentationRangeScopeKey, debouncedSearchQuery]
  );
  const filterSelectorsAnalytics = useMonitoringAnalytics({
    enabled: !availability.checking,
    fetchGenerationKey: selectorGenerationKey,
    scheduler,
    priority: 3,
    timeZone: browserTimeZone,
    fromMs: bounds?.fromMs,
    toMs: bounds?.toMs,
    nowMs,
    dataScopeKey: filterSelectorsDataScopeKey,
    searchQuery: debouncedSearchQuery,
    include: filterSelectorsInclude,
    throttleMs: 0,
  });

  const heatmapDateInclude = useMemo(
    () => ({
      granularity: 'hour' as const,
      heatmap: true,
    }),
    []
  );
  const heatmapDateDataScopeKey = useMemo(
    () =>
      JSON.stringify({
        date: selectedHeatmapDate
          ? {
              fromMs: selectedHeatmapDate.fromMs,
              key: selectedHeatmapDate.key,
              toMs: selectedHeatmapDate.toMs,
            }
          : null,
        filters: analyticsFilters,
        searchQuery: debouncedSearchQuery,
      }),
    [analyticsFilters, debouncedSearchQuery, selectedHeatmapDate]
  );
  const heatmapDateAnalytics = useMonitoringAnalytics({
    enabled: !availability.checking && activeTabState === 'heatmap' && Boolean(selectedHeatmapDate),
    fetchGenerationKey,
    scheduler,
    timeZone: browserTimeZone,
    fromMs: selectedHeatmapDate?.fromMs,
    toMs: selectedHeatmapDate?.toMs,
    nowMs,
    dataScopeKey: heatmapDateDataScopeKey,
    searchQuery: debouncedSearchQuery,
    filters: analyticsFilters,
    include: heatmapDateInclude,
    throttleMs: 0,
  });

  const dimensionsData = currentAnalyticsData(dimensionsAnalytics);
  const exactData = currentAnalyticsData(exactAnalytics);
  const previewData = currentAnalyticsData(previewAnalytics);
  const analyticsData = useMemo(
    () =>
      progressive
        ? mergeUsageAnalyticsSections(
            coreData,
            dimensionsData,
            exactData,
            previewData,
            sectionsVersion,
            sectionIncludes.exact.percentiles
          )
        : coreData
          ? { ...coreData, drilldown_preview: previewData?.drilldown_preview }
          : null,
    [
      coreData,
      dimensionsData,
      exactData,
      previewData,
      progressive,
      sectionsVersion,
      sectionIncludes.exact.percentiles,
    ]
  );
  const filterSelectorsData = currentAnalyticsData(filterSelectorsAnalytics);
  const timelineBounds = include.timeline ? bounds : null;
  const adapted = useMemo(
    () =>
      adaptUsageAnalyticsData(
        analyticsData,
        resolvedGranularity,
        filters.apiKeyKeyword,
        apiKeyDisplayMap,
        credentialDisplayContext,
        timelineBounds
      ),
    [
      analyticsData,
      apiKeyDisplayMap,
      credentialDisplayContext,
      filters.apiKeyKeyword,
      timelineBounds,
      resolvedGranularity,
    ]
  );
  const apiKeyTrendRows = useMemo(
    () => adapted.apiKeyRows.filter((row) => Boolean(getSelectableApiKeyHash(row.apiKeyHash))),
    [adapted.apiKeyRows]
  );
  const apiKeyTrendHashes = useMemo(() => {
    if (activeTabState !== 'overview' && activeTabState !== 'trends') return [];
    return Array.from(
      new Set(apiKeyTrendRows.map((row) => getSelectableApiKeyHash(row.apiKeyHash)))
    ).slice(0, API_KEY_TREND_SERIES_LIMIT);
  }, [activeTabState, apiKeyTrendRows]);
  const apiKeyTrendFilters = useMemo(
    () =>
      apiKeyTrendHashes.length > 0
        ? { ...analyticsFilters, api_key_hashes: apiKeyTrendHashes }
        : analyticsFilters,
    [analyticsFilters, apiKeyTrendHashes]
  );
  const apiKeyTrendInclude = useMemo(
    () => ({
      granularity: resolvedGranularity,
      api_key_timeline: true,
    }),
    [resolvedGranularity]
  );
  const apiKeyTrendDataScopeKey = useMemo(
    () =>
      JSON.stringify({
        activeTab: activeTabState,
        apiKeyHashes: apiKeyTrendHashes,
        presentationRangeScopeKey,
        filters: apiKeyTrendFilters,
        granularity: resolvedGranularity,
        searchQuery: debouncedSearchQuery,
      }),
    [
      activeTabState,
      apiKeyTrendFilters,
      apiKeyTrendHashes,
      presentationRangeScopeKey,
      debouncedSearchQuery,
      resolvedGranularity,
    ]
  );
  const apiKeyTrendAnalytics = useMonitoringAnalytics({
    enabled: !availability.checking,
    fetchGenerationKey,
    scheduler,
    timeZone: browserTimeZone,
    fromMs:
      (activeTabState === 'overview' || activeTabState === 'trends') && apiKeyTrendHashes.length > 0
        ? bounds?.fromMs
        : undefined,
    toMs:
      (activeTabState === 'overview' || activeTabState === 'trends') && apiKeyTrendHashes.length > 0
        ? bounds?.toMs
        : undefined,
    nowMs,
    dataScopeKey: apiKeyTrendDataScopeKey,
    searchQuery: debouncedSearchQuery,
    filters: apiKeyTrendFilters,
    include: apiKeyTrendInclude,
    throttleMs: 0,
  });
  const apiKeyTrendData = apiKeyTrendAnalytics.dataStale ? null : apiKeyTrendAnalytics.data;
  const apiKeyTimeline = useMemo(
    () => buildUsageApiKeyTimeline(apiKeyTrendData?.api_key_timeline ?? [], resolvedGranularity),
    [apiKeyTrendData, resolvedGranularity]
  );
  const hasExactAPIKeyTimeline = Array.isArray(apiKeyTrendData?.api_key_timeline);
  const heatmapDateData = heatmapDateAnalytics.dataStale ? null : heatmapDateAnalytics.data;
  const heatmapDateRows = useMemo(
    () => buildUsageHeatmap(heatmapDateData?.heatmap ?? [], apiKeyDisplayMap),
    [apiKeyDisplayMap, heatmapDateData]
  );
  const heatmapDetailSource = selectedHeatmapDate ? heatmapDateRows : adapted.heatmap;
  const heatmapDateSection = analyticsSectionState(
    heatmapDateAnalytics,
    activeTabState === 'heatmap' && Boolean(selectedHeatmapDate)
  );
  const summaryDelta = useMemo(
    () => buildUsageSummaryDelta(adapted.summary, adapted.summaryComparison),
    [adapted.summary, adapted.summaryComparison]
  );

  const selectedBucket = useMemo(
    () =>
      selectedBucketMs === null
        ? null
        : (adapted.timeline.find((point) => point.bucketMs === selectedBucketMs) ?? null),
    [adapted.timeline, selectedBucketMs]
  );

  const anomalyAnalysis = useMemo<UsageAnomalyAnalysis | null>(
    () =>
      selectedBucketMs === null ? null : analyzeUsageBucket(adapted.timeline, selectedBucketMs),
    [adapted.timeline, selectedBucketMs]
  );

  const selectedModel =
    adapted.modelRows.find((row) => row.id === selectedModelId) ?? adapted.modelRows[0] ?? null;
  const selectedApiKey =
    adapted.apiKeyRows.find((row) => {
      const apiKeyHash = getSelectableApiKeyHash(row.apiKeyHash);
      return Boolean(apiKeyHash) && apiKeyHash === selectedApiKeyHash;
    }) ??
    adapted.apiKeyRows.find((row) => Boolean(getSelectableApiKeyHash(row.apiKeyHash))) ??
    adapted.apiKeyRows[0] ??
    null;
  const selectedCredential =
    adapted.credentialRows.find((row) => row.id === selectedCredentialId) ??
    adapted.credentialRows[0] ??
    null;

  const modelTrendSeries = useMemo(
    () => buildEntityTrendSeries(adapted.modelRows, adapted.timeline, trendMetric, 4),
    [adapted.modelRows, adapted.timeline, trendMetric]
  );
  const apiKeyTrendSeries = useMemo(
    () =>
      hasExactAPIKeyTimeline
        ? buildApiKeyTrendSeries(
            apiKeyTrendRows,
            adapted.timeline,
            apiKeyTimeline,
            trendMetric,
            API_KEY_TREND_SERIES_LIMIT
          )
        : progressive
          ? []
          : buildEntityTrendSeries(
              apiKeyTrendRows,
              adapted.timeline,
              trendMetric,
              API_KEY_TREND_SERIES_LIMIT,
              adapted.apiKeyRows
            ),
    [
      apiKeyTimeline,
      apiKeyTrendRows,
      adapted.apiKeyRows,
      adapted.timeline,
      hasExactAPIKeyTimeline,
      progressive,
      trendMetric,
    ]
  );
  const selectedApiKeyFilterHash = getSelectableApiKeyHash(selectedApiKey?.apiKeyHash);
  const selectedApiKeyTimelineFilters = useMemo(
    () =>
      selectedApiKeyFilterHash
        ? buildUsageAnalyticsFilters({ ...filters, apiKeyHash: selectedApiKeyFilterHash })
        : {},
    [filters, selectedApiKeyFilterHash]
  );
  const selectedApiKeyTimelineInclude = useMemo(
    () => ({
      granularity: resolvedGranularity,
      timeline: true,
      ...(progressive ? { timeline_profile: 'core' as const } : {}),
    }),
    [progressive, resolvedGranularity]
  );
  const selectedApiKeyTimelineDataScopeKey = useMemo(
    () =>
      JSON.stringify({
        activeTab: activeTabState,
        presentationRangeScopeKey,
        filters: selectedApiKeyTimelineFilters,
        granularity: resolvedGranularity,
        searchQuery: debouncedSearchQuery,
        selectedApiKeyHash: selectedApiKeyFilterHash,
      }),
    [
      activeTabState,
      presentationRangeScopeKey,
      debouncedSearchQuery,
      resolvedGranularity,
      selectedApiKeyFilterHash,
      selectedApiKeyTimelineFilters,
    ]
  );
  const selectedApiKeyTimelineAnalytics = useMonitoringAnalytics({
    enabled: !availability.checking,
    fetchGenerationKey,
    scheduler,
    timeZone: browserTimeZone,
    fromMs: activeTabState === 'apiKeys' && selectedApiKeyFilterHash ? bounds?.fromMs : undefined,
    toMs: activeTabState === 'apiKeys' && selectedApiKeyFilterHash ? bounds?.toMs : undefined,
    nowMs,
    dataScopeKey: selectedApiKeyTimelineDataScopeKey,
    searchQuery: debouncedSearchQuery,
    filters: selectedApiKeyTimelineFilters,
    include: selectedApiKeyTimelineInclude,
    throttleMs: 0,
  });
  const selectedApiKeyTimelineData = selectedApiKeyTimelineAnalytics.dataStale
    ? null
    : selectedApiKeyTimelineAnalytics.data;
  const selectedApiKeyTimeline = useMemo(
    () => buildUsageTimeline(selectedApiKeyTimelineData?.timeline ?? [], resolvedGranularity),
    [resolvedGranularity, selectedApiKeyTimelineData]
  );
  const selectedApiKeyTrendSeries = useMemo(
    () => buildSelectedApiKeyTrendSeries(selectedApiKey, selectedApiKeyTimeline, trendMetric),
    [selectedApiKey, selectedApiKeyTimeline, trendMetric]
  );
  const selectedCredentialFilterID = selectedCredential?.id || '';
  const selectedCredentialTimelineFilters = useMemo(
    () =>
      selectedCredentialFilterID
        ? { ...analyticsFilters, credential_ids: [selectedCredentialFilterID] }
        : analyticsFilters,
    [analyticsFilters, selectedCredentialFilterID]
  );
  const selectedCredentialTimelineInclude = useMemo(
    () => ({
      granularity: resolvedGranularity,
      credential_timeline: true,
    }),
    [resolvedGranularity]
  );
  const selectedCredentialTimelineDataScopeKey = useMemo(
    () =>
      JSON.stringify({
        activeTab: activeTabState,
        presentationRangeScopeKey,
        filters: selectedCredentialTimelineFilters,
        granularity: resolvedGranularity,
        searchQuery: debouncedSearchQuery,
        selectedCredentialID: selectedCredentialFilterID,
      }),
    [
      activeTabState,
      presentationRangeScopeKey,
      debouncedSearchQuery,
      resolvedGranularity,
      selectedCredentialFilterID,
      selectedCredentialTimelineFilters,
    ]
  );
  const selectedCredentialTimelineAnalytics = useMonitoringAnalytics({
    enabled: !availability.checking,
    fetchGenerationKey,
    scheduler,
    timeZone: browserTimeZone,
    fromMs:
      activeTabState === 'credentials' && selectedCredentialFilterID ? bounds?.fromMs : undefined,
    toMs: activeTabState === 'credentials' && selectedCredentialFilterID ? bounds?.toMs : undefined,
    nowMs,
    dataScopeKey: selectedCredentialTimelineDataScopeKey,
    searchQuery: debouncedSearchQuery,
    filters: selectedCredentialTimelineFilters,
    include: selectedCredentialTimelineInclude,
    throttleMs: 0,
  });
  const selectedCredentialTimelineData = selectedCredentialTimelineAnalytics.dataStale
    ? null
    : selectedCredentialTimelineAnalytics.data;
  const credentialTrendLoading = Boolean(
    activeTabState === 'credentials' &&
    selectedCredentialFilterID &&
    (selectedCredentialTimelineAnalytics.loading ||
      selectedCredentialTimelineAnalytics.dataStale ||
      (!selectedCredentialTimelineAnalytics.data && !selectedCredentialTimelineAnalytics.error))
  );
  const credentialTrendError =
    activeTabState === 'credentials' && selectedCredentialFilterID
      ? selectedCredentialTimelineAnalytics.error
      : '';
  const selectedCredentialTimeline = useMemo(
    () =>
      buildUsageCredentialTimeline(
        selectedCredentialTimelineData?.credential_timeline ?? [],
        resolvedGranularity,
        credentialDisplayContext
      ),
    [credentialDisplayContext, resolvedGranularity, selectedCredentialTimelineData]
  );
  const credentialTrendSeries = useMemo(
    () =>
      buildSelectedCredentialTrendSeries(
        selectedCredential,
        selectedCredentialTimeline,
        trendMetric
      ),
    [selectedCredential, selectedCredentialTimeline, trendMetric]
  );
  const heatmapDetail = useMemo(
    () => buildUsageHeatmapCellDetail(heatmapDetailSource, selectedHeatmapCell, heatmapMetric),
    [heatmapDetailSource, heatmapMetric, selectedHeatmapCell]
  );
  const heatmapHighlights = useMemo(
    () => buildUsageHeatmapHighlights(adapted.heatmap),
    [adapted.heatmap]
  );
  const matrix = useMemo(
    () =>
      buildUsageMatrix({
        apiKeyRows: adapted.apiKeyRows,
        credentialRows: adapted.credentialRows,
        dimension: matrixDimension,
        metric: matrixMetric,
      }),
    [adapted.apiKeyRows, adapted.credentialRows, matrixDimension, matrixMetric]
  );
  const keyAnomalies = useMemo(() => buildKeyAnomalies(adapted.apiKeyRows), [adapted.apiKeyRows]);
  const credentialAnomalies = useMemo(
    () => buildKeyAnomalies(adapted.credentialRows),
    [adapted.credentialRows]
  );
  const credentialQuotaRows = useMemo(
    () => buildCredentialQuotaRows(adapted.credentialRows, nowMs),
    [adapted.credentialRows, nowMs]
  );
  const insights = useMemo(
    () =>
      buildUsageInsights({
        apiKeyRows: adapted.apiKeyRows,
        credentialRows: adapted.credentialRows,
        modelRows: adapted.modelRows,
        providerRows: adapted.providerRows,
        summary: adapted.summary,
      }),
    [
      adapted.apiKeyRows,
      adapted.credentialRows,
      adapted.modelRows,
      adapted.providerRows,
      adapted.summary,
    ]
  );
  const setFilters = useCallback((patch: Partial<UsageAnalyticsFiltersState>) => {
    setFiltersState((current) => {
      const next = {
        ...current,
        ...patch,
        ...(patch.apiKeyHash === undefined
          ? {}
          : { apiKeyHash: getSelectableApiKeyHash(patch.apiKeyHash) || 'all' }),
      };
      writeUsageAnalyticsUiState({ filters: next });
      return next;
    });
    setSelectedBucketMs(null);
    setSelectedHeatmapDateKey(USAGE_HEATMAP_ALL_DATES_KEY);
    setSelectedHeatmapCell(null);
  }, []);

  const resetFilters = useCallback(() => {
    setFiltersState(USAGE_ANALYTICS_DEFAULT_FILTERS);
    writeUsageAnalyticsUiState({ filters: USAGE_ANALYTICS_DEFAULT_FILTERS });
    setSelectedBucketMs(null);
    setSelectedHeatmapDateKey(USAGE_HEATMAP_ALL_DATES_KEY);
    setSelectedHeatmapCell(null);
  }, []);

  const clearFilter = useCallback((key: UsageSelectedFilterKey) => {
    setFiltersState((current) => {
      const next = {
        ...current,
        [key]: 'all',
      };
      writeUsageAnalyticsUiState({ filters: next });
      return next;
    });
    setSelectedBucketMs(null);
    setSelectedHeatmapDateKey(USAGE_HEATMAP_ALL_DATES_KEY);
    setSelectedHeatmapCell(null);
  }, []);

  const selectBucket = useCallback((point: UsageTimelinePoint | null) => {
    setSelectedBucketMs(point?.bucketMs ?? null);
  }, []);

  const selectHeatmapCell = useCallback((cell: UsageHeatmapCellSelection | null) => {
    setSelectedHeatmapCell(cell);
    setSelectedHeatmapDateKey(USAGE_HEATMAP_ALL_DATES_KEY);
  }, []);

  const selectHeatmapDate = useCallback((key: string) => {
    setSelectedHeatmapDateKey(key || USAGE_HEATMAP_ALL_DATES_KEY);
  }, []);

  const selectApiKeyHash = useCallback((hash: string) => {
    setSelectedApiKeyHash(getSelectableApiKeyHash(hash));
  }, []);

  const refresh = useCallback(() => {
    // Changing the generation schedules each hook exactly once. Calling refresh()
    // as well would send both the old bounds and the new bounds on a rolling range.
    setNowMs(Date.now());
    setRefreshId((id) => id + 1);
    void loadApiKeyAliases();
    void loadMonitoringMeta();
  }, [loadApiKeyAliases, loadMonitoringMeta]);

  const exactComplete =
    !progressive ||
    (composedAnomalies
      ? basisComplete && hasRequestedPercentiles(exactData, sectionIncludes.exact.percentiles)
      : Boolean(
          exactData?.percentiles?.timeline &&
          (activeTabState !== 'overview' || exactData.percentiles.summary)
        ));
  const sections = {
    core: {
      ...analyticsSectionState(
        analytics,
        Boolean(bounds) && !availability.checking,
        Boolean(coreData?.summary)
      ),
      retry: refresh,
    },
    dimensions: analyticsSectionState(progressive ? dimensionsAnalytics : analytics, hasDimensions),
    exact: analyticsSectionState(progressive ? exactAnalytics : analytics, hasExact, exactComplete),
    anomalies: {
      ...analyticsSectionState(progressive ? exactAnalytics : analytics, hasExact, exactComplete),
      ...(composedAnomalies && hasExact
        ? {
            coreCoverage: coreData?.coverage,
            compositionSemantics: 'core_additive_exact_latency' as const,
          }
        : {}),
    },
    selectors: analyticsSectionState(
      filterSelectorsAnalytics,
      Boolean(bounds),
      Boolean(filterSelectorsData?.filter_options)
    ),
    preview: analyticsSectionState(
      previewAnalytics,
      previewEnabled,
      Boolean(previewData?.drilldown_preview)
    ),
    trends: analyticsSectionState(apiKeyTrendAnalytics, apiKeyTrendHashes.length > 0),
    apiKeyTrend: analyticsSectionState(
      selectedApiKeyTimelineAnalytics,
      activeTabState === 'apiKeys' && Boolean(selectedApiKeyFilterHash)
    ),
    credentialTrend: analyticsSectionState(
      selectedCredentialTimelineAnalytics,
      activeTabState === 'credentials' && Boolean(selectedCredentialFilterID)
    ),
    heatmap: analyticsSectionState(
      progressive ? dimensionsAnalytics : analytics,
      activeTabState === 'heatmap'
    ),
    heatmapDate: heatmapDateSection,
    metadata: {
      enabled: true,
      loading: monitoringMetaLoading,
      loaded: monitoringMetaLoaded,
      stale: monitoringMetaLoaded && Boolean(monitoringMetaError || monitoringMetaLoading),
      error: monitoringMetaError,
      retry: () => {
        void loadMonitoringMeta();
      },
    },
  };
  if (basisError) {
    sections.exact.loading = false;
    sections.anomalies.loading = false;
    sections.exact.error = 'usage_analytics.section_missing';
    sections.anomalies.error = 'usage_analytics.section_missing';
    sections.exact.retry = refresh;
    sections.anomalies.retry = refresh;
  }
  if (progressive && !sections.core.loaded && !sections.core.loading) {
    // A failed prerequisite is unavailable, not an exact request still running.
    sections.exact.loading = false;
    sections.anomalies.loading = false;
    sections.exact.retry = refresh;
    sections.anomalies.retry = refresh;
  }

  return {
    filters,
    setFilters,
    resetFilters,
    clearFilter,
    activeTab: activeTabState,
    setActiveTab,
    bounds,
    resolvedGranularity,
    progressive,
    fetchGenerationKey,
    sections,
    loading: analytics.loading || availability.checking,
    error: analytics.error,
    enabled: analytics.enabled,
    apiKeyDisplayMap,
    unavailableReason: analytics.unavailableReason,
    lastRefreshedAt: analytics.lastRefreshedAt,
    refresh,
    coverage: adapted.coverage,
    summary: adapted.summary,
    summaryDelta,
    timeline: adapted.timeline,
    modelRows: adapted.modelRows,
    apiKeyRows: adapted.apiKeyRows,
    credentialRows: adapted.credentialRows,
    allCredentialRows: adapted.credentialRows,
    providerRows: adapted.providerRows,
    heatmap: adapted.heatmap,
    heatmapMetric,
    setHeatmapMetric,
    heatmapScaleMode,
    setHeatmapScaleMode,
    heatmapDateOptions,
    selectedHeatmapDateKey: activeHeatmapDateKey,
    selectHeatmapDate,
    heatmapDateLoading: heatmapDateSection.loading,
    heatmapDateLoaded: Boolean(heatmapDateData),
    heatmapDateError: selectedHeatmapDate ? heatmapDateAnalytics.error : '',
    selectedHeatmapCell,
    selectHeatmapCell,
    heatmapDetail,
    heatmapHighlights,
    browserTimeZone,
    matrix,
    matrixDimension,
    setMatrixDimension,
    matrixMetric,
    setMatrixMetric,
    trendMetric,
    setTrendMetric,
    modelTrendSeries,
    apiKeyTrendSeries,
    selectedApiKeyTrendSeries,
    credentialTrendSeries,
    credentialTrendLoading,
    credentialTrendLoaded: Boolean(selectedCredentialTimelineData),
    credentialTrendError,
    keyAnomalies,
    credentialAnomalies,
    credentialQuotaRows,
    insights,
    anomalyPoints: adapted.anomalyPoints,
    drilldownPreview: adapted.drilldownPreview,
    filterOptions: filterSelectorsData?.filter_options ?? adapted.filterOptions,
    selectedBucket,
    selectBucket,
    anomalyAnalysis,
    selectedModel,
    setSelectedModelId,
    selectedApiKey,
    setSelectedApiKeyHash: selectApiKeyHash,
    selectedCredential,
    setSelectedCredentialId,
  };
}
