import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import {
  useRequestMonitoringAvailability,
  type RequestMonitoringUnavailableReason,
} from '@/hooks/useRequestMonitoringAvailability';
import {
  monitoringAnalyticsApi,
  type MonitoringAnalyticsEventsPageRequest,
  type MonitoringAnalyticsFilters,
  type MonitoringAnalyticsInclude,
  type MonitoringAnalyticsRequest,
  type MonitoringAnalyticsResponse,
} from '@/services/api/usageService';
import { useAuthStore } from '@/stores';

const DEFAULT_REFRESH_THROTTLE_MS = 5_000;

export interface MonitoringAnalyticsScheduler {
  run<T>(work: () => Promise<T>, signal: AbortSignal, priority?: number): Promise<T>;
}

/** Page-local admission, not a result cache. Aborted running work holds its slot until settled. */
export function createMonitoringAnalyticsScheduler(limit = 2): MonitoringAnalyticsScheduler {
  let active = 0;
  const queue: Array<{ priority: number; start: () => void; signal: AbortSignal }> = [];
  const pump = () => {
    queue.sort((a, b) => a.priority - b.priority);
    while (active < Math.max(1, limit) && queue.length) {
      const next = queue.shift()!;
      if (!next.signal.aborted) next.start();
    }
  };
  return {
    run: <T>(work: () => Promise<T>, signal: AbortSignal, priority = 2) =>
      new Promise<T>((resolve, reject) => {
        const abort = () => {
          const index = queue.indexOf(entry);
          if (index >= 0) queue.splice(index, 1);
          reject(new DOMException('Analytics request canceled', 'AbortError'));
        };
        const entry = {
          priority,
          signal,
          start: () => {
            signal.removeEventListener('abort', abort);
            active += 1;
            Promise.resolve()
              .then(() => {
                if (signal.aborted)
                  throw new DOMException('Analytics request canceled', 'AbortError');
                return work();
              })
              .then(resolve, reject)
              .finally(() => {
                active -= 1;
                pump();
              });
          },
        };
        if (signal.aborted) {
          abort();
          return;
        }
        signal.addEventListener('abort', abort, { once: true });
        queue.push(entry);
        queueMicrotask(pump);
      }),
  };
}

export interface UseMonitoringAnalyticsParams {
  fromMs?: number | null;
  toMs?: number | null;
  nowMs?: number;
  dataScopeKey?: string;
  /** Opt-in exact immutable identity, separate from a rounded presentation scope. */
  fetchGenerationKey?: string;
  timeZone?: string;
  scheduler?: MonitoringAnalyticsScheduler;
  priority?: number;
  searchQuery?: string;
  searchApiKeyHash?: string;
  filters?: MonitoringAnalyticsFilters;
  include?: MonitoringAnalyticsInclude;
  eventsPage?: MonitoringAnalyticsEventsPageRequest | null;
  throttleMs?: number;
  /** Defer a section until its dependencies and capability probe are ready. */
  enabled?: boolean;
}

export interface MonitoringAnalyticsRefreshOptions {
  force?: boolean;
}

export interface UseMonitoringAnalyticsReturn {
  enabled: boolean;
  loading: boolean;
  error: string;
  data: MonitoringAnalyticsResponse | null;
  dataStale: boolean;
  lastRefreshedAt: Date | null;
  serviceBase: string;
  unavailableReason: RequestMonitoringUnavailableReason | '';
  refresh: (options?: MonitoringAnalyticsRefreshOptions) => Promise<void>;
}

const isFiniteTimestamp = (value: number | null | undefined): value is number =>
  typeof value === 'number' && Number.isFinite(value);

const stableJson = (value: unknown) => JSON.stringify(value ?? {});

const parseJson = <T>(value: string): T => JSON.parse(value) as T;

const getBrowserTimeZone = () => {
  if (typeof Intl === 'undefined') return '';
  return Intl.DateTimeFormat().resolvedOptions().timeZone || '';
};

const buildInFlightRequestIdentityKey = (
  dataScopeKey: string | undefined,
  request: MonitoringAnalyticsRequest,
  requestKey: string
) => {
  if (!dataScopeKey) {
    return requestKey;
  }

  const eventsPage = request.include?.events_page;
  return stableJson({
    dataScopeKey,
    eventsPage: eventsPage
      ? {
          limit: eventsPage.limit ?? null,
          before_ms: eventsPage.before_ms ?? null,
          before_id: eventsPage.before_id ?? null,
        }
      : null,
  });
};

export function useMonitoringAnalytics({
  fromMs,
  toMs,
  nowMs,
  dataScopeKey,
  fetchGenerationKey,
  timeZone: requestedTimeZone,
  scheduler,
  priority,
  searchQuery,
  searchApiKeyHash,
  filters,
  include,
  eventsPage,
  throttleMs = DEFAULT_REFRESH_THROTTLE_MS,
  enabled: requestedEnabled = true,
}: UseMonitoringAnalyticsParams): UseMonitoringAnalyticsReturn {
  const managementKey = useAuthStore((state) => state.managementKey);
  const apiBase = useAuthStore((state) => state.apiBase);
  const availability = useRequestMonitoringAvailability();
  const [data, setData] = useState<MonitoringAnalyticsResponse | null>(null);
  const [dataScopeStateKey, setDataScopeStateKey] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [lastRefreshedAt, setLastRefreshedAt] = useState<Date | null>(null);
  const requestIdRef = useRef(0);
  const lastStartedAtRef = useRef(0);
  const lastRequestKeyRef = useRef('');
  const inFlightRequestIdentityKeyRef = useRef('');
  const inFlightRequestIdRef = useRef(0);
  const abortControllerRef = useRef<AbortController | null>(null);

  const filtersKey = useMemo(() => stableJson(filters), [filters]);
  const includeKey = useMemo(() => stableJson(include), [include]);
  const eventsPageKey = useMemo(() => JSON.stringify(eventsPage ?? null), [eventsPage]);

  const request = useMemo<MonitoringAnalyticsRequest | null>(() => {
    if (!isFiniteTimestamp(fromMs) || !isFiniteTimestamp(toMs) || fromMs <= 0 || fromMs >= toMs) {
      return null;
    }

    const nextFilters = parseJson<MonitoringAnalyticsFilters>(filtersKey);
    const nextInclude = parseJson<MonitoringAnalyticsInclude>(includeKey);
    const nextEventsPage = parseJson<MonitoringAnalyticsEventsPageRequest | null>(eventsPageKey);
    if (nextEventsPage) {
      nextInclude.events_page = nextEventsPage;
    }

    const payload: MonitoringAnalyticsRequest = {
      from_ms: fromMs,
      to_ms: toMs,
    };
    const timeZone = requestedTimeZone ?? getBrowserTimeZone();
    if (timeZone) {
      payload.time_zone = timeZone;
    }
    if (isFiniteTimestamp(nowMs)) {
      payload.now_ms = nowMs;
    }

    const normalizedSearchQuery = searchQuery?.trim();
    if (normalizedSearchQuery) {
      payload.search_query = normalizedSearchQuery;
    }

    const normalizedApiKeyHash = searchApiKeyHash?.trim();
    if (normalizedApiKeyHash) {
      payload.search_api_key_hash = normalizedApiKeyHash;
    }

    if (Object.keys(nextFilters).length > 0) {
      payload.filters = nextFilters;
    }
    if (Object.keys(nextInclude).length > 0) {
      payload.include = nextInclude;
    }
    return payload;
  }, [
    eventsPageKey,
    filtersKey,
    fromMs,
    includeKey,
    nowMs,
    requestedTimeZone,
    searchApiKeyHash,
    searchQuery,
    toMs,
  ]);

  const requestKey = useMemo(() => (request ? stableJson(request) : ''), [request]);
  const exactRequestKey =
    fetchGenerationKey === undefined
      ? ''
      : stableJson([fetchGenerationKey, requestKey, requestedEnabled]);
  const inFlightRequestIdentityKey = useMemo(
    () =>
      request
        ? exactRequestKey || buildInFlightRequestIdentityKey(dataScopeKey, request, requestKey)
        : '',
    [dataScopeKey, exactRequestKey, request, requestKey]
  );
  const activeDataScopeKey = exactRequestKey || dataScopeKey || requestKey;
  const serviceBase = availability.serviceBase;
  const enabled =
    requestedEnabled && availability.available && Boolean(serviceBase) && Boolean(request);
  const connectionScope = JSON.stringify([apiBase, serviceBase, managementKey]);
  const connectionScopeRef = useRef(connectionScope);
  connectionScopeRef.current = connectionScope;
  const exactRequestKeyRef = useRef(exactRequestKey);
  exactRequestKeyRef.current = exactRequestKey;
  useLayoutEffect(() => {
    if (!exactRequestKey) return;
    requestIdRef.current += 1;
    abortControllerRef.current?.abort();
    inFlightRequestIdentityKeyRef.current = '';
    inFlightRequestIdRef.current = 0;
    lastRequestKeyRef.current = '';
    setLoading(false);
    setError('');
  }, [exactRequestKey]);
  useLayoutEffect(() => {
    requestIdRef.current += 1;
    abortControllerRef.current?.abort();
    inFlightRequestIdentityKeyRef.current = '';
    inFlightRequestIdRef.current = 0;
    lastRequestKeyRef.current = '';
    lastStartedAtRef.current = 0;
    setData(null);
    setDataScopeStateKey('');
    setLastRefreshedAt(null);
    setError('');
    return () => {
      requestIdRef.current += 1;
    };
  }, [connectionScope]);

  const refresh = useCallback(
    async (options: MonitoringAnalyticsRefreshOptions = {}) => {
      if (
        connectionScopeRef.current !== connectionScope ||
        exactRequestKeyRef.current !== exactRequestKey
      )
        return;
      if (!enabled || !request || !serviceBase) {
        abortControllerRef.current?.abort();
        abortControllerRef.current = null;
        requestIdRef.current += 1;
        inFlightRequestIdentityKeyRef.current = '';
        inFlightRequestIdRef.current = 0;
        setData(null);
        setDataScopeStateKey('');
        setLastRefreshedAt(null);
        setLoading(false);
        return;
      }

      if (inFlightRequestIdentityKeyRef.current === inFlightRequestIdentityKey) {
        return;
      }

      const startedAt = Date.now();
      const isSameRequest = lastRequestKeyRef.current === requestKey;
      if (
        !options.force &&
        isSameRequest &&
        startedAt - lastStartedAtRef.current < Math.max(0, throttleMs)
      ) {
        return;
      }

      const requestId = requestIdRef.current + 1;
      abortControllerRef.current?.abort();
      const controller = new AbortController();
      abortControllerRef.current = controller;
      requestIdRef.current = requestId;
      lastStartedAtRef.current = startedAt;
      lastRequestKeyRef.current = requestKey;
      inFlightRequestIdentityKeyRef.current = inFlightRequestIdentityKey;
      inFlightRequestIdRef.current = requestId;
      setLoading(true);
      setError('');

      try {
        const work = () =>
          monitoringAnalyticsApi.getAnalytics(
            serviceBase,
            managementKey,
            request,
            controller.signal
          );
        const response = await (scheduler
          ? scheduler.run(work, controller.signal, priority)
          : work());
        if (
          requestIdRef.current !== requestId ||
          connectionScopeRef.current !== connectionScope ||
          exactRequestKeyRef.current !== exactRequestKey
        )
          return;
        setData(response);
        setDataScopeStateKey(activeDataScopeKey);
        setLastRefreshedAt(new Date());
      } catch (err) {
        if (controller.signal.aborted) return;
        if (
          requestIdRef.current !== requestId ||
          connectionScopeRef.current !== connectionScope ||
          exactRequestKeyRef.current !== exactRequestKey
        )
          return;
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (inFlightRequestIdRef.current === requestId) {
          inFlightRequestIdentityKeyRef.current = '';
          inFlightRequestIdRef.current = 0;
        }
        if (abortControllerRef.current === controller) {
          abortControllerRef.current = null;
        }
        if (requestIdRef.current === requestId) {
          setLoading(false);
        }
      }
    },
    [
      activeDataScopeKey,
      connectionScope,
      enabled,
      exactRequestKey,
      scheduler,
      priority,
      inFlightRequestIdentityKey,
      managementKey,
      request,
      requestKey,
      serviceBase,
      throttleMs,
    ]
  );

  useEffect(() => {
    if (availability.checking) {
      return;
    }
    void refresh({ force: true });
  }, [availability.checking, refresh]);

  useEffect(
    () => () => {
      abortControllerRef.current?.abort();
      abortControllerRef.current = null;
    },
    []
  );

  const dataStale = Boolean(
    (dataScopeKey || exactRequestKey) &&
    data &&
    (dataScopeStateKey !== activeDataScopeKey || (exactRequestKey && (loading || error)))
  );
  const scopedData = dataScopeKey || dataScopeStateKey === activeDataScopeKey ? data : null;

  return useMemo(
    () => ({
      enabled,
      loading: availability.checking || loading,
      error,
      data: scopedData,
      dataStale,
      lastRefreshedAt,
      serviceBase,
      unavailableReason: availability.reason,
      refresh,
    }),
    [
      availability.checking,
      availability.reason,
      enabled,
      error,
      dataStale,
      lastRefreshedAt,
      loading,
      refresh,
      scopedData,
      serviceBase,
    ]
  );
}
