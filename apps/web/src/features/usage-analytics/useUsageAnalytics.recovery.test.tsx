import { useEffect } from 'react';
import { MemoryRouter } from 'react-router-dom';
import { act, create, type ReactTestRenderer } from 'react-test-renderer';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { getDemoMonitoringAnalytics } from '@/features/demo/demoFixtures';
import type {
  MonitoringAnalyticsRequest,
  MonitoringAnalyticsResponse,
} from '@/services/api/usageService';
import type { Config } from '@/types/config';
import { useUsageAnalytics } from './useUsageAnalytics';

const mocks = vi.hoisted(() => ({
  getAnalytics: vi.fn(),
  authFiles: vi.fn(),
  channels: vi.fn(),
  aliases: vi.fn(),
  config: null as Config | null,
  auth: { apiBase: 'https://panel.test', managementKey: 'test' },
}));
vi.mock('@/services/api/usageService', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/services/api/usageService')>()),
  monitoringAnalyticsApi: { getAnalytics: mocks.getAnalytics },
}));
vi.mock('@/services/api/authFiles', () => ({ authFilesApi: { list: mocks.authFiles } }));
vi.mock('@/services/api/client', () => ({ apiClient: { get: mocks.channels } }));
vi.mock('@/hooks/usePanelFeatureAvailability', () => ({
  usePanelFeatureAvailability: () => ({ analyticsSectionsVersion: 1, checking: false }),
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
vi.mock('@/stores', () => ({
  useConfigStore: (select: (state: { config: Config | null }) => unknown) =>
    select({ config: mocks.config }),
  useAuthStore: (select: (state: typeof mocks.auth) => unknown) => select(mocks.auth),
}));
(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const configuredChannel = {
  name: 'Example channel',
  baseUrl: 'https://example.invalid',
  apiKeyEntries: [
    { apiKey: 'fixture-one', authIndex: 'idx-one' },
    { apiKey: 'fixture-two', authIndex: 'idx-two' },
  ],
};
const endpointChannel = {
  name: configuredChannel.name,
  'base-url': configuredChannel.baseUrl,
  'api-key-entries': configuredChannel.apiKeyEntries,
};
const analyticsResponse = (request: MonitoringAnalyticsRequest): MonitoringAnalyticsResponse => {
  const response = getDemoMonitoringAnalytics(request);
  if (request.include?.credential_stats) {
    response.credential_stats = [
      {
        id: 'credential-one',
        auth_index: 'idx-one',
        auth_label_snapshot: 'Legacy display label',
        auth_provider_snapshot: 'openai',
        calls: 10,
        success_calls: 10,
        failure_calls: 0,
        success_rate: 1,
        input_tokens: 70,
        output_tokens: 30,
        cached_tokens: 0,
        cache_read_tokens: 0,
        cache_creation_tokens: 0,
        total_tokens: 100,
        cost: 1,
        average_latency_ms: 100,
        last_seen_ms: request.to_ms - 1,
      },
    ];
  }
  return response;
};

// Keep the real request hooks, metadata service/normalizer, and display adapters.
// Only transport and connection/config stores are replaced.
describe('usage analytics section recovery', () => {
  let renderer: ReactTestRenderer | undefined;
  let latest: ReturnType<typeof useUsageAnalytics>;
  function Harness() {
    const result = useUsageAnalytics();
    useEffect(() => {
      latest = result;
    }, [result]);
    return null;
  }
  const tree = (query = 'tab=credentials') => (
    <MemoryRouter initialEntries={[`/usage-analytics?${query}`]}>
      <Harness />
    </MemoryRouter>
  );
  const mount = async (query?: string) => {
    await act(async () => {
      renderer = create(tree(query));
    });
  };
  beforeEach(() => {
    vi.stubEnv('TZ', 'UTC');
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date('2026-11-06T12:00:00Z'));
    mocks.config = { openaiCompatibility: [configuredChannel] };
    mocks.auth.managementKey = 'test';
    mocks.authFiles.mockReset().mockResolvedValue({ files: [] });
    mocks.channels.mockReset().mockResolvedValue([]);
    mocks.aliases.mockReset();
    mocks.getAnalytics
      .mockReset()
      .mockImplementation(
        async (_base: string, _key: string, request: MonitoringAnalyticsRequest) =>
          analyticsResponse(request)
      );
  });
  afterEach(async () => {
    await act(async () => {
      renderer?.unmount();
    });
    renderer = undefined;
    vi.useRealTimers();
    vi.unstubAllEnvs();
  });

  it('ends a failed heatmap-date refresh in error and recovers only on explicit retry', async () => {
    await mount('tab=heatmap&time_range=7d');
    await act(async () => {
      latest.selectHeatmapCell({ weekday: 1, hour: 8 });
    });
    expect(latest.heatmapDateOptions.length).toBeGreaterThan(0);
    await act(async () => {
      latest.selectHeatmapDate(latest.heatmapDateOptions[0].key);
    });
    expect(latest.heatmapDateLoaded).toBe(true);
    expect(latest.heatmapDetail).not.toBeNull();
    const coreCalls = () =>
      mocks.getAnalytics.mock.calls.filter(([, , request]) => request.include?.summary).length;
    const originalCoreCalls = coreCalls();

    let failRefresh!: (error: Error) => void;
    mocks.getAnalytics.mockImplementationOnce(
      () =>
        new Promise<MonitoringAnalyticsResponse>((_resolve, reject) => {
          failRefresh = reject;
        })
    );
    await act(async () => {
      latest.sections.heatmapDate.retry();
    });
    expect(latest.heatmapDateLoading).toBe(true);
    expect(latest.heatmapDateLoaded).toBe(false);
    expect(latest.heatmapDetail).toBeNull();
    await act(async () => {
      failRefresh(new Error('date request failed'));
    });

    expect(latest.sections.heatmapDate).toMatchObject({
      loading: false,
      loaded: false,
      error: 'date request failed',
      stale: true,
    });
    expect(latest.heatmapDateLoading).toBe(false);
    expect(latest.heatmapDateLoaded).toBe(false);
    expect(latest.heatmapDateError).toBe('date request failed');
    expect(latest.heatmapDetail).toBeNull();
    const callsAfterFailure = mocks.getAnalytics.mock.calls.length;
    await act(async () => {
      renderer!.update(tree('tab=heatmap&time_range=7d'));
    });
    expect(mocks.getAnalytics).toHaveBeenCalledTimes(callsAfterFailure);

    await act(async () => {
      latest.sections.heatmapDate.retry();
    });
    expect(latest.heatmapDateLoading).toBe(false);
    expect(latest.heatmapDateLoaded).toBe(true);
    expect(latest.heatmapDateError).toBe('');
    expect(latest.heatmapDetail).not.toBeNull();
    expect(coreCalls()).toBe(originalCoreCalls);
  });

  it.each([false, true])(
    'uses normalized configuration channels on initial endpoint failure (auth files fail=%s)',
    async (authFilesFail) => {
      mocks.channels.mockRejectedValue(new Error('channel endpoint unavailable'));
      if (authFilesFail) mocks.authFiles.mockRejectedValue(new Error('auth files unavailable'));
      await mount();
      expect(mocks.channels).toHaveBeenCalledWith('/openai-compatibility');
      expect(latest.credentialRows[0].label).toBe('Example channel #1');
      expect(latest.sections.metadata).toMatchObject({ loading: false, loaded: !authFilesFail });
      expect(latest.sections.metadata.error).toContain('channel endpoint unavailable');
      expect(latest.error).toBe('');

      mocks.authFiles.mockResolvedValue({ files: [] });
      mocks.channels.mockResolvedValue([endpointChannel]);
      await act(async () => {
        latest.sections.metadata.retry();
      });
      expect(latest.credentialRows[0].label).toBe('Example channel #1');
      expect(latest.sections.metadata).toMatchObject({ loaded: true, stale: false, error: '' });
    }
  );

  it.each([
    ['empty', []],
    ['nonempty', [{ ...endpointChannel, 'api-key-entries': [{ authIndex: 'another-index' }] }]],
  ])(
    'preserves a successful %s channel snapshot on later failure instead of installing fallback',
    async (_kind, channels) => {
      mocks.channels.mockResolvedValue(channels);
      await mount();
      expect(latest.credentialRows[0].label).toBe('Legacy display label');
      mocks.channels.mockRejectedValue(new Error('channel endpoint unavailable'));
      await act(async () => {
        latest.sections.metadata.retry();
      });
      expect(latest.credentialRows[0].label).toBe('Legacy display label');
      expect(latest.sections.metadata).toMatchObject({
        loaded: true,
        stale: true,
        error: 'channel endpoint unavailable',
      });

      mocks.channels.mockResolvedValue([endpointChannel]);
      await act(async () => {
        latest.sections.metadata.retry();
      });
      expect(latest.credentialRows[0].label).toBe('Example channel #1');
    }
  );

  it('keeps last-good channel identities absent from configuration until a successful replacement', async () => {
    mocks.channels.mockResolvedValue([
      { ...endpointChannel, 'api-key-entries': [{ authIndex: 'live-only-index' }] },
    ]);
    mocks.getAnalytics.mockImplementation(
      async (_base: string, _key: string, request: MonitoringAnalyticsRequest) => {
        const response = analyticsResponse(request);
        if (response.credential_stats) {
          response.credential_stats[0].auth_index = 'live-only-index';
          response.credential_stats[0].source = configuredChannel.apiKeyEntries[0].apiKey;
        }
        return response;
      }
    );
    await mount();
    expect(latest.credentialRows[0].label).toBe('Example channel #1');

    mocks.channels.mockRejectedValue(new Error('channel endpoint unavailable'));
    await act(async () => {
      latest.sections.metadata.retry();
    });
    expect(latest.credentialRows[0].label).toBe('Example channel #1');
    expect(latest.sections.metadata.error).toBe('channel endpoint unavailable');

    mocks.channels.mockResolvedValue([]);
    await act(async () => {
      latest.sections.metadata.retry();
    });
    expect(latest.credentialRows[0].label).toBe('Legacy display label');
    expect(latest.sections.metadata).toMatchObject({ loaded: true, stale: false, error: '' });
  });

  it('preserves last-good channels across config refresh but resets them for a new connection', async () => {
    await mount(); // A successful empty response is authoritative, not missing metadata.
    expect(latest.credentialRows[0].label).toBe('Legacy display label');
    mocks.channels.mockRejectedValue(new Error('channel endpoint unavailable'));
    mocks.config = { ...mocks.config };
    await act(async () => {
      renderer!.update(tree());
    });
    expect(latest.credentialRows[0].label).toBe('Legacy display label');
    expect(latest.sections.metadata).toMatchObject({
      loaded: true,
      stale: true,
      error: 'channel endpoint unavailable',
    });

    mocks.auth.managementKey = 'new-session';
    await act(async () => {
      renderer!.update(tree());
    });
    expect(latest.credentialRows[0].label).toBe('Example channel #1');
  });
});
