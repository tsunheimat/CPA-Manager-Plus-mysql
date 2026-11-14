import { useEffect } from 'react';
import { act, create, type ReactTestRenderer } from 'react-test-renderer';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useApiKeyAliases } from './useApiKeyAliases';

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
  prices: vi.fn(),
  save: vi.fn(),
  notify: vi.fn(),
  auth: { apiBase: 'panel-a', managementKey: 'key-a' },
  availability: { modelPricesAvailable: true, managerServiceBase: 'manager-a' },
}));
vi.mock('@/stores', () => ({
  useAuthStore: (select: (state: typeof mocks.auth) => unknown) => select(mocks.auth),
}));
vi.mock('@/hooks/usePanelFeatureAvailability', () => ({
  usePanelFeatureAvailability: () => mocks.availability,
}));
vi.mock('@/services/api/usageService', () => ({
  usageServiceApi: {
    getApiKeyAliases: mocks.get,
    getModelPrices: mocks.prices,
    saveModelPrices: mocks.save,
  },
}));
vi.mock('@/i18n', () => ({ default: { t: (key: string) => key } }));
vi.mock('@/stores/useNotificationStore', () => ({
  useNotificationStore: { getState: () => ({ showNotification: mocks.notify }) },
}));
(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
const item = { apiKeyHash: 'hash', alias: 'alias' };

describe('alias-only loading', () => {
  let renderer: ReactTestRenderer | undefined;
  let latest: ReturnType<typeof useApiKeyAliases>;
  function Harness() {
    const result = useApiKeyAliases();
    useEffect(() => {
      latest = result;
    }, [result]);
    return null;
  }
  const render = () =>
    act(async () => {
      renderer = create(<Harness />);
    });
  beforeEach(() => {
    vi.resetAllMocks();
    mocks.auth = { apiBase: 'panel-a', managementKey: 'key-a' };
    mocks.availability = { modelPricesAvailable: true, managerServiceBase: 'manager-a' };
    mocks.get.mockResolvedValue({ items: [item] });
  });
  afterEach(() => {
    act(() => renderer?.unmount());
    renderer = undefined;
  });

  it('auto-loads once, never loads prices, keeps last good aliases on refresh error', async () => {
    await render();
    expect(mocks.get).toHaveBeenCalledTimes(1);
    expect(latest.apiKeyAliases).toEqual([item]);
    mocks.get.mockRejectedValueOnce(new Error('offline'));
    await act(async () => {
      await latest.loadApiKeyAliases();
    });
    expect(latest.apiKeyAliases).toEqual([item]);
    expect(mocks.notify).toHaveBeenCalledWith('notification.refresh_failed', 'error');
    expect(mocks.prices).not.toHaveBeenCalled();
    expect(mocks.save).not.toHaveBeenCalled();
  });

  it.each(['auth', 'base', 'service', 'availability'])(
    'resets immediately and fences stale responses and old callbacks on %s changes',
    async (change) => {
      await render();
      const oldLoad = latest.loadApiKeyAliases;
      let resolve!: (value: { items: (typeof item)[] }) => void;
      mocks.get.mockReturnValueOnce(
        new Promise((yes) => {
          resolve = yes;
        })
      );
      let pending!: Promise<void>;
      act(() => {
        pending = latest.loadApiKeyAliases();
      });
      if (change === 'auth') mocks.auth.managementKey = 'key-b';
      if (change === 'base') mocks.auth.apiBase = 'panel-b';
      if (change === 'service') mocks.availability.managerServiceBase = 'manager-b';
      if (change === 'availability') mocks.availability.modelPricesAvailable = false;
      mocks.get.mockReturnValueOnce(new Promise(() => {}));
      await act(async () => {
        renderer!.update(<Harness />);
      });
      expect(latest.apiKeyAliases).toEqual([]);
      await act(async () => {
        resolve({ items: [item] });
        await pending;
      });
      expect(latest.apiKeyAliases).toEqual([]);
      const calls = mocks.get.mock.calls.length;
      await act(async () => {
        await oldLoad();
      });
      expect(mocks.get).toHaveBeenCalledTimes(calls);
      expect(mocks.notify).not.toHaveBeenCalled();
    }
  );

  it('rejects superseded loads and unmounted errors without notifications', async () => {
    await render();
    let resolve!: (value: { items: (typeof item)[] }) => void;
    mocks.get.mockReturnValueOnce(
      new Promise((yes) => {
        resolve = yes;
      })
    );
    let pending!: Promise<void>;
    act(() => {
      pending = latest.loadApiKeyAliases();
    });
    mocks.get.mockResolvedValueOnce({ items: [] });
    await act(async () => {
      await latest.loadApiKeyAliases();
      resolve({ items: [item] });
      await pending;
    });
    expect(latest.apiKeyAliases).toEqual([]);
    let reject!: (error: Error) => void;
    mocks.get.mockReturnValueOnce(
      new Promise((_yes, no) => {
        reject = no;
      })
    );
    act(() => {
      pending = latest.loadApiKeyAliases();
    });
    act(() => renderer!.unmount());
    renderer = undefined;
    await act(async () => {
      reject(new Error('offline'));
      await pending;
    });
    expect(mocks.notify).not.toHaveBeenCalled();
  });

  it('keeps aliases cleared across A → B → A until the newest A request completes', async () => {
    await render();
    expect(latest.apiKeyAliases).toEqual([item]);
    const responses: Array<(response: { items: (typeof item)[] }) => void> = [];
    mocks.get.mockImplementation(
      () =>
        new Promise((resolve) => {
          responses.push(resolve);
        })
    );
    let oldRefresh!: Promise<void>;
    act(() => {
      oldRefresh = latest.loadApiKeyAliases();
    });
    mocks.auth.managementKey = 'key-b';
    await act(async () => {
      renderer!.update(<Harness />);
    });
    expect(latest.apiKeyAliases).toEqual([]);
    mocks.auth.managementKey = 'key-a';
    await act(async () => {
      renderer!.update(<Harness />);
    });
    expect(mocks.get).toHaveBeenCalledTimes(4);
    expect(mocks.get).toHaveBeenLastCalledWith('manager-a', 'key-a');
    expect(latest.apiKeyAliases).toEqual([]);
    await act(async () => {
      responses[0]({ items: [{ ...item, alias: 'stale A' }] });
      responses[1]({ items: [{ ...item, alias: 'stale B' }] });
      await oldRefresh;
    });
    expect(latest.apiKeyAliases).toEqual([]);
    const current = { ...item, alias: 'current A' };
    await act(async () => {
      responses[2]({ items: [current] });
    });
    expect(latest.apiKeyAliases).toEqual([current]);
    expect(mocks.notify).not.toHaveBeenCalled();
  });

  it('does not call a service while unavailable and loads when available', async () => {
    mocks.availability.modelPricesAvailable = false;
    await render();
    expect(mocks.get).not.toHaveBeenCalled();
    expect(latest.apiKeyAliases).toEqual([]);
    mocks.availability.modelPricesAvailable = true;
    await act(async () => {
      renderer!.update(<Harness />);
    });
    expect(mocks.get).toHaveBeenCalledTimes(1);
  });
});
