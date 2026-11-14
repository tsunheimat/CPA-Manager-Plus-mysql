import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import i18n from '@/i18n';
import { usePanelFeatureAvailability } from '@/hooks/usePanelFeatureAvailability';
import { usageServiceApi, type ApiKeyAlias } from '@/services/api/usageService';
import { useAuthStore } from '@/stores';
import { useNotificationStore } from '@/stores/useNotificationStore';

/** Alias loading has no price-book or browser price-migration side effects. */
export function useApiKeyAliases() {
  const apiBase = useAuthStore((state) => state.apiBase);
  const managementKey = useAuthStore((state) => state.managementKey);
  const availability = usePanelFeatureAvailability();
  // Preserve the existing availability contract; aliases share this service.
  const serviceBase = availability.modelPricesAvailable ? availability.managerServiceBase : '';
  const scope = JSON.stringify([apiBase, managementKey, serviceBase]);
  const scopeRef = useRef(scope);
  const requestIdRef = useRef(0);
  const mountedRef = useRef(false);
  const [state, setState] = useState<{ scope: string; items: ApiKeyAlias[] }>({ scope, items: [] });
  // Reset scoped state during render so even A → B → A cannot reveal A's old
  // aliases while the new A request is pending. This converges in one render.
  if (state.scope !== scope) setState({ scope, items: [] });

  useLayoutEffect(() => {
    scopeRef.current = scope;
    mountedRef.current = true;
    requestIdRef.current += 1;
    return () => {
      mountedRef.current = false;
      requestIdRef.current += 1;
    };
  }, [scope]);

  const loadApiKeyAliases = useCallback(async () => {
    if (!serviceBase || !mountedRef.current || scopeRef.current !== scope) return;
    const requestId = ++requestIdRef.current;
    const isCurrent = () =>
      mountedRef.current && scopeRef.current === scope && requestIdRef.current === requestId;
    try {
      const response = await usageServiceApi.getApiKeyAliases(serviceBase, managementKey);
      if (isCurrent())
        setState({ scope, items: Array.isArray(response.items) ? response.items : [] });
    } catch {
      if (isCurrent()) {
        useNotificationStore
          .getState()
          .showNotification(i18n.t('notification.refresh_failed'), 'error');
      }
    }
  }, [managementKey, scope, serviceBase]);

  useEffect(() => {
    // The setter runs only after the awaited service response, never synchronously.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    void loadApiKeyAliases();
  }, [loadApiKeyAliases]);

  return { apiKeyAliases: state.scope === scope ? state.items : [], loadApiKeyAliases };
}
