import { useEffect, useState } from 'react';
import InvestigateView from '../components/InvestigateView';
import ManageContextsView from '../components/ManageContextsView';
import type { IncidentContext, IncidentContextUpsertPayload } from '../types/incidentContext';
import {
  bootstrapAuthSession,
  isAuthenticated,
  signInWithHostedUi,
  signOutFromHostedUi,
} from './auth';
import {
  createIncidentContext,
  deleteIncidentContext,
  getIncidentContext,
  listIncidentContexts,
  updateIncidentContext,
} from './incidentContextsApi';
import { buttonStyle } from './ui';
import { useInvestigationPoller } from './useInvestigationPoller';

const API_URL = import.meta.env.VITE_API_URL as string | undefined;
const API_CONFIG_ERROR =
  'VITE_API_URL is not set. Configure frontend/.env with VITE_API_URL=<your API base URL>.';

if (!API_URL) {
  console.error(API_CONFIG_ERROR);
}

const AUTH_BOOTSTRAP_TIMEOUT_MS = 10_000;

type ActiveView = 'contexts' | 'investigate';

const PAGE_STYLE = {
  maxWidth: 940,
  margin: '0 auto',
  padding: '24px 20px',
  fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
  color: '#1a2438',
} as const;

export default function App() {
  const [authReady, setAuthReady] = useState(false);
  const [signedIn, setSignedIn] = useState(false);
  const [authError, setAuthError] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<ActiveView>('contexts');
  const [contexts, setContexts] = useState<IncidentContext[]>([]);
  const [contextsLoading, setContextsLoading] = useState(false);
  const [contextsError, setContextsError] = useState<string | null>(null);
  const [selectedContext, setSelectedContext] = useState<IncidentContext | null>(null);
  const [contextSubmitting, setContextSubmitting] = useState(false);

  const { state, lastTriggerValues, onStarted, onReset } = useInvestigationPoller(API_URL ?? '');

  useEffect(() => {
    if (!signedIn || !API_URL) return;
    void refreshContexts();
  }, [signedIn]);

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const timeout = new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error('Authentication service timed out.')), AUTH_BOOTSTRAP_TIMEOUT_MS),
        );
        await Promise.race([bootstrapAuthSession(), timeout]);
        const ok = await Promise.race([isAuthenticated(), timeout]);
        if (!mounted) return;
        setSignedIn(ok);
        setAuthError(null);
      } catch (err) {
        if (!mounted) return;
        setSignedIn(false);
        setAuthError(String(err));
      } finally {
        if (mounted) setAuthReady(true);
      }
    })();

    return () => {
      mounted = false;
    };
  }, []);

  async function refreshContexts() {
    if (!API_URL) return;
    setContextsLoading(true);
    setContextsError(null);
    try {
      const items = await listIncidentContexts(API_URL);
      setContexts(items);
      if (items.length === 0) {
        setSelectedContext(null);
        return;
      }
      if (!selectedContext || !items.some((item) => item.contextId === selectedContext.contextId)) {
        const first = items[0];
        const full = await getIncidentContext(API_URL, first.contextId);
        setSelectedContext(full);
      }
    } catch (err) {
      setContextsError(String(err));
    } finally {
      setContextsLoading(false);
    }
  }

  async function handleSelectContext(contextId: string) {
    if (!API_URL) return;
    setContextsError(null);
    try {
      const full = await getIncidentContext(API_URL, contextId);
      setSelectedContext(full);
    } catch (err) {
      setContextsError(String(err));
    }
  }

  async function handleCreateContext(payload: IncidentContextUpsertPayload) {
    if (!API_URL) return;
    setContextSubmitting(true);
    setContextsError(null);
    try {
      const created = await createIncidentContext(API_URL, payload);
      await refreshContexts();
      setSelectedContext(created);
    } catch (err) {
      setContextsError(String(err));
      throw err;
    } finally {
      setContextSubmitting(false);
    }
  }

  async function handleUpdateContext(payload: IncidentContextUpsertPayload) {
    if (!API_URL || !selectedContext) return;
    setContextSubmitting(true);
    setContextsError(null);
    try {
      const updated = await updateIncidentContext(API_URL, selectedContext.contextId, payload);
      await refreshContexts();
      setSelectedContext(updated);
    } catch (err) {
      setContextsError(String(err));
      throw err;
    } finally {
      setContextSubmitting(false);
    }
  }

  async function handleDeleteContext() {
    if (!API_URL || !selectedContext) return;
    if (!window.confirm(`Delete context "${selectedContext.name}"? This cannot be undone.`)) return;
    setContextSubmitting(true);
    setContextsError(null);
    try {
      await deleteIncidentContext(API_URL, selectedContext.contextId);
      setSelectedContext(null);
      await refreshContexts();
    } catch (err) {
      setContextsError(String(err));
    } finally {
      setContextSubmitting(false);
    }
  }

  async function handleSignIn() {
    try {
      await signInWithHostedUi();
    } catch (err) {
      setAuthError(String(err));
    }
  }

  if (!API_URL) {
    return (
      <div style={PAGE_STYLE}>
        <h1 style={{ fontSize: '1.4rem', marginBottom: 20 }}>Incident Investigator</h1>
        <p style={{ color: '#b91c1c', fontWeight: 600 }}>Frontend configuration error</p>
        <p style={{ color: '#475569' }}>{API_CONFIG_ERROR}</p>
      </div>
    );
  }

  if (!authReady) {
    return (
      <div style={PAGE_STYLE}>
        <h1 style={{ fontSize: '1.4rem', marginBottom: 20 }}>Incident Investigator</h1>
        <p style={{ color: '#667085' }}>Loading authentication...</p>
      </div>
    );
  }

  if (!signedIn) {
    return (
      <div style={PAGE_STYLE}>
        <h1 style={{ fontSize: '1.4rem', marginBottom: 20 }}>Incident Investigator</h1>
        <p style={{ color: '#475569', marginBottom: 16 }}>Sign in to access the investigation dashboard.</p>
        {authError && <p style={{ color: '#b91c1c', fontSize: '0.9rem', marginBottom: 12 }}>{authError}</p>}
        <button onClick={() => void handleSignIn()} style={buttonStyle('primary')}>
          Sign in with Cognito
        </button>
      </div>
    );
  }

  return (
    <div style={PAGE_STYLE}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
        <h1 style={{ fontSize: '1.4rem', margin: 0, color: '#0f172a' }}>Incident Investigator</h1>
        <button onClick={signOutFromHostedUi} style={buttonStyle('ghost')}>Sign out</button>
      </div>

      <nav style={{ display: 'flex', gap: 0, marginBottom: 24, borderBottom: '1px solid #e2e8f0' }}>
        {(['contexts', 'investigate'] as ActiveView[]).map((view) => {
          const label = view === 'contexts' ? 'Contexts' : 'Investigate';
          const active = activeView === view;
          return (
            <button
              key={view}
              onClick={() => setActiveView(view)}
              style={{
                background: 'none',
                border: 'none',
                borderBottom: active ? '2px solid #2563eb' : '2px solid transparent',
                padding: '10px 18px',
                fontSize: '0.9rem',
                fontWeight: active ? 600 : 400,
                color: active ? '#1e40af' : '#64748b',
                cursor: 'pointer',
                transition: 'all 120ms ease',
              }}
            >
              {label}
            </button>
          );
        })}
      </nav>

      {activeView === 'contexts' && (
        <ManageContextsView
          apiUrl={API_URL}
          contexts={contexts}
          contextsLoading={contextsLoading}
          contextsError={contextsError}
          selectedContext={selectedContext}
          contextSubmitting={contextSubmitting}
          onSelectContext={handleSelectContext}
          onCreateContext={handleCreateContext}
          onUpdateContext={handleUpdateContext}
          onDeleteContext={handleDeleteContext}
        />
      )}

      {activeView === 'investigate' && (
        <InvestigateView
          apiUrl={API_URL}
          state={state}
          contexts={contexts}
          lastTriggerValues={lastTriggerValues}
          onStarted={onStarted}
          onReset={onReset}
          onSignIn={handleSignIn}
          onNavigateToContexts={() => setActiveView('contexts')}
        />
      )}
    </div>
  );
}
