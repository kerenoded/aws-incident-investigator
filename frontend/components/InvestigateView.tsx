import ErrorBoundary from './ErrorBoundary';
import ReportView from './ReportView';
import TriggerForm, { type TriggerFormValues } from './TriggerForm';
import type { IncidentContext } from '../types/incidentContext';
import type { InvestigationState } from '../app/useInvestigationPoller';
import { buttonStyle, cardStyle } from '../app/ui';

const POLL_INTERVAL_MS = 3000;

interface Props {
  apiUrl: string;
  state: InvestigationState;
  contexts: IncidentContext[];
  lastTriggerValues: TriggerFormValues | null;
  onStarted: (incidentId: string, values: TriggerFormValues) => void;
  onReset: () => void;
  onSignIn: () => Promise<void>;
  onNavigateToContexts: () => void;
}

export default function InvestigateView({
  apiUrl,
  state,
  contexts,
  lastTriggerValues,
  onStarted,
  onReset,
  onSignIn,
  onNavigateToContexts,
}: Props) {
  return (
    <section>
      {(state.phase === 'idle' || state.phase === 'submitting') && (
        <TriggerForm
          apiUrl={apiUrl}
          contexts={contexts}
          disabled={state.phase === 'submitting'}
          initialValues={lastTriggerValues ?? undefined}
          onStarted={(id: string, submittedValues: TriggerFormValues) => {
            onStarted(id, submittedValues);
          }}
          onNavigateToContexts={onNavigateToContexts}
        />
      )}

      {state.phase === 'polling' && (
        <div style={{ ...cardStyle, padding: '40px 24px', textAlign: 'center' }}>
          <div style={{ marginBottom: 20 }}>
            <div style={{
              display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
              width: 52, height: 52, borderRadius: '50%',
              background: 'linear-gradient(135deg, #dbeafe, #eff6ff)',
              border: '1px solid #bfdbfe',
            }}>
              <span style={{ fontSize: '1.6rem', animation: 'spin 2s linear infinite' }}>⏳</span>
            </div>
          </div>
          <p style={{ fontWeight: 600, color: '#1a2438', margin: '0 0 6px', fontSize: '1rem' }}>
            Investigating
          </p>
          <code style={{ display: 'inline-block', background: '#f1f5f9', border: '1px solid #e2e8f0', padding: '3px 10px', borderRadius: 6, fontSize: '0.82rem', color: '#475569', marginBottom: 12 }}>
            {state.incidentId}
          </code>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8 }}>
            <span style={{ display: 'inline-flex', gap: 3 }}>
              {[0, 1, 2].map((i) => (
                <span key={i} style={{
                  display: 'inline-block', width: 6, height: 6, borderRadius: '50%',
                  background: '#93c5fd',
                  opacity: 0.4,
                  animation: `pulse 1.2s ease-in-out ${i * 0.2}s infinite`,
                }} />
              ))}
            </span>
            <span style={{ color: '#94a3b8', fontSize: '0.82rem' }}>Polling every {POLL_INTERVAL_MS / 1000}s</span>
          </div>
          <style>{`
            @keyframes pulse { 0%, 80%, 100% { opacity: 0.3; transform: scale(0.8); } 40% { opacity: 1; transform: scale(1); } }
          `}</style>
        </div>
      )}

      {state.phase === 'completed' && (
        <div>
          <div style={{
            display: 'flex',
            alignItems: 'center',
            gap: 12,
            marginBottom: 16,
            padding: '10px 14px',
            background: '#f0fdf4',
            border: '1px solid #bbf7d0',
            borderRadius: 8,
          }}>
            <span style={{ color: '#16a34a', fontWeight: 600, fontSize: '0.92rem' }}>Investigation complete</span>
            <button onClick={onReset} style={buttonStyle('secondary')}>New investigation</button>
          </div>
          <ErrorBoundary>
            <ReportView report={state.report} />
          </ErrorBoundary>
        </div>
      )}

      {state.phase === 'authExpired' && (
        <div style={{ ...cardStyle, padding: '24px 22px' }}>
          <p style={{ color: '#c2410c', fontWeight: 600, margin: '0 0 4px' }}>Session expired</p>
          <p style={{ color: '#667085', fontSize: '0.88rem', marginBottom: 14 }}>Sign in again to continue.</p>
          <div style={{ display: 'flex', gap: 8 }}>
            <button onClick={() => void onSignIn()} style={buttonStyle('primary')}>Sign in again</button>
            <button onClick={onReset} style={buttonStyle('ghost')}>Cancel</button>
          </div>
        </div>
      )}

      {state.phase === 'unauthorized' && (
        <div style={{ ...cardStyle, padding: '24px 22px' }}>
          <p style={{ color: '#b91c1c', fontWeight: 600, margin: '0 0 4px' }}>Not authorized</p>
          <p style={{ color: '#475569', fontSize: '0.9rem' }}>{state.message}</p>
          <button onClick={onReset} style={buttonStyle('secondary')}>Try a different service</button>
        </div>
      )}

      {state.phase === 'error' && (
        <div style={{ ...cardStyle, padding: '24px 22px' }}>
          <p style={{ color: '#b91c1c', fontWeight: 600, margin: '0 0 4px' }}>Error</p>
          <p style={{ color: '#475569', fontSize: '0.9rem' }}>{state.message}</p>
          <button onClick={onReset} style={buttonStyle('secondary')}>Try again</button>
        </div>
      )}
    </section>
  );
}
