import { useEffect, useState } from 'react';
import { AuthRequiredError } from '../app/auth';
import { startInvestigation } from '../app/investigationsApi';
import { buttonStyle, cardStyle, inputStyle, labelStyle, formatDateTimeUtc } from '../app/ui';
import { IncidentContext } from '../types/incidentContext';

interface TriggerFormProps {
  apiUrl: string;
  contexts: IncidentContext[];
  disabled: boolean;
  onStarted: (incidentId: string, submittedValues: TriggerFormValues) => void;
  onNavigateToContexts: () => void;
  initialValues?: TriggerFormValues;
}

export interface TriggerFormValues {
  contextId: string;
  signalType: string;
  windowStart: string;
  windowEnd: string;
}

const SIGNAL_TYPES = ['latency_spike', 'error_spike'];

function localDatetimeToUtcIso(value: string): string {
  return new Date(value).toISOString();
}

function utcPreview(value: string): string {
  if (!value) return '';
  return `UTC: ${formatDateTimeUtc(localDatetimeToUtcIso(value))}`;
}

export default function TriggerForm({
  apiUrl,
  contexts,
  disabled,
  onStarted,
  onNavigateToContexts,
  initialValues,
}: TriggerFormProps) {
  const [selectedContextId, setSelectedContextId] = useState<string>(initialValues?.contextId ?? '');
  const [signalType, setSignalType] = useState(initialValues?.signalType ?? SIGNAL_TYPES[0]);
  const [windowStart, setWindowStart] = useState(initialValues?.windowStart ?? '');
  const [windowEnd, setWindowEnd] = useState(initialValues?.windowEnd ?? '');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (!initialValues) return;
    setSelectedContextId(initialValues.contextId ?? '');
    setSignalType(initialValues.signalType ?? SIGNAL_TYPES[0]);
    setWindowStart(initialValues.windowStart ?? '');
    setWindowEnd(initialValues.windowEnd ?? '');
  }, [initialValues]);

  useEffect(() => {
    if (contexts.length > 0 && !selectedContextId) {
      setSelectedContextId(contexts[0].contextId);
    }
  }, [contexts]);

  const selectedContext = contexts.find((c) => c.contextId === selectedContextId) ?? null;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    if (!selectedContextId) {
      setError('Select an incident context before starting an investigation.');
      return;
    }

    if (windowStart && windowEnd && windowStart >= windowEnd) {
      setError('Window start must be before window end.');
      return;
    }

    setSubmitting(true);
    try {
      const data = await startInvestigation(apiUrl, {
        contextId: selectedContextId,
        signalType,
        windowStart: windowStart ? localDatetimeToUtcIso(windowStart) : '',
        windowEnd: windowEnd ? localDatetimeToUtcIso(windowEnd) : '',
      });
      onStarted(data.incidentId, {
        contextId: selectedContextId,
        signalType,
        windowStart,
        windowEnd,
      });
    } catch (err) {
      setSubmitting(false);
      if (err instanceof AuthRequiredError) {
        setError('Your session has expired. Please sign in again.');
        return;
      }
      setError(`Network error: ${String(err)}`);
    }
  }

  if (contexts.length === 0) {
    return (
      <div style={{ ...cardStyle, padding: '28px 22px', textAlign: 'center' }}>
        <p style={{ color: '#667085', marginBottom: 16 }}>
          Create at least one incident context before starting an investigation.
        </p>
        <button type="button" onClick={onNavigateToContexts} style={buttonStyle('primary')}>
          Create your first context
        </button>
      </div>
    );
  }

  const isDisabled = disabled || submitting;

  return (
    <form onSubmit={handleSubmit} style={{ ...cardStyle, padding: '20px 22px' }}>
      <h2 style={{ fontSize: '1.1rem', marginTop: 0, marginBottom: 18, color: '#1a2438' }}>Start Investigation</h2>

      <label style={labelStyle}>
        Incident context
        <select
          value={selectedContextId}
          onChange={(e) => setSelectedContextId(e.target.value)}
          required
          disabled={isDisabled}
          style={inputStyle}
        >
          {contexts.map((c) => (
            <option key={c.contextId} value={c.contextId}>{c.name}</option>
          ))}
        </select>
        {selectedContext && (
          <span style={{ fontSize: '0.82rem', color: '#98a2b3', marginTop: 3, display: 'block' }}>
            {selectedContext.region}
          </span>
        )}
      </label>

      <label style={labelStyle}>
        Signal type
        <select
          value={signalType}
          onChange={(e) => setSignalType(e.target.value)}
          required
          disabled={isDisabled}
          style={inputStyle}
        >
          {SIGNAL_TYPES.map((t) => (
            <option key={t} value={t}>{t.replace('_', ' ')}</option>
          ))}
        </select>
      </label>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
        <label style={labelStyle}>
          Window start (local time)
          <input
            type="datetime-local"
            value={windowStart}
            onChange={(e) => setWindowStart(e.target.value)}
            required
            disabled={isDisabled}
            style={inputStyle}
          />
          {windowStart && (
            <span style={{ fontSize: '0.78rem', color: '#98a2b3', marginTop: 2, display: 'block' }}>{utcPreview(windowStart)}</span>
          )}
        </label>

        <label style={labelStyle}>
          Window end (local time)
          <input
            type="datetime-local"
            value={windowEnd}
            onChange={(e) => setWindowEnd(e.target.value)}
            required
            disabled={isDisabled}
            style={inputStyle}
          />
          {windowEnd && (
            <span style={{ fontSize: '0.78rem', color: '#98a2b3', marginTop: 2, display: 'block' }}>{utcPreview(windowEnd)}</span>
          )}
        </label>
      </div>

      {error && <p style={{ color: '#b91c1c', fontSize: '0.88rem', marginBottom: 10 }}>{error}</p>}

      <button type="submit" disabled={isDisabled} style={{ ...buttonStyle('primary', isDisabled), marginTop: 4, display: 'flex', alignItems: 'center', gap: 6 }}>
        {submitting ? (
          <>
            <span style={{ display: 'inline-block', width: 12, height: 12, border: '2px solid rgba(255,255,255,0.4)', borderTopColor: '#fff', borderRadius: '50%', animation: 'spin 0.7s linear infinite' }} />
            Starting...
            <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
          </>
        ) : 'Investigate'}
      </button>
    </form>
  );
}
