import { useEffect, useState } from 'react';
import { buttonStyle, inputStyle, labelStyle } from '../app/ui';
import type {
  IncidentContext,
  IncidentContextUpsertPayload,
  MetricDescriptor,
} from '../types/incidentContext';
import IncidentContextDiscoveryPanel from './IncidentContextDiscoveryPanel';

const DEFAULT_REGION = (import.meta.env.VITE_COGNITO_REGION as string | undefined) ?? '';

interface IncidentContextFormProps {
  investigationsApiUrl: string;
  mode: 'create' | 'edit';
  initialValue?: IncidentContext | null;
  submitting: boolean;
  onSubmit: (payload: IncidentContextUpsertPayload) => Promise<void>;
  onCancelEdit?: () => void;
}

export default function IncidentContextForm({
  investigationsApiUrl,
  mode,
  initialValue,
  submitting,
  onSubmit,
  onCancelEdit,
}: IncidentContextFormProps) {
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [region, setRegion] = useState(DEFAULT_REGION);
  const [logGroups, setLogGroups] = useState<string[]>([]);
  const [xrayServices, setXrayServices] = useState<string[]>([]);
  const [metricDescriptors, setMetricDescriptors] = useState<MetricDescriptor[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!initialValue) {
      if (mode === 'create') {
        setName(''); setDescription(''); setRegion(DEFAULT_REGION);
        setLogGroups([]); setXrayServices([]); setMetricDescriptors([]);
      }
      return;
    }
    setName(initialValue.name);
    setDescription(initialValue.description ?? '');
    setRegion(initialValue.region);
    setLogGroups(initialValue.logGroups ?? []);
    setXrayServices(initialValue.xrayServices ?? []);
    setMetricDescriptors(initialValue.metricDescriptors ?? []);
  }, [initialValue, mode]);

  async function handleSubmit(event: React.FormEvent) {
    event.preventDefault();
    setError(null);
    try {
      const payload: IncidentContextUpsertPayload = {
        name: name.trim(), description: description.trim(), region: region.trim(),
        logGroups, xrayServices, metricDescriptors,
      };
      if (!payload.name || !payload.region) { setError('Name and region are required.'); return; }
      await onSubmit(payload);
      if (mode === 'create') {
        setName(''); setDescription(''); setRegion(DEFAULT_REGION);
        setLogGroups([]); setXrayServices([]); setMetricDescriptors([]);
      }
    } catch (err) { setError(String(err)); }
  }

  return (
    <form onSubmit={handleSubmit}>
      {/* Name + Region side by side */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 160px', gap: 12, marginBottom: 10 }}>
        <label style={labelStyle}>
          Name <span style={{ color: '#e11d48' }}>*</span>
          <input value={name} onChange={(e) => setName(e.target.value)} required disabled={submitting} style={inputStyle} />
        </label>
        <label style={labelStyle}>
          Region <span style={{ color: '#e11d48' }}>*</span>
          <input value={region} onChange={(e) => setRegion(e.target.value)} required disabled={submitting} placeholder="eu-west-1" style={inputStyle} />
        </label>
      </div>

      <label style={labelStyle}>
        Description <span style={{ color: '#e11d48' }}>*</span>
        <textarea value={description} onChange={(e) => setDescription(e.target.value)} required disabled={submitting} rows={2} style={{ ...inputStyle, resize: 'vertical' }} />
      </label>

      <IncidentContextDiscoveryPanel
        investigationsApiUrl={investigationsApiUrl}
        region={region}
        disabled={submitting}
        state={{ logGroups, metricDescriptors, xrayServices }}
        onChange={(next) => {
          setLogGroups(next.logGroups);
          setMetricDescriptors(next.metricDescriptors);
          setXrayServices(next.xrayServices);
        }}
      />

      {error && <p style={{ color: '#b91c1c', fontSize: '0.88rem', marginBottom: 10 }}>{error}</p>}

      <div style={{ display: 'flex', gap: 8 }}>
        <button type="submit" disabled={submitting} style={buttonStyle('primary', submitting)}>
          {submitting ? 'Saving...' : mode === 'create' ? 'Create context' : 'Save changes'}
        </button>
        {onCancelEdit && (
          <button type="button" onClick={onCancelEdit} disabled={submitting} style={buttonStyle('ghost', submitting)}>Cancel</button>
        )}
      </div>
    </form>
  );
}

