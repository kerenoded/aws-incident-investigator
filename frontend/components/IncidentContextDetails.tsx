import { useState } from 'react';
import { formatDateTime, buttonStyle, cardStyle, metadataBarStyle, resourceItemStyle, resourceTypeTheme } from '../app/ui';
import type { ResourceType } from '../app/ui';
import type { IncidentContext, MetricDescriptor } from '../types/incidentContext';
import ResourceTabs from './ResourceTabs';

interface IncidentContextDetailsProps {
  context: IncidentContext;
  onEdit?: () => void;
  onDelete?: () => void;
  deleteDisabled?: boolean;
}

export default function IncidentContextDetails({
  context,
  onEdit,
  onDelete,
  deleteDisabled,
}: IncidentContextDetailsProps) {
  const [activeTab, setActiveTab] = useState<ResourceType>('log');

  const counts = {
    log: context.logGroups?.length ?? 0,
    metric: context.metricDescriptors?.length ?? 0,
    xray: context.xrayServices?.length ?? 0,
  };

  return (
    <div style={{ ...cardStyle, padding: '16px 18px' }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 20, marginBottom: 14 }}>
        <div>
          <h3 style={{ margin: '0 0 2px', fontSize: '1.05rem', color: '#1a2438' }}>{context.name}</h3>
          <p style={{ margin: 0, color: '#667085', fontSize: '0.88rem' }}>{context.description || 'No description.'}</p>
        </div>
        {(onEdit || onDelete) && (
          <div style={{ display: 'flex', gap: 6, flexShrink: 0 }}>
            {onEdit && <button type="button" onClick={onEdit} style={buttonStyle('secondary')}>Edit</button>}
            {onDelete && <button type="button" onClick={onDelete} disabled={deleteDisabled} style={buttonStyle('danger', deleteDisabled)}>Delete</button>}
          </div>
        )}
      </div>

      {/* Metadata bar */}
      <div style={{ ...metadataBarStyle, display: 'flex', padding: '9px 16px', marginBottom: 16 }}>
        <MetaField label="Context ID" value={context.contextId} mono flex1 />
        <Divider />
        <MetaField label="Region" value={context.region} mono />
        <Divider />
        <MetaField label="Created" value={formatDateTime(context.createdAt)} />
        <Divider />
        <MetaField label="Updated" value={formatDateTime(context.updatedAt)} />
      </div>

      {/* Tabbed resources */}
      <div style={{ border: '1px solid #e2e8f0', borderRadius: 10, overflow: 'hidden' }}>
        <ResourceTabs activeTab={activeTab} counts={counts} onChange={setActiveTab} />
        <div style={{ padding: '12px 14px' }}>
          {activeTab === 'log' && <ReadOnlyList type="log" items={context.logGroups ?? []} />}
          {activeTab === 'metric' && <MetricList items={context.metricDescriptors ?? []} />}
          {activeTab === 'xray' && <ReadOnlyList type="xray" items={context.xrayServices ?? []} />}
        </div>
      </div>
    </div>
  );
}

function MetaField({ label, value, mono, flex1 }: { label: string; value: string; mono?: boolean; flex1?: boolean }) {
  return (
    <div style={{ flex: flex1 ? 1 : '0 0 auto', minWidth: 0 }}>
      <div style={{ fontSize: 9, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '.07em', marginBottom: 3 }}>
        {label}
      </div>
      <div style={{
        fontSize: 11,
        color: '#374151',
        fontFamily: mono ? 'monospace' : undefined,
        overflow: flex1 ? 'hidden' : undefined,
        textOverflow: flex1 ? 'ellipsis' : undefined,
        whiteSpace: flex1 ? 'nowrap' : undefined,
      }}>
        {value}
      </div>
    </div>
  );
}

function Divider() {
  return <div style={{ width: 1, background: '#cbd5e1', margin: '0 14px', flexShrink: 0 }} />;
}

function ReadOnlyList({ type, items }: { type: ResourceType; items: string[] }) {
  if (items.length === 0) return <EmptyState />;
  return (
    <div style={{ display: 'grid', gap: 5 }}>
      {items.map((item, i) => (
        <div key={i} style={resourceItemStyle(type)}>
          <span style={{ color: resourceTypeTheme(type).color }}>{item}</span>
        </div>
      ))}
    </div>
  );
}

function MetricList({ items }: { items: MetricDescriptor[] }) {
  if (items.length === 0) return <EmptyState />;
  return (
    <div style={{ display: 'grid', gap: 5 }}>
      {items.map((m, i) => (
        <div key={i} style={resourceItemStyle('metric')}>
          <span style={{ color: resourceTypeTheme('metric').color }}>{formatMetric(m)}</span>
        </div>
      ))}
    </div>
  );
}

function EmptyState() {
  return <div style={{ color: '#94a3b8', fontSize: 12 }}>None added.</div>;
}

function formatMetric(m: MetricDescriptor): string {
  const dims = m.dimensions && Object.keys(m.dimensions).length > 0
    ? ` [${Object.entries(m.dimensions).map(([k, v]) => `${k}=${v}`).join(', ')}]`
    : '';
  const stat = m.stat ? ` (${m.stat})` : '';
  return `${m.namespace ?? 'unknown'} / ${m.metricName ?? 'unknown'}${dims}${stat}`;
}
