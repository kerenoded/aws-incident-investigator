import { cardStyle, formatDateTime } from '../app/ui';
import type { IncidentContext } from '../types/incidentContext';

interface IncidentContextListProps {
  items: IncidentContext[];
  selectedContextId: string | null;
  onSelect: (contextId: string) => void;
}

export default function IncidentContextList({
  items,
  selectedContextId,
  onSelect,
}: IncidentContextListProps) {
  if (items.length === 0) {
    return (
      <div style={{ ...cardStyle, padding: '20px 16px', textAlign: 'center', color: '#98a2b3', fontSize: '0.9rem' }}>
        No contexts yet.
      </div>
    );
  }

  return (
    <div style={{ ...cardStyle, overflow: 'hidden' }}>
      {items.map((item) => {
        const selected = selectedContextId === item.contextId;
        return (
          <button
            key={item.contextId}
            type="button"
            onClick={() => onSelect(item.contextId)}
            style={{
              width: '100%',
              textAlign: 'left',
              border: 'none',
              borderBottom: '1px solid #f1f5f9',
              padding: '11px 14px',
              cursor: 'pointer',
              background: selected ? '#eff6ff' : '#fff',
              borderLeft: selected ? '3px solid #2563eb' : '3px solid transparent',
              transition: 'background 100ms ease',
            }}
          >
            <div style={{ fontWeight: 500, fontSize: '0.9rem', color: '#1a2438' }}>{item.name}</div>
            <div style={{ fontSize: '0.8rem', color: '#98a2b3', marginTop: 2 }}>
              {item.region} &middot; {formatDateTime(item.updatedAt)}
            </div>
          </button>
        );
      })}
    </div>
  );
}
