import { type ReactNode, useState } from 'react';
import {
  discoverLogGroups,
  discoverMetricNamespaces,
  discoverMetrics,
  discoverXrayServices,
  type MetricNamespaceDiscoveryItem,
} from '../app/discoveryApi';
import { buttonStyle, inputStyle, resourceItemStyle, resourceTypeTheme } from '../app/ui';
import type { ResourceType } from '../app/ui';
import type { MetricDescriptor } from '../types/incidentContext';
import ResourceTabs from './ResourceTabs';

interface DiscoveryState {
  logGroups: string[];
  metricDescriptors: MetricDescriptor[];
  xrayServices: string[];
}

interface IncidentContextDiscoveryPanelProps {
  investigationsApiUrl: string;
  region: string;
  state: DiscoveryState;
  disabled?: boolean;
  onChange: (next: DiscoveryState) => void;
}

export default function IncidentContextDiscoveryPanel({
  investigationsApiUrl,
  region,
  state,
  disabled,
  onChange,
}: IncidentContextDiscoveryPanelProps) {
  const [activeTab, setActiveTab] = useState<ResourceType>('log');
  const [error, setError] = useState<string | null>(null);
  const [searching, setSearching] = useState(false);

  // Log group search state
  const [logQuery, setLogQuery] = useState('');
  const [logResults, setLogResults] = useState<Array<{ logGroupName: string; arn?: string }>>([]);

  // Metric search state
  const [namespaceQuery, setNamespaceQuery] = useState('');
  const [namespaceResults, setNamespaceResults] = useState<MetricNamespaceDiscoveryItem[]>([]);
  const [selectedNamespace, setSelectedNamespace] = useState<string | null>(null);
  const [metricQuery, setMetricQuery] = useState('');
  const [metricResults, setMetricResults] = useState<MetricDescriptor[]>([]);
  const [selectedStat, setSelectedStat] = useState('Average');

  // X-Ray search state
  const [xrayQuery, setXrayQuery] = useState('');
  const [xrayResults, setXrayResults] = useState<Array<{ serviceName: string }>>([]);

  function requireRegion(): boolean {
    if (!region.trim()) { setError('Region is required before discovery.'); return false; }
    return true;
  }

  async function searchLogs() {
    if (!requireRegion()) return;
    setError(null); setSearching(true);
    try {
      const res = await discoverLogGroups(investigationsApiUrl, { region, q: logQuery, pageSize: 25 });
      setLogResults(res.items);
    } catch (err) { setError(String(err)); } finally { setSearching(false); }
  }

  async function searchNamespaces() {
    if (!requireRegion()) return;
    setError(null); setSearching(true);
    try {
      const res = await discoverMetricNamespaces(investigationsApiUrl, { region, q: namespaceQuery, pageSize: 25 });
      setNamespaceResults(res.items);
    } catch (err) { setError(String(err)); } finally { setSearching(false); }
  }

  function clearNamespace() {
    setSelectedNamespace(null);
    setNamespaceResults([]);
    setMetricResults([]);
    setMetricQuery('');
  }

  async function searchMetrics() {
    if (!requireRegion()) return;
    if (!selectedNamespace) { setError('Select a metric namespace first.'); return; }
    setError(null); setSearching(true);
    try {
      const res = await discoverMetrics(investigationsApiUrl, { region, namespace: selectedNamespace, q: metricQuery, pageSize: 25 });
      setMetricResults(res.items);
    } catch (err) { setError(String(err)); } finally { setSearching(false); }
  }

  async function searchXrayServices() {
    if (!requireRegion()) return;
    setError(null); setSearching(true);
    try {
      const res = await discoverXrayServices(investigationsApiUrl, { region, q: xrayQuery, lookbackMinutes: 180, pageSize: 25 });
      setXrayResults(res.items);
    } catch (err) { setError(String(err)); } finally { setSearching(false); }
  }

  const counts = {
    log: state.logGroups.length,
    metric: state.metricDescriptors.length,
    xray: state.xrayServices.length,
  };

  return (
    <div style={{ border: '1px solid #e2e8f0', borderRadius: 10, overflow: 'hidden', marginBottom: 12 }}>
      {/* Panel header */}
      <div style={{ background: '#f8fafc', padding: '10px 14px', borderBottom: '1px solid #e2e8f0' }}>
        <div style={{ fontSize: 12, fontWeight: 600, color: '#374151' }}>Discover AWS Resources</div>
        <div style={{ fontSize: 11, color: '#94a3b8', marginTop: 1 }}>Search your account and add resources to this context</div>
      </div>

      {error && <div style={{ background: '#fef2f2', padding: '8px 14px', fontSize: 12, color: '#b91c1c' }}>{error}</div>}

      <ResourceTabs activeTab={activeTab} counts={counts} onChange={setActiveTab} />

      <div style={{ padding: '12px 14px' }}>
        {activeTab === 'log' && (
          <LogGroupsTab
            query={logQuery} onQueryChange={setLogQuery} onSearch={searchLogs}
            results={logResults} added={state.logGroups} disabled={!!disabled || searching} searching={searching}
            onAdd={(lg) => onChange({ ...state, logGroups: dedupeStrings([...state.logGroups, lg]) })}
            onRemove={(lg) => onChange({ ...state, logGroups: state.logGroups.filter((x) => x !== lg) })}
          />
        )}
        {activeTab === 'metric' && (
          <MetricsTab
            namespaceQuery={namespaceQuery} onNamespaceQueryChange={setNamespaceQuery}
            onSearchNamespaces={searchNamespaces} namespaceResults={namespaceResults}
            selectedNamespace={selectedNamespace} onSelectNamespace={setSelectedNamespace}
            onClearNamespace={clearNamespace}
            selectedStat={selectedStat} onStatChange={setSelectedStat}
            metricQuery={metricQuery} onMetricQueryChange={setMetricQuery}
            onSearchMetrics={searchMetrics} metricResults={metricResults}
            added={state.metricDescriptors} disabled={!!disabled || searching} searching={searching}
            onAdd={(item) => onChange({ ...state, metricDescriptors: dedupeMetricDescriptors([...state.metricDescriptors, { ...item, stat: selectedStat }]) })}
            onRemove={(idx) => onChange({ ...state, metricDescriptors: state.metricDescriptors.filter((_, i) => i !== idx) })}
          />
        )}
        {activeTab === 'xray' && (
          <XRayTab
            query={xrayQuery} onQueryChange={setXrayQuery} onSearch={searchXrayServices}
            results={xrayResults} added={state.xrayServices} disabled={!!disabled || searching} searching={searching}
            onAdd={(svc) => onChange({ ...state, xrayServices: dedupeStrings([...state.xrayServices, svc]) })}
            onRemove={(svc) => onChange({ ...state, xrayServices: state.xrayServices.filter((x) => x !== svc) })}
          />
        )}
      </div>
    </div>
  );
}

// ── Log Groups Tab ──────────────────────────────────────────────────────────

function LogGroupsTab({ query, onQueryChange, onSearch, results, added, disabled, searching, onAdd, onRemove }: {
  query: string; onQueryChange: (v: string) => void; onSearch: () => void;
  results: Array<{ logGroupName: string }>; added: string[]; disabled: boolean; searching: boolean;
  onAdd: (lg: string) => void; onRemove: (lg: string) => void;
}) {
  return (
    <>
      <SearchRow>
        <input value={query} onChange={(e) => onQueryChange(e.target.value)} disabled={disabled} placeholder="Filter log groups..." style={inputStyle} />
        <SearchButton onClick={onSearch} disabled={disabled} searching={searching} />
      </SearchRow>
      <ResultList rows={results.map((r) => ({ key: r.logGroupName, text: r.logGroupName, onAdd: () => onAdd(r.logGroupName) }))} type="log" />
      <AddedList rows={added.map((lg) => ({ key: lg, text: lg, onRemove: () => onRemove(lg) }))} type="log" />
    </>
  );
}

// ── Metrics Tab ─────────────────────────────────────────────────────────────

function MetricsTab({ namespaceQuery, onNamespaceQueryChange, onSearchNamespaces, namespaceResults, selectedNamespace, onSelectNamespace, onClearNamespace, selectedStat, onStatChange, metricQuery, onMetricQueryChange, onSearchMetrics, metricResults, added, disabled, searching, onAdd, onRemove }: {
  namespaceQuery: string; onNamespaceQueryChange: (v: string) => void; onSearchNamespaces: () => void;
  namespaceResults: MetricNamespaceDiscoveryItem[]; selectedNamespace: string | null;
  onSelectNamespace: (ns: string) => void; onClearNamespace: () => void;
  selectedStat: string; onStatChange: (s: string) => void;
  metricQuery: string; onMetricQueryChange: (v: string) => void; onSearchMetrics: () => void;
  metricResults: MetricDescriptor[]; added: MetricDescriptor[]; disabled: boolean; searching: boolean;
  onAdd: (item: MetricDescriptor) => void; onRemove: (idx: number) => void;
}) {
  return (
    <>
      {!selectedNamespace ? (
        <>
          <SearchRow>
            <input value={namespaceQuery} onChange={(e) => onNamespaceQueryChange(e.target.value)} disabled={disabled} placeholder="Namespace (optional filter)" style={inputStyle} />
            <SearchButton onClick={onSearchNamespaces} disabled={disabled} searching={searching} />
          </SearchRow>
          {namespaceResults.length > 0 && (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginTop: 4 }}>
              {namespaceResults.map((ns) => (
                <button key={ns.namespace} type="button" onClick={() => onSelectNamespace(ns.namespace)}
                  style={{ background: '#eff6ff', border: '1px solid #c5d5f7', borderRadius: 6, padding: '4px 10px', fontSize: 12, fontFamily: 'monospace', cursor: 'pointer' }}>
                  {ns.namespace}
                </button>
              ))}
            </div>
          )}
        </>
      ) : (
        <>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
            <span style={{ background: '#eff6ff', border: '1px solid #c5d5f7', borderRadius: 4, padding: '2px 8px', fontSize: 12, fontFamily: 'monospace' }}>
              {selectedNamespace}
            </span>
            <button type="button" onClick={onClearNamespace} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#94a3b8', fontSize: 16 }}>×</button>
          </div>
          <SearchRow>
            <select value={selectedStat} onChange={(e) => onStatChange(e.target.value)} disabled={disabled} style={{ ...inputStyle, flexShrink: 0, width: 'auto' }}>
              {['Average', 'Sum', 'Maximum', 'Minimum', 'p99', 'p95', 'p50'].map((s) => <option key={s}>{s}</option>)}
            </select>
            <input value={metricQuery} onChange={(e) => onMetricQueryChange(e.target.value)} disabled={disabled} placeholder="Metric name filter" style={inputStyle} />
            <SearchButton onClick={onSearchMetrics} disabled={disabled} searching={searching} />
          </SearchRow>
          <ResultList
            rows={metricResults.map((item, idx) => ({
              key: `${item.namespace}-${item.metricName}-${idx}`,
              text: formatMetricDescriptor({ ...item, stat: selectedStat }),
              onAdd: () => onAdd(item),
            }))}
            type="metric"
          />
        </>
      )}
      <AddedList rows={added.map((m, idx) => ({ key: `added-${idx}`, text: formatMetricDescriptor(m), onRemove: () => onRemove(idx) }))} type="metric" />
    </>
  );
}

// ── X-Ray Tab ───────────────────────────────────────────────────────────────

function XRayTab({ query, onQueryChange, onSearch, results, added, disabled, searching, onAdd, onRemove }: {
  query: string; onQueryChange: (v: string) => void; onSearch: () => void;
  results: Array<{ serviceName: string }>; added: string[]; disabled: boolean; searching: boolean;
  onAdd: (svc: string) => void; onRemove: (svc: string) => void;
}) {
  return (
    <>
      <SearchRow>
        <input value={query} onChange={(e) => onQueryChange(e.target.value)} disabled={disabled} placeholder="Search observed service names" style={inputStyle} />
        <SearchButton onClick={onSearch} disabled={disabled} searching={searching} />
      </SearchRow>
      <p style={{ margin: '4px 0 8px', fontSize: 11, color: '#94a3b8' }}>
        X-Ray discovery returns recently observed services, not full account inventory.
      </p>
      <ResultList rows={results.map((r) => ({ key: r.serviceName, text: r.serviceName, onAdd: () => onAdd(r.serviceName) }))} type="xray" />
      <AddedList rows={added.map((svc) => ({ key: svc, text: svc, onRemove: () => onRemove(svc) }))} type="xray" />
    </>
  );
}

// ── Shared sub-components ───────────────────────────────────────────────────

function SearchButton({ onClick, disabled, searching }: { onClick: () => void; disabled: boolean; searching: boolean }) {
  return (
    <button type="button" onClick={onClick} disabled={disabled} style={{ ...buttonStyle('primary', disabled), flexShrink: 0, display: 'flex', alignItems: 'center', gap: 6 }}>
      {searching ? (
        <>
          <span style={{ display: 'inline-block', width: 12, height: 12, border: '2px solid rgba(255,255,255,0.4)', borderTopColor: '#fff', borderRadius: '50%', animation: 'spin 0.7s linear infinite' }} />
          Searching...
        </>
      ) : 'Search'}
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </button>
  );
}

function SearchRow({ children }: { children: ReactNode }) {
  return <div style={{ display: 'flex', gap: 8, marginBottom: 10 }}>{children}</div>;
}

function ResultList({ rows, type }: { rows: Array<{ key: string; text: string; onAdd: () => void }>; type: ResourceType }) {
  if (rows.length === 0) return null;
  return (
    <div style={{ display: 'grid', gap: 5, marginBottom: 8 }}>
      {rows.map((row) => (
        <div key={row.key} style={{ ...resourceItemStyle(type), justifyContent: 'space-between' }}>
          <span style={{ color: resourceTypeTheme(type).color }}>{row.text}</span>
          <button type="button" onClick={row.onAdd} style={buttonStyle('secondary')}>Add</button>
        </div>
      ))}
    </div>
  );
}

function AddedList({ rows, type }: { rows: Array<{ key: string; text: string; onRemove: () => void }>; type: ResourceType }) {
  if (rows.length === 0) return <div style={{ color: '#94a3b8', fontSize: 12, marginBottom: 4 }}>None added yet.</div>;
  return (
    <div style={{ display: 'grid', gap: 5, marginBottom: 4 }}>
      {rows.map((row) => (
        <div key={row.key} style={resourceItemStyle(type)}>
          <span style={{ color: resourceTypeTheme(type).color, flex: 1, minWidth: 0 }}>{row.text}</span>
          <button type="button" onClick={row.onRemove}
            style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#94a3b8', fontSize: 16, lineHeight: 1, padding: '0 2px', flexShrink: 0 }}>
            ×
          </button>
        </div>
      ))}
    </div>
  );
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function formatMetricDescriptor(m: MetricDescriptor): string {
  const dims = m.dimensions && Object.keys(m.dimensions).length > 0
    ? ` [${Object.entries(m.dimensions).map(([k, v]) => `${k}=${v}`).join(', ')}]` : '';
  const stat = m.stat ? ` (${m.stat})` : '';
  return `${m.namespace ?? 'unknown'} / ${m.metricName ?? 'unknown'}${dims}${stat}`;
}

function dedupeStrings(values: string[]): string[] {
  return Array.from(new Set(values.map((v) => v.trim()).filter(Boolean)));
}

function dedupeMetricDescriptors(values: MetricDescriptor[]): MetricDescriptor[] {
  const seen = new Set<string>();
  return values.filter((m) => {
    const key = `${m.namespace}::${m.metricName}::${m.stat}::${JSON.stringify(m.dimensions)}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}
