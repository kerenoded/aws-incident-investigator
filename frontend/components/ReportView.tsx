import React from 'react';
import { formatDateTimeUtc } from '../app/ui';
import type {
  AiAssessment,
  AiNextBestAction,
  ConfidenceExplanation,
  FinalReport,
  OperatorFocus,
  TopHypothesis,
} from '../types/report';

interface ReportViewProps {
  report: FinalReport;
}

export default function ReportView({ report }: ReportViewProps) {
  const topHypothesis = report.topHypotheses?.[0];
  const topConfidencePct = topHypothesis ? Math.round(topHypothesis.confidence * 100) : null;
  const strongestEvidence = report.confidenceExplanation?.strongestEvidence ?? [];
  const supportingSources = buildSupportingSources(report, strongestEvidence);
  const aiContrib = report.confidenceExplanation?.contribution?.ai;
  const aiPlausibilityPct =
    aiContrib?.assessmentAvailable && aiContrib.topHypothesisMatch && aiContrib.plausibility != null
      ? Math.round(aiContrib.plausibility * 100)
      : null;

  return (
    <div>
      <Section title="Investigation Overview" tier="primary">
        <AtAGlanceBlock
          topCause={topHypothesis?.cause}
          topConfidencePct={topConfidencePct}
          aiPlausibilityPct={aiPlausibilityPct}
          strongestEvidenceCount={strongestEvidence.length}
          supportingSources={supportingSources}
          incidentWindow={report.incidentWindow}
          baselineWindow={report.baselineWindow}
        />
        <p style={{ margin: 0, fontSize: '1rem', lineHeight: 1.6, color: '#222' }}>{report.summary}</p>
      </Section>

      {report.operatorFocus && (
        <Section title="Where to Look First" tier="primary">
          <OperatorFocusBlock operatorFocus={report.operatorFocus} />
        </Section>
      )}

      <Section title="Root Cause Analysis" tier="secondary">
        <HypothesesTable hypotheses={report.topHypotheses} />
        {report.confidenceExplanation && (
          <div style={{ marginTop: 14 }}>
          <ConfidenceExplanationBlock
            confidenceExplanation={report.confidenceExplanation}
            topHypothesis={report.topHypotheses?.[0]}
          />
          </div>
        )}
      </Section>

      {report.aiAssessments && report.aiAssessments.length > 0 && (
        <Section title="AI Advisory" tier="ai">
          <AiAssessmentsBlock assessments={report.aiAssessments} />
          {report.aiNextBestActions && report.aiNextBestActions.length > 0 && (
            <div style={{ marginTop: 14 }}>
              <AiNextBestActionsBlock actions={report.aiNextBestActions} />
            </div>
          )}
        </Section>
      )}

      {!report.aiAssessments?.length && report.aiNextBestActions && report.aiNextBestActions.length > 0 && (
        <Section title="AI Advisory" tier="ai">
          <AiNextBestActionsBlock actions={report.aiNextBestActions} />
        </Section>
      )}

      {report.evidenceHighlights && report.evidenceHighlights.length > 0 && (
        <Section title="Evidence Highlights" tier="secondary">
          <ul style={{ margin: 0, paddingLeft: 20 }}>
            {report.evidenceHighlights.map((h, i) => (
              <li key={i} style={{ marginBottom: 6, fontSize: '0.92rem', lineHeight: 1.5 }}>
                <EvidenceHighlightItem text={h} />
              </li>
            ))}
          </ul>
        </Section>
      )}

      {report.workerErrors && report.workerErrors.length > 0 && (
        <Section title="Collection Warnings" tier="secondary">
          <div>
            {report.workerErrors.map((e, i) => (
              <div
                key={i}
                style={{
                  marginBottom: 6,
                  padding: '7px 12px',
                  borderLeft: '3px solid #e09000',
                  background: '#fffbf0',
                  fontSize: '0.88rem',
                  borderRadius: '0 4px 4px 0',
                }}
              >
                <span style={{ fontWeight: 600, color: '#7a4000' }}>{e.source}</span>
                {': '}
                <span style={{ color: '#555' }}>{e.reason}</span>
              </div>
            ))}
          </div>
        </Section>
      )}
    </div>
  );
}

function buildSupportingSources(report: FinalReport, strongestEvidence: ConfidenceExplanation['strongestEvidence']): string[] {
  const fromEvidenceIds = Array.from(
    new Set(
      (report.topHypotheses ?? [])
        .flatMap((hypothesis) => hypothesis.supportingEvidenceIds ?? [])
        .map((evidenceId) => sourceFromEvidenceId(evidenceId))
        .filter((source): source is string => typeof source === 'string' && source.length > 0),
    ),
  );
  if (fromEvidenceIds.length > 0) {
    return fromEvidenceIds;
  }

  // Backward-compatible fallback for reports missing supportingEvidenceIds.
  return Array.from(
    new Set(
      strongestEvidence
        .map((item) => item.source)
        .filter((source): source is string => typeof source === 'string' && source.length > 0),
    ),
  );
}

function sourceFromEvidenceId(evidenceId: string): string | null {
  if (evidenceId.startsWith('ev-logs-')) return 'logs';
  if (evidenceId.startsWith('ev-metrics-')) return 'metrics';
  if (evidenceId.startsWith('ev-traces-')) return 'traces';
  return null;
}

// Parses "summary text [finding_type]" and renders the bracketed part as a styled tag.
function EvidenceHighlightItem({ text }: { text: string }) {
  const match = text.match(/^(.*?)\s*\[([^\]]+)\]$/);
  if (!match) return <span style={{ color: '#333' }}>{text}</span>;
  return (
    <span>
      <span style={{ color: '#222' }}>{match[1]}</span>{' '}
      <span
        style={{
          background: '#eef2fb',
          color: '#3659d9',
          borderRadius: 3,
          padding: '1px 6px',
          fontSize: '0.75rem',
          fontWeight: 600,
          letterSpacing: '0.02em',
        }}
      >
        {match[2]}
      </span>
    </span>
  );
}

type SectionTier = 'primary' | 'secondary' | 'ai';

function Section({ title, tier = 'secondary', children }: { title: string; tier?: SectionTier; children: React.ReactNode }) {
  const isPrimary = tier === 'primary';
  const isAi = tier === 'ai';

  const wrapperStyle: React.CSSProperties = {
    marginBottom: 28,
    ...(isAi ? {
      background: '#fffbec',
      border: '1px solid #f0c040',
      borderRadius: 8,
      padding: '16px 20px',
    } : {}),
  };

  const headingStyle: React.CSSProperties = {
    margin: '0 0 12px 0',
    paddingBottom: isPrimary ? 6 : 5,
    borderBottom: isPrimary ? '2px solid #ccc' : '1px solid #e0e0e0',
    fontSize: isPrimary ? '1.05rem' : '0.95rem',
    fontWeight: isPrimary ? 700 : 600,
    color: isPrimary ? '#111' : isAi ? '#7a4000' : '#555',
    display: 'flex',
    alignItems: 'center',
    gap: 8,
  };

  return (
    <div style={wrapperStyle}>
      <h3 style={headingStyle}>
        {title}
        {isAi && (
          <span style={{
            background: '#fff3cd',
            color: '#7a4000',
            borderRadius: 4,
            padding: '1px 7px',
            fontSize: '0.72rem',
            fontWeight: 700,
            letterSpacing: '0.04em',
            textTransform: 'uppercase',
          }}>AI</span>
        )}
      </h3>
      {children}
    </div>
  );
}

function HypothesesTable({ hypotheses }: { hypotheses: TopHypothesis[] }) {
  if (!hypotheses || hypotheses.length === 0) {
    return <p style={{ color: '#666' }}>No hypotheses generated.</p>;
  }
  return (
    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.88rem' }}>
      <thead>
        <tr style={{ textAlign: 'left', borderBottom: '1px solid #e0e0e0' }}>
          <th style={{ padding: '5px 8px', fontWeight: 600, color: '#666' }}>Cause</th>
          <th style={{ padding: '5px 8px', width: 110, fontWeight: 600, color: '#666' }}>Confidence</th>
          <th style={{ padding: '5px 8px', fontWeight: 600, color: '#666' }}>Evidence</th>
        </tr>
      </thead>
      <tbody>
        {hypotheses.map((h, i) => {
          const ids = h.supportingEvidenceIds ?? [];
          const visibleIds = ids.slice(0, 2);
          const hiddenCount = ids.length - visibleIds.length;
          return (
            <tr key={i} style={{ borderBottom: '1px solid #f0f0f0' }}>
              <td style={{ padding: '5px 8px', lineHeight: 1.45, color: '#333' }}>{h.cause}</td>
              <td style={{ padding: '5px 8px' }}>
                <ConfidenceBar value={h.confidence} />
              </td>
              <td style={{ padding: '8px 10px' }}>
                {ids.length === 0 ? (
                  <span style={{ color: '#bbb' }}>—</span>
                ) : (
                  <span style={{ display: 'flex', flexWrap: 'wrap', gap: 4, alignItems: 'center' }}>
                    {visibleIds.map((id) => (
                      <span key={id} style={{
                        fontFamily: 'monospace',
                        fontSize: '0.72rem',
                        color: '#888',
                        background: '#f2f2f2',
                        borderRadius: 3,
                        padding: '1px 5px',
                      }}>{id}</span>
                    ))}
                    {hiddenCount > 0 && (
                      <span style={{ fontSize: '0.75rem', color: '#aaa' }}>+{hiddenCount} more</span>
                    )}
                  </span>
                )}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function ConfidenceBar({ value }: { value: number }) {
  const pct = Math.round(value * 100);
  const color = pct >= 60 ? '#2a7a2a' : pct >= 30 ? '#a07000' : '#888';
  return (
    <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <span
        style={{
          display: 'inline-block',
          width: pct,
          maxWidth: 70,
          height: 10,
          background: color,
          borderRadius: 3,
        }}
      />
      <span style={{ color, fontWeight: 'bold', fontSize: '0.85rem' }}>{pct}%</span>
    </span>
  );
}

function AiAssessmentsBlock({ assessments }: { assessments: AiAssessment[] }) {
  return (
    <div>
      <p style={{ margin: '0 0 12px', fontSize: '0.82rem', color: '#9a6000' }}>
        AI-generated, supplementary to deterministic ranking.
      </p>
      {assessments.map((a, i) => (
        <div
          key={i}
          style={{
            marginBottom: 10,
            padding: '10px 14px',
            background: '#fff',
            border: '1px solid #f0d070',
            borderRadius: 6,
          }}
        >
          <div style={{ fontWeight: 600, marginBottom: 4, display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', fontSize: '0.95rem' }}>
            {a.cause}
            <span style={{ fontWeight: 400, color: '#777', fontSize: '0.83rem' }}>
              {Math.round(a.plausibility * 100)}% plausibility
            </span>
          </div>
          <div style={{ fontSize: '0.88rem', color: '#444', lineHeight: 1.55 }}>{a.reason}</div>
        </div>
      ))}
    </div>
  );
}

function AiNextBestActionsBlock({ actions }: { actions: AiNextBestAction[] }) {
  return (
    <div>
      <p style={{ margin: '0 0 10px', fontSize: '0.82rem', color: '#9a6000' }}>
        Suggested next steps (advisory only).
      </p>
      <ol style={{ margin: 0, paddingLeft: 18, display: 'flex', flexDirection: 'column', gap: 10 }}>
        {actions.map((item, index) => (
          <li key={`${item.action}-${index}`} style={{ fontSize: '0.88rem', lineHeight: 1.5, color: '#333' }}>
            <div style={{ fontWeight: 600 }}>{item.action}</div>
            <div style={{ color: '#555', marginTop: 2 }}>{item.why}</div>
            <div style={{ marginTop: 4, display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
              <span style={{ fontSize: '0.78rem', color: '#666' }}>
                Expected signal: <strong>{item.expectedSignal}</strong>
              </span>
              <span style={{ fontSize: '0.78rem', color: '#666' }}>
                Confidence: <strong>{Math.round(item.confidence * 100)}%</strong>
              </span>
            </div>
            <div style={{ marginTop: 4, display: 'flex', flexWrap: 'wrap', gap: 4 }}>
              {item.evidenceIds.map((id) => (
                <span key={id} style={{
                  fontFamily: 'monospace',
                  fontSize: '0.72rem',
                  color: '#888',
                  background: '#f2f2f2',
                  borderRadius: 3,
                  padding: '1px 5px',
                }}>{id}</span>
              ))}
            </div>
          </li>
        ))}
      </ol>
    </div>
  );
}

function ConfidenceExplanationBlock({
  confidenceExplanation,
  topHypothesis,
}: {
  confidenceExplanation: ConfidenceExplanation;
  topHypothesis?: TopHypothesis;
}) {
  const [showBreakdown, setShowBreakdown] = React.useState(false);
  const deterministic = confidenceExplanation.contribution.deterministic;
  const ai = confidenceExplanation.contribution.ai;
  const confidenceBreakdown = topHypothesis?.confidenceBreakdown;
  const appliedBoosts = confidenceBreakdown?.boosts?.filter((b) => b.applied) ?? [];
  return (
    <div>
      <p style={{ marginTop: 0, marginBottom: 14, fontSize: '0.92rem', color: '#333', lineHeight: 1.55 }}>
        {confidenceExplanation.whyRankedHighest}
      </p>

      <div style={{ marginBottom: 14 }}>
        <p style={{ margin: '0 0 6px', fontWeight: 600, fontSize: '0.9rem', color: '#333' }}>Strongest evidence</p>
        {confidenceExplanation.strongestEvidence.length > 0 ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {confidenceExplanation.strongestEvidence.map((item) => (
              <div key={item.evidenceId} style={{
                padding: '7px 11px',
                background: '#f8f9fb',
                borderRadius: 5,
                border: '1px solid #e8e8e8',
                fontSize: '0.88rem',
              }}>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap', marginBottom: 3 }}>
                  <span style={{ fontWeight: 600, color: '#222' }}>{item.findingType}</span>
                  <span style={{ color: '#888', fontSize: '0.8rem' }}>{item.source}</span>
                  <span style={{ color: '#3a7a3a', fontSize: '0.8rem', fontWeight: 600 }}>{Math.round(item.score * 100)}%</span>
                  <span style={{ fontFamily: 'monospace', fontSize: '0.72rem', color: '#aaa', background: '#f0f0f0', borderRadius: 3, padding: '1px 5px' }}>{item.evidenceId}</span>
                </div>
                <div style={{ color: '#444', lineHeight: 1.45 }}>{item.summary}</div>
              </div>
            ))}
          </div>
        ) : (
          <p style={{ margin: '6px 0 0', color: '#888', fontSize: '0.88rem' }}>No strongest evidence items were linked.</p>
        )}
      </div>

      <div style={{ marginBottom: 14 }}>
        <p style={{ margin: '0 0 6px', fontWeight: 600, fontSize: '0.9rem', color: '#333' }}>Evidence gaps</p>
        <div style={{ fontSize: '0.88rem', color: '#444' }}>
          {confidenceExplanation.missingEvidence.aiIdentified.length > 0 ? (
            <div style={{ marginBottom: 4 }}>
              <span style={{ color: '#7a4000', fontWeight: 500 }}>AI-identified: </span>
              {confidenceExplanation.missingEvidence.aiIdentified.join(', ')}
            </div>
          ) : null}
          {confidenceExplanation.missingEvidence.collectionGaps.length > 0 ? (
            <div>
              <span style={{ fontWeight: 500 }}>Collection gaps: </span>
              {confidenceExplanation.missingEvidence.collectionGaps.map((gap, idx) => (
                <span key={`${gap.source}-${idx}`}>
                  {idx > 0 ? '; ' : ''}<strong>{gap.source}</strong>: {gap.reason}
                </span>
              ))}
            </div>
          ) : null}
          {confidenceExplanation.missingEvidence.aiIdentified.length === 0 &&
            confidenceExplanation.missingEvidence.collectionGaps.length === 0 && (
            <span style={{ color: '#aaa' }}>None identified</span>
          )}
        </div>
      </div>

      <div style={{ marginBottom: 10, fontSize: '0.88rem', color: '#555' }}>
        Deterministic score: {Math.round(deterministic.topConfidence * 100)}%
        {deterministic.runnerUpConfidence !== null && (
          <> (runner-up: {Math.round(deterministic.runnerUpConfidence * 100)}%)</>
        )}
        {ai.assessmentAvailable && ai.topHypothesisMatch && (
          <span style={{ marginLeft: 6, color: '#2a7a2a', fontWeight: 500 }}>· AI confirmed</span>
        )}
        {ai.assessmentAvailable && ai.topHypothesisMatch === false && (
          <span style={{ marginLeft: 6, color: '#a07000' }}>· AI diverged</span>
        )}
      </div>

      {confidenceBreakdown && (
        <div style={{ marginTop: 8 }}>
          <button
            onClick={() => setShowBreakdown((v) => !v)}
            style={{
              background: 'none',
              border: 'none',
              padding: 0,
              cursor: 'pointer',
              color: '#3659d9',
              fontSize: '0.85rem',
              fontWeight: 500,
            }}
          >
            {showBreakdown ? '▼ Hide' : '▶ Show'} confidence breakdown
          </button>
          {showBreakdown && (
            <div style={{ marginTop: 8, fontSize: '0.88rem', color: '#444', padding: '8px 12px', background: '#f8f9fb', borderRadius: 5, border: '1px solid #e8e8e8' }}>
              <div>Base: {Math.round(confidenceBreakdown.base * 100)}%</div>
              {appliedBoosts.length > 0 && (
                <ul style={{ margin: '4px 0 4px', paddingLeft: 18 }}>
                  {appliedBoosts.map((boost, idx) => (
                    <li key={`${boost.name}-${idx}`}>{formatBoostName(boost.name)} +{Math.round(boost.value * 100)}%</li>
                  ))}
                </ul>
              )}
              <div style={{ fontWeight: 600 }}>Final: {Math.round(confidenceBreakdown.final * 100)}%</div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function OperatorFocusBlock({ operatorFocus }: { operatorFocus: OperatorFocus }) {
  const primary = operatorFocus.primaryImplicatedResource;
  const topError = operatorFocus.topErrorPattern;
  const traceHint = operatorFocus.traceDependencyHint;

  return (
    <div style={{
      border: '1px solid #c8d8f8',
      background: '#f5f8ff',
      borderRadius: 8,
      padding: '16px 20px',
    }}>

      {/* Hero: Where to Look First */}
      <div style={{
        background: '#3659d9',
        color: '#fff',
        borderRadius: 6,
        padding: '12px 16px',
        marginBottom: 18,
      }}>
        <div style={{ fontSize: '0.75rem', fontWeight: 700, letterSpacing: '0.07em', textTransform: 'uppercase', opacity: 0.75, marginBottom: 4 }}>
          First inspection step
        </div>
        <div style={{ fontSize: '1rem', fontWeight: 600, lineHeight: 1.5 }}>
          {operatorFocus.whereToLookFirst}
        </div>
      </div>

      {/* Affected component */}
      <div style={{ marginBottom: 16 }}>
        <p style={{ margin: '0 0 3px', fontSize: '0.78rem', fontWeight: 700, letterSpacing: '0.05em', textTransform: 'uppercase', color: '#7a8aaa' }}>
          Most likely affected component
        </p>
        <p style={{ margin: 0, fontSize: '1rem', fontWeight: 600, color: '#1a1a1a' }}>
          {operatorFocus.mostLikelyAffectedComponent ?? <span style={{ color: '#aaa', fontWeight: 400 }}>Not clearly identified</span>}
        </p>
      </div>

      {/* Primary resource */}
      <div style={{ marginBottom: 16 }}>
        <p style={{ margin: '0 0 6px', fontSize: '0.78rem', fontWeight: 700, letterSpacing: '0.05em', textTransform: 'uppercase', color: '#7a8aaa' }}>
          Primary resource
        </p>
        {primary ? (
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginBottom: 4 }}>
              {primary.resourceType && (
                <span style={{
                  background: '#dde8ff',
                  color: '#2a4aaa',
                  borderRadius: 4,
                  padding: '2px 8px',
                  fontSize: '0.78rem',
                  fontWeight: 600,
                }}>{primary.resourceType}</span>
              )}
              <span style={{ fontSize: '0.97rem', fontWeight: 600, color: '#1a1a1a' }}>{primary.resourceName ?? 'unknown'}</span>
            </div>
            <div style={{ fontSize: '0.87rem', color: '#555', lineHeight: 1.5, marginBottom: 3 }}>{primary.summary}</div>
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              <MetaChip label="source" value={primary.source} />
            </div>
          </div>
        ) : (
          <p style={{ margin: 0, color: '#aaa', fontSize: '0.88rem' }}>Not identified from available evidence.</p>
        )}
      </div>

      {/* Top error */}
      <div>
        <p style={{ margin: '0 0 6px', fontSize: '0.78rem', fontWeight: 700, letterSpacing: '0.05em', textTransform: 'uppercase', color: '#7a8aaa' }}>
          Top error
        </p>
        {topError ? (
          <div>
            {/* Exception samples are PRIMARY — deduped with occurrence count */}
            {topError.exceptionSamples && topError.exceptionSamples.length > 0 ? (() => {
              const counts = new Map<string, number>();
              for (const s of topError.exceptionSamples!) {
                const normalized = s.trim();
                counts.set(normalized, (counts.get(normalized) ?? 0) + 1);
              }
              return (
                <div style={{ marginBottom: 8 }}>
                  {Array.from(counts.entries()).slice(0, 3).map(([sample, count]) => (
                    <div
                      key={sample}
                      style={{
                        fontFamily: 'monospace',
                        fontSize: '0.84rem',
                        color: '#c0000e',
                        background: '#fff5f5',
                        border: '1px solid #fcc',
                        padding: '6px 10px',
                        borderRadius: 4,
                        marginBottom: 5,
                        wordBreak: 'break-all',
                        lineHeight: 1.45,
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'flex-start',
                        gap: 8,
                      }}
                    >
                      <span style={{ flex: 1 }}>{sample}</span>
                      {count > 1 && (
                        <span style={{
                          flexShrink: 0,
                          fontSize: '0.72rem',
                          fontFamily: 'sans-serif',
                          color: '#a00',
                          background: '#ffe4e4',
                          borderRadius: 3,
                          padding: '1px 6px',
                          whiteSpace: 'nowrap',
                          alignSelf: 'center',
                        }}>×{count}</span>
                      )}
                    </div>
                  ))}
                </div>
              );
            })() : null}
            {/* Pattern is secondary metadata */}
            <div style={{ marginBottom: 6, fontSize: '0.85rem', color: '#555' }}>
              <span style={{ fontWeight: 500 }}>Pattern: </span>
              <span style={{ fontFamily: 'monospace', color: '#666' }}>{topError.pattern}</span>
            </div>
            {(topError.incidentCount != null || topError.baselineCount != null || topError.changeRatio != null) && (
              <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 6 }}>
                {topError.incidentCount != null && <StatChip label="Incident window" value={String(topError.incidentCount)} />}
                {topError.baselineCount != null && <StatChip label="Baseline" value={String(topError.baselineCount)} />}
                {topError.changeRatio != null && <StatChip label="Spike" value={`×${topError.changeRatio}`} highlight />}
              </div>
            )}
            {topError.source && (
              <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                <MetaChip label="source" value={topError.source} />
              </div>
            )}
          </div>
        ) : (
          <p style={{ margin: 0, color: '#aaa', fontSize: '0.88rem' }}>No specific error was linked.</p>
        )}
      </div>

      {traceHint && (
        <div style={{ marginTop: 16 }}>
          <p style={{ margin: '0 0 6px', fontSize: '0.78rem', fontWeight: 700, letterSpacing: '0.05em', textTransform: 'uppercase', color: '#7a8aaa' }}>
            Trace dependency signal
          </p>
          <div style={{
            background: '#eef5ff',
            border: '1px solid #c9dcff',
            borderRadius: 6,
            padding: '8px 10px',
          }}>
            <div style={{ color: '#1f3f8f', fontSize: '0.87rem', fontWeight: 600, marginBottom: 3 }}>
              {traceHint.summary}
            </div>
            <div style={{ fontSize: '0.82rem', color: '#4a5f8f', lineHeight: 1.45 }}>
              {traceHint.subsegmentName && (
                <span style={{ marginRight: 10 }}>
                  <strong>Subsegment:</strong> <span style={{ fontFamily: 'monospace' }}>{traceHint.subsegmentName}</span>
                </span>
              )}
              {traceHint.httpStatus != null && (
                <span style={{ marginRight: 10 }}><strong>HTTP:</strong> {traceHint.httpStatus}</span>
              )}
              {traceHint.resourceName && (
                <span><strong>Trace service:</strong> {traceHint.resourceName}</span>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function MetaChip({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <span style={{ fontSize: '0.75rem', color: '#888' }}>
      <span style={{ color: '#aaa' }}>{label}: </span>
      <span style={mono ? { fontFamily: 'monospace', color: '#999' } : {}}>{value}</span>
    </span>
  );
}

function StatChip({ label, value, highlight }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div style={{
      background: highlight ? '#fff2f0' : '#f0f4ff',
      border: `1px solid ${highlight ? '#ffb3a7' : '#d0dbff'}`,
      borderRadius: 5,
      padding: '3px 10px',
      fontSize: '0.82rem',
    }}>
      <span style={{ color: '#888', marginRight: 4 }}>{label}</span>
      <span style={{ fontWeight: 700, color: highlight ? '#c0000e' : '#2a4aaa' }}>{value}</span>
    </div>
  );
}

function formatBoostName(name: string): string {
  return name.split('_').join(' ');
}

function AtAGlanceBlock({
  topCause,
  topConfidencePct,
  aiPlausibilityPct,
  strongestEvidenceCount,
  supportingSources,
  incidentWindow,
  baselineWindow,
}: {
  topCause?: string;
  topConfidencePct: number | null;
  aiPlausibilityPct?: number | null;
  strongestEvidenceCount: number;
  supportingSources: string[];
  incidentWindow?: { start: string; end: string };
  baselineWindow?: { start: string; end: string };
}) {
  const sourceLabel = supportingSources.length > 0 ? supportingSources.join(', ') : 'No source breakdown';
  const incidentWindowLabel = incidentWindow
    ? `${formatDateTimeUtc(incidentWindow.start)} – ${formatDateTimeUtc(incidentWindow.end)}`
    : 'Not available';
  const baselineWindowLabel = baselineWindow
    ? `${formatDateTimeUtc(baselineWindow.start)} – ${formatDateTimeUtc(baselineWindow.end)}`
    : 'Not available';
  return (
    <>
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
          gap: 10,
          marginBottom: 10,
        }}
      >
        <GlanceCard label="Incident window" value={incidentWindowLabel} />
        <GlanceCard label="Baseline window" value={baselineWindowLabel} />
        <GlanceCard label="Top cause" value={topCause ?? 'Not identified'} />
        <GlanceCard
          label="Deterministic confidence"
          value={topConfidencePct != null ? `${topConfidencePct}%` : 'N/A'}
        />
        {aiPlausibilityPct != null && (
          <GlanceCard label="AI plausibility" value={`${aiPlausibilityPct}%`} />
        )}
        <GlanceCard label="Strongest evidence items" value={String(strongestEvidenceCount)} />
        <GlanceCard label="Supporting sources" value={sourceLabel} />
      </div>
    </>
  );
}

function GlanceCard({ label, value }: { label: string; value: string }) {
  return (
    <div
      style={{
        border: '1px solid #e5e8ef',
        borderRadius: 8,
        padding: '10px 12px',
        background: '#fafbff',
      }}
    >
      <div
        style={{
          marginBottom: 4,
          color: '#666',
          fontSize: '0.75rem',
          fontWeight: 700,
          letterSpacing: '0.04em',
          textTransform: 'uppercase',
        }}
      >
        {label}
      </div>
      <div style={{ color: '#111', fontSize: '0.9rem', fontWeight: 600, lineHeight: 1.4 }}>{value}</div>
    </div>
  );
}
