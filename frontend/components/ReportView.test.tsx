/**
 * Tests for the Investigation Overview glance cards in ReportView.
 */

import { cleanup, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import ReportView from './ReportView';
import type { FinalReport } from '../types/report';

afterEach(cleanup);

const BASE_REPORT: FinalReport = {
  incidentId: 'inc-001',
  summary: 'Test summary',
  topHypotheses: [
    {
      cause: 'application error surge',
      confidence: 0.6,
      supportingEvidenceIds: [],
      confidenceBreakdown: { base: 0.25, boosts: [], totalBeforeCap: 0.6, cap: 0.7, final: 0.6 },
    },
  ],
  evidenceHighlights: [],
  incidentWindow: { start: '2026-03-22T18:21:00Z', end: '2026-03-22T18:24:00Z' },
  baselineWindow: { start: '2026-03-22T18:18:00Z', end: '2026-03-22T18:21:00Z' },
  confidenceExplanation: {
    topHypothesisCause: 'application error surge',
    whyRankedHighest: 'Test ranking reason',
    strongestEvidence: [],
    missingEvidence: { aiIdentified: [], collectionGaps: [] },
    contribution: {
      rankingDriver: 'deterministic',
      deterministic: { topConfidence: 0.6, runnerUpConfidence: 0.35, confidenceDelta: 0.25 },
      ai: {
        assessmentAvailable: true,
        topHypothesisMatch: true,
        plausibility: 0.75,
        reason: 'Test AI reason',
        unavailableReason: null,
      },
    },
  },
};

describe('ReportView — AI plausibility glance card', () => {
  it('shows AI plausibility card when AI assessed the top hypothesis', () => {
    render(<ReportView report={BASE_REPORT} />);
    expect(screen.getByText('AI plausibility')).toBeDefined();
    expect(screen.getByText('75%')).toBeDefined();
  });

  it('does not show AI plausibility card when assessmentAvailable is false', () => {
    const report: FinalReport = {
      ...BASE_REPORT,
      confidenceExplanation: {
        ...BASE_REPORT.confidenceExplanation!,
        contribution: {
          ...BASE_REPORT.confidenceExplanation!.contribution,
          ai: {
            assessmentAvailable: false,
            topHypothesisMatch: null,
            plausibility: null,
            reason: null,
          },
        },
      },
    };
    render(<ReportView report={report} />);
    expect(screen.queryByText('AI plausibility')).toBeNull();
  });

  it('does not show AI plausibility card when AI evaluated a different top hypothesis', () => {
    const report: FinalReport = {
      ...BASE_REPORT,
      confidenceExplanation: {
        ...BASE_REPORT.confidenceExplanation!,
        contribution: {
          ...BASE_REPORT.confidenceExplanation!.contribution,
          ai: {
            assessmentAvailable: true,
            topHypothesisMatch: false,
            plausibility: 0.5,
            reason: 'Different hypothesis matched',
          },
        },
      },
    };
    render(<ReportView report={report} />);
    expect(screen.queryByText('AI plausibility')).toBeNull();
  });

  it('does not show AI plausibility card when confidenceExplanation is absent', () => {
    const { confidenceExplanation: _, ...reportWithoutExplanation } = BASE_REPORT;
    render(<ReportView report={reportWithoutExplanation as FinalReport} />);
    expect(screen.queryByText('AI plausibility')).toBeNull();
  });
});
