// TypeScript interfaces matching schemas/final-report.schema.json
// and the GET /investigations/{id} status response from docs/API_CONTRACT.md.

export interface TopHypothesis {
  cause: string;
  confidence: number;
  supportingEvidenceIds?: string[];
  confidenceBreakdown?: ConfidenceBreakdown;
}

export interface ConfidenceBoost {
  name: string;
  value: number;
  applied: boolean;
}

export interface ConfidenceBreakdown {
  base: number;
  boosts: ConfidenceBoost[];
  totalBeforeCap: number;
  cap: number;
  final: number;
}

export interface AiAssessment {
  cause: string;
  plausibility: number;
  reason: string;
}

export interface AiNextBestAction {
  action: string;
  why: string;
  evidenceIds: string[];
  expectedSignal: string;
  confidence: number;
}

export interface WorkerError {
  source: string;
  reason: string;
}

export interface IncidentWindow {
  start: string;
  end: string;
}

export interface ConfidenceStrongestEvidenceItem {
  evidenceId: string;
  source: string;
  findingType: string;
  score: number;
  summary: string;
}

export interface ConfidenceCollectionGap {
  source: string;
  reason: string;
}

export interface ConfidenceMissingEvidence {
  aiIdentified: string[];
  collectionGaps: ConfidenceCollectionGap[];
}

export interface ConfidenceDeterministicContribution {
  topConfidence: number;
  runnerUpConfidence: number | null;
  confidenceDelta: number | null;
}

export interface ConfidenceAiContribution {
  assessmentAvailable: boolean;
  topHypothesisMatch: boolean | null;
  plausibility: number | null;
  reason: string | null;
  unavailableReason?: string | null;
}

export interface ConfidenceExplanation {
  topHypothesisCause: string;
  whyRankedHighest: string;
  strongestEvidence: ConfidenceStrongestEvidenceItem[];
  missingEvidence: ConfidenceMissingEvidence;
  contribution: {
    rankingDriver: 'deterministic';
    deterministic: ConfidenceDeterministicContribution;
    ai: ConfidenceAiContribution;
  };
}

export interface OperatorFocusPrimaryResource {
  evidenceId: string;
  source: string;
  resourceType: string | null;
  resourceName: string | null;
  findingType: string | null;
  score: number | null;
  summary: string;
}

export interface OperatorFocusTopErrorPattern {
  evidenceId: string | null;
  source: string | null;
  findingType: string | null;
  pattern: string;
  summary: string;
  incidentCount: number | null;
  baselineCount: number | null;
  changeRatio: number | null;
  exceptionSamples?: string[];
}

export interface OperatorFocusTraceDependencyHint {
  evidenceId: string | null;
  resourceName: string | null;
  summary: string;
  score: number | null;
  subsegmentName: string | null;
  namespace: string | null;
  httpStatus: number | null;
  occurrences: number | null;
  sampledTraceCount: number | null;
}

export interface OperatorFocus {
  mostLikelyAffectedComponent: string | null;
  primaryImplicatedResource: OperatorFocusPrimaryResource | null;
  topErrorPattern: OperatorFocusTopErrorPattern | null;
  traceDependencyHint?: OperatorFocusTraceDependencyHint | null;
  whereToLookFirst: string;
}

export interface FinalReport {
  incidentId: string;
  summary: string;
  topHypotheses: TopHypothesis[];
  evidenceHighlights: string[];
  incidentWindow?: IncidentWindow;
  baselineWindow?: IncidentWindow;
  workerErrors?: WorkerError[];
  // Additive AI field — may be absent if AI evaluation did not run or returned empty.
  aiAssessments?: AiAssessment[];
  // Additive optional AI guidance: operator next investigative actions.
  aiNextBestActions?: AiNextBestAction[];
  // Additive bounded explanation field — may be absent for older reports.
  confidenceExplanation?: ConfidenceExplanation;
  // Additive operator-oriented section derived from deterministic evidence.
  operatorFocus?: OperatorFocus;
}

export interface InvestigationStatus {
  incidentId: string;
  status: 'RUNNING' | 'COMPLETED' | 'FAILED';
  service?: string;
  region?: string;
  windowStart?: string;
  windowEnd?: string;
  createdAt?: string;
  updatedAt?: string;
}
