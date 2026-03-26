# Execution Flow

## Step-by-Step
1. An incident trigger arrives from a manual/API request.
2. The request is normalized into an incident payload (see `schemas/incident.schema.json`).
3. Step Functions starts the investigation workflow.
4. Scope Builder selects the relevant time window, baseline window, and evidence sources for the investigation scope.
5. Three evidence workers run in parallel: Metrics, Logs, Traces. Each returns the standard worker output envelope (see `schemas/worker-output.schema.json`). Workers that encounter missing or unavailable sources return `findings: []` with an `errors` entry (see ADR-009). For `ERROR` and `Exception` log patterns with incident-window hits, the Logs Worker also issues a secondary sample query and attaches up to 3 message samples (≤200 chars each) to the finding's `details.exceptionSamples`. Sampling failures are silent and do not block the finding.
6. Worker outputs are collected. Step Functions uses `Catch` on each parallel branch so one worker failure does not abort the workflow.
7. Candidate Hypotheses Builder creates a shortlist of plausible causes from the combined evidence.
8. AI Hypothesis Evaluator reviews the hypotheses and evidence via Bedrock.
9. Final Report Builder creates the incident report payload (see `schemas/final-report.schema.json`).
10. Results are stored in DynamoDB/S3.
11. On `GET /investigations/{incidentId}/report`, API returns the stored report.
12. UI renders the final report payload, including an operator-focused section (`operatorFocus`) derived from deterministic evidence.

> Note: EventBridge/CloudWatch Alarm-triggered ingestion is not implemented. The current trigger path is API-driven only.

## Implementation notes
- Step Functions passes full investigation payloads between states (not S3 pointers).
- `Scope Builder`, `Candidate Hypotheses Builder`, and `Final Report Builder` write artifacts to S3 (`scope.json`, `hypotheses.json`, `final-report.json`).
- Runtime Step Functions path is: Scope Builder → GatherEvidence → AggregateWorkerOutputs → CandidateHypothesesBuilder → AIHypothesisEvaluator → FinalReportBuilder.
- `Final Report Builder` adds deterministic operator-focused guidance (primary implicated resource, top error pattern when available, and where-to-look-first guidance) derived from supporting evidence only.

---
