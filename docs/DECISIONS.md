# Architecture Decisions

## ADR-001: Incident-driven workflow, not continuous crawling
We investigate only after an alert or manual trigger.  
We do not continuously scan all logs/telemetry across the account.

## ADR-002: Deterministic workers, bounded AI
Metrics, logs, and traces workers are deterministic.  
GenAI is used as a bounded advisory layer: it evaluates competing hypotheses, assigns comparative plausibility with reasoning, and identifies missing evidence.

## ADR-003: Scope before retrieval
Every investigation must first define relevant investigation scope, time window, and data sources.  
This is required for cost control, latency, and explainability.

## ADR-004: Baseline comparison is part of abnormality detection
Workers should compare incident behavior to a recent healthy/baseline window where relevant.  
Default baseline rule: use the immediately preceding window with equal duration.  
This rule should remain configurable later per service or incident type.

## ADR-007: Stable evidence IDs are required
Each finding should include a stable `evidenceId` so hypotheses, AI outputs, and final reports can reference evidence consistently.

Traces worker ID scheme: 3 finding slots per trace service (formula: `service_idx * 3 + slot + 1`).
- Slot 0 → `trace_latency_spike`
- Slot 1 → `latency_dominant_segment`
- Slot 2 → `trace_dependency_fault`

For a single-service incident these map to `ev-traces-001`, `ev-traces-002`, `ev-traces-003`.
IDs for multi-service (idx > 0) incidents shift from the previous 2-slot scheme; this is acceptable at the current MVP scope and should be considered when stored results are replayed.

## ADR-008: Technology stack
- **Lambda / backend:** Python 3.12 — matches boto3 and Bedrock SDK, minimal setup and fast iteration.
- **Infrastructure as Code:** AWS CDK (Python) — typed constructs, matches backend language.
- **Frontend:** Plain React with Vite — minimal setup, fast build, sufficient for a report UI.
- **Rationale:** single-language backend simplifies shared models and reduces context-switching. CDK in Python keeps IaC and business logic in the same ecosystem. Vite avoids SSR complexity that a report page doesn't need.

## ADR-009: Worker partial-failure handling
Workers must always return the standard envelope: `{ incidentId, source, findings: [], errors: [] }`.

Rules:
- If a telemetry source is missing, disabled, or errors out, the worker returns `findings: []` and populates `errors` with `{ source, reason }`.
- Workers must not throw unhandled exceptions for missing/empty data sources.
- Step Functions uses `Catch` on each parallel worker branch. On Lambda failure, the orchestration substitutes `{ incidentId, source, findings: [], errors: [{ source, reason: "worker_failed" }] }` via a fallback state.
- The workflow **continues** unless the Scope Builder or Final Report Builder fails — those are critical.
- Downstream components (Candidate Hypotheses Builder, AI Evaluator) receive all worker outputs including errors, and must account for incomplete evidence.

## ADR-010: Scope Builder owns context-to-scope transformation
Scope Builder derives investigation scope directly from `contextSnapshot` captured at trigger time.  
There is no separate Context Worker.

Rationale: context metadata is a deterministic input to scoping, not independent evidence.

## ADR-011: State transfer strategy (MVP)
Step Functions keeps investigation data in-state between steps (full payload transfer).

Rationale:
- lowest-risk approach for MVP scope
- aligns with current implementation across scope/hypothesis/report stages
- avoids a broad workflow redesign while preserving deterministic + bounded-AI flow

Guardrail:
- Step Functions state payload limit is 256 KB. Payload growth must be monitored.

Future trigger for change:
- move to S3-pointer passing when payload size approaches limits (especially before expanding Trace Worker output volume).

## ADR-013: Final report remains bounded to active investigation outputs

Final report content is restricted to:
- deterministic hypotheses
- bounded evidence highlights
- bounded confidence explanation
- optional bounded AI assessments

## ADR-016: Incident-context control plane is owner-scoped

Runtime-managed incident context CRUD is the control-plane capability for trigger inputs.

Rules:
- incident contexts are stored in DynamoDB (`IncidentContexts`)
- owner-only access:
  - creator owns the context
  - creator can list/get/update own contexts
  - no admin override group
- keep schema practical and permissive where useful:
  - `metricDescriptors` is an object list (no over-modeling)

Investigation trigger/runtime uses `contextId` and a captured immutable `contextSnapshot`.

## ADR-017: Dynamic discovery APIs support context authoring

A minimal dynamic discovery layer supports building incident contexts from real AWS resources.

Rules:
- discovery endpoints are under `/incident-contexts/discovery/*`
- discovery supports CloudWatch log groups, CloudWatch metrics, and X-Ray service names
- discovery access is authenticated-user only (no service-group authz for discovery)
- context CRUD remains owner-only as defined in ADR-016
- investigation runtime authorization uses service-group checks derived from the selected context snapshot

Scope boundaries:
- no runbooks/history enrichment surface
- no AI-based discovery/recommendation
- metrics discovery is intentionally narrow (`namespace` required)
- X-Ray discovery is explicitly "recently observed services", not account-wide inventory

## ADR-018: Exception-class label enrichment is deterministic, display-only, and general

The `_rule_application_error_surge` hypothesis rule enriches the `cause` label with the dominant
exception class found in `details.exceptionSamples` (e.g. `"application error surge — ValidationException"`).

Rules:
- Extraction uses a general regex against the AWS SDK parenthesised format `(ExceptionClass)` with a
  bare-prefix fallback. No hardcoded per-service or per-operation rules.
- The most-frequent matched class across all findings and samples is used.
- Label enrichment is **display-only**: it does not affect confidence scores, boosts, or evidence IDs.
- If no exception class can be extracted, the label falls back to the plain `"application error surge"`
  with no behaviour change.
- Enrichment must not invent details not present in the actual sampled log messages.

---

## ADR-019: Three distinct scoring concepts in the hypothesis builder

The hypothesis system uses three related but distinct numeric measures. Conflating them causes confusion about what each number represents.

**Evidence score** (`finding.score`, range 0–1)
- Produced by each deterministic worker.
- Measures *how strongly the worker considers this finding anomalous* relative to baseline.
- Calculated independently per finding from raw telemetry (e.g. change ratio capped at 1.0).
- Does not imply any causal direction.

**Hypothesis confidence** (`hypothesis.confidence`, range 0–1)
- Produced by the Candidate Hypotheses Builder.
- Measures *how strongly the deterministic evidence pattern supports this hypothesis*.
- Computed from named constants (base + boosts + cap). All constants live in `hypotheses_builder.py`.
- These are **heuristic weights, not calibrated probabilities**. They reflect engineering intuition about evidential strength and should be re-evaluated against real data over time.

**AI plausibility** (`hypothesis.aiAssessment`, range 0–1)
- Produced by the AI Hypothesis Evaluator (Bedrock).
- Measures *how plausible the AI finds this hypothesis* given all structured evidence.
- Interprets the evidence bundle and may diverge from `confidence`.
- Should be treated as advisory, not authoritative.

Rules:
- Rule functions in `hypotheses_builder.py` must not read individual evidence scores to compute confidence. The score constants block (named constants, not magic floats) is the single source of truth for confidence computation.
- Worker-specific `findingType` string knowledge is confined to `signals.py`. Rule functions consume `NormalizedSignals` only.
- Enrichments that are display-only (e.g. exception class label, resource finding types) must not affect any numeric score.
