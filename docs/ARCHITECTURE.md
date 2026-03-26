# Architecture

## High-Level Architecture
```text
Manual Trigger (API Gateway)
        |
        v
Step Functions State Machine
   |
   +--> Scope Builder (Lambda)
   |      resolves scope from context snapshot captured at trigger time
   |
   +--> Parallel Evidence Collection (3 workers)
   |      +--> Metrics Worker (Lambda)
   |      +--> Logs Worker (Lambda)
   |      +--> Traces Worker (Lambda)
   |
   +--> Candidate Hypotheses Builder (Lambda)
   |
   +--> AI Hypothesis Evaluator (Lambda -> Bedrock)
   |
   +--> Final Report Builder (Lambda)
   |
   +--> Save to DynamoDB / S3
        |
        v
UI / Report page
```

Current trigger path is API-driven only. EventBridge alarm-triggered ingestion is not implemented.

## Incident Context Control Plane

Incident contexts are runtime-managed:

```text
React UI
  -> API Gateway (/incident-contexts)
  -> Contexts API Lambda
  -> DynamoDB IncidentContexts table (owner-scoped CRUD)
```

Investigations are triggered with `contextId`. Trigger resolves and snapshots the context into workflow input.

## Dynamic Discovery for Context Authoring

Incident-context management now includes dynamic discovery endpoints:

```text
React UI (Context Form)
  -> API Gateway (/incident-contexts/discovery/*)
  -> Discovery API Lambda
  -> AWS read/list APIs:
       - CloudWatch Logs (DescribeLogGroups)
       - CloudWatch Metrics (ListMetrics; namespace required)
       - X-Ray (GetTraceSummaries)
```

Scope boundary for this phase:
- discovery is for context authoring only (search/browse/select resources)
- context CRUD remains owner-scoped
- discovery auth is authenticated-user only
- investigation runtime is context-triggered (`contextId`)
- X-Ray discovery returns recently observed trace service names, not authoritative account inventory

## Why This Architecture
- bounded AI usage
- strong explainability
- easy parallelization
- easier debugging than full autonomous agentic loops

## Runtime Sequence

For the step-by-step execution sequence, see [EXECUTION_FLOW.md](EXECUTION_FLOW.md).

## Technology Stack
- **Backend:** Python 3.12 (all Lambda functions)
- **IaC:** AWS CDK (Python)
- **Frontend:** React + Vite
- See ADR-008 in `docs/DECISIONS.md` for rationale.

## Non-Goals

See "What this is NOT" in the main [README](../README.md).

## Worker Evidence Coverage

### Metrics Worker
Produces findings for any `metricDescriptor` in the context snapshot that shows a ≥1.5× change ratio versus the baseline window. Named metric types map to structured `findingType` values: `latency_spike`, `error_rate_spike`, `cpu_spike`, `memory_spike`, `db_connection_spike`, `throttle_spike`. API Gateway metrics (`4XXError`, `5XXError`) map to `error_rate_spike`; `Latency` and `IntegrationLatency` map to `latency_spike`. Recommended stats: `Sum` for error/count metrics, `p95`/`p99` for latency metrics.

### Logs Worker
Applies `PREDEFINED_PATTERNS` (ERROR, Exception, timeout, connection refused) to all log groups in context. For log groups under `/aws/apigateway/`, additionally applies `APIGW_PATTERNS` (HTTP 4XX, HTTP 5XX) which use CWL Insights regex to match the JSON-structured status field in API Gateway access logs. Evidence IDs are slot-stable by `(log_group_index × _TOTAL_PATTERN_SLOTS) + pattern_index` (ADR-007).

---
