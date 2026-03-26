# API Contract

REST API served by API Gateway + Lambda. Consumed by the React frontend.

## Authentication

All API endpoints require a valid Cognito bearer token.

- Mechanism: API Gateway Cognito User Pools Authorizer
- Token type sent by frontend: **Cognito Access Token**
- Header format:

```http
Authorization: Bearer <access_token>
```

## Authorization

Authorization uses context ownership for context management and service-group checks derived from the selected context during investigation runtime.

- Required group naming convention: `svc-<serviceName>`
  - Example: `svc-checkout-service`
- Group provisioning and user membership assignment are currently manual operational steps.
- API behavior:
  - `POST /investigations` requires ownership of `contextId` and service-group access to the context-resolved service
  - `GET /investigations/{incidentId}` and `GET /investigations/{incidentId}/report` require access to the investigation's stored `service`
  - `POST/GET/PATCH /incident-contexts*` are owner-scoped: creator can list/get/update only contexts they created
  - `GET /incident-contexts/discovery/*` requires authenticated user only (no service-group authorization)

## Endpoints

### 1. Trigger Investigation

```
POST /investigations
```

Primary request body (context-first):

```json
{
  "contextId": "ctx-4f7a1e9d2c3b",
  "signalType": "latency_spike",
  "windowStart": "2024-01-15T10:00:00Z",
  "windowEnd": "2024-01-15T10:30:00Z"
}
```

Validation notes:
- request must include `contextId`
- trigger loads context from DynamoDB, validates owner access, and snapshots context into workflow input
- unknown request fields are rejected

Optional header for safe retries:

```http
Idempotency-Key: <client-generated-key>
```

When provided, duplicate POST retries with the same key for the same trigger scope
(`context:<contextId>`)
return the already-associated `incidentId` and do not start a second workflow execution.

**Response (202 Accepted):**
```json
{
  "incidentId": "inc-20240115-abc123",
  "status": "RUNNING"
}
```

**Response (202 Accepted, duplicate idempotent retry):**
```json
{
  "incidentId": "inc-20240115-abc123",
  "status": "RUNNING",
  "duplicateRequest": true
}
```

**Behavior:** Validates input, creates a DynamoDB record, starts the Step Functions execution, returns immediately.

Validation at trigger boundary currently enforces:
- required fields (`contextId`, `signalType`, `windowStart`, `windowEnd`)
- non-empty strings for required fields
- ISO 8601 timestamp format for `windowStart` / `windowEnd` with explicit UTC timezone
- `windowEnd > windowStart`
- optional fields (`environment`, `severity`) must be non-empty strings when provided
- `Idempotency-Key` (when provided) must be a non-empty string up to 128 chars

Unknown extra request fields are rejected.

---

### 2. Get Investigation Status

```
GET /investigations/{incidentId}
```

**Response (202 Accepted, when investigation is still running):**
```json
{
  "incidentId": "inc-20240115-abc123",
  "status": "RUNNING",
  "service": "checkout-service",
  "region": "eu-west-1",
  "windowStart": "2024-01-15T10:00:00Z",
  "windowEnd": "2024-01-15T10:30:00Z",
  "createdAt": "2024-01-15T10:35:00Z",
  "updatedAt": "2024-01-15T10:36:00Z"
}
```

**Response (200 OK, when investigation is no longer running):** same shape as above, with `status` set to `COMPLETED` or `FAILED`.

**Status values:** `RUNNING`, `COMPLETED`, `FAILED`

---

### 3. Get Full Report

```
GET /investigations/{incidentId}/report
```

**Response (200 OK):** Returns the full report JSON as defined in `schemas/final-report.schema.json`.

Additive notes:
- report payload may include optional `confidenceExplanation` (bounded machine-readable explanation of top-hypothesis ranking, strongest evidence, missing evidence, and deterministic vs AI contribution).
- report payload may include optional `operatorFocus` (deterministic operator-focused guidance: most likely affected component, primary implicated resource, top error pattern when available, and where to look first).

**Response (404):** Investigation not found.

**Response (202 Accepted):** Investigation exists but report is not yet available.
```json
{
  "incidentId": "inc-20240115-abc123",
  "status": "RUNNING"
}
```

**Behavior:** Reads the report JSON from S3 using the key stored in DynamoDB. If the investigation exists but is still running, returns 202 with the current status instead of 404.

Current runtime flow note:
- investigation runtime path is: Scope Builder → parallel evidence workers → Candidate Hypotheses Builder → AI Hypothesis Evaluator → Final Report Builder.

---

### 4. Incident Context CRUD

```
POST /incident-contexts
GET /incident-contexts
GET /incident-contexts/{contextId}
PATCH /incident-contexts/{contextId}
DELETE /incident-contexts/{contextId}
```

Purpose:
- Manage runtime incident contexts used to scope investigations.
- Contexts are the active control-plane model for creating investigations.

`POST /incident-contexts` request body (minimal):
```json
{
  "name": "Payments Production",
  "description": "Manual context for payment incidents",
  "region": "eu-west-1",
  "logGroups": ["/aws/lambda/payment-service"],
  "metricDescriptors": [{"namespace": "AWS/Lambda", "metricName": "Duration"}],
    "xrayServices": ["checkout-service"]
}
```

`POST /incident-contexts` response (201):
```json
{
  "contextId": "ctx-4f7a1e9d2c3b",
  "name": "Payments Production",
  "description": "Manual context for payment incidents",
  "region": "eu-west-1",
  "logGroups": ["/aws/lambda/payment-service"],
  "metricDescriptors": [{"namespace": "AWS/Lambda", "metricName": "Duration"}],
    "xrayServices": ["checkout-service"],
  "createdAt": "2026-03-19T09:00:00Z",
  "updatedAt": "2026-03-19T09:00:00Z",
  "createdBy": "<caller-sub>",
  "updatedBy": "<caller-sub>"
}
```

`GET /incident-contexts` response (200):
```json
{
  "items": [
    {
      "contextId": "ctx-4f7a1e9d2c3b",
      "name": "Payments Production",
      "description": "Manual context for payment incidents",
      "region": "eu-west-1",
      "logGroups": ["/aws/lambda/payment-service"],
      "metricDescriptors": [],
      "xrayServices": ["checkout-service"],
      "createdAt": "2026-03-19T09:00:00Z",
      "updatedAt": "2026-03-19T09:10:00Z",
      "createdBy": "<caller-sub>",
      "updatedBy": "<caller-sub>"
    }
  ]
}
```

`PATCH /incident-contexts/{contextId}` supports partial updates for:
- `name`, `description`, `region`, `logGroups`, `metricDescriptors`, `xrayServices`

`DELETE /incident-contexts/{contextId}` deletes a context owned by the caller.
- Returns `204 No Content` on success.
- Returns `404 NOT_FOUND` if the context does not exist or the caller is not the owner.

Security behavior:
- caller must be authenticated
- caller can only access contexts where `ownerSub == caller sub`
- unauthorized get/update/delete returns `404 NOT_FOUND` to avoid resource enumeration

Current model note:
- contexts are active trigger inputs for investigations (`contextId`)

---

### 5. Incident Context Discovery APIs

```
GET /incident-contexts/discovery/log-groups
GET /incident-contexts/discovery/metrics
GET /incident-contexts/discovery/xray/services
```

Purpose:
- provide dynamic runtime resource search for building incident contexts
- avoid closed predefined lists and local config files as primary workflow
- investigation runtime is context-triggered in this model

Common query parameters:
- `region` (required)
- `q` (optional; case-insensitive filter)
- `pageSize` (optional; default 25, max 50)
- `nextToken` (optional)

Common response envelope:
```json
{
  "items": [],
  "nextToken": null
}
```

Endpoint-specific parameters:
- `/metrics`: `namespace` (required)
- `/xray/services`: `lookbackMinutes` (optional; 5..1440, default 180)

Examples:

`GET /incident-contexts/discovery/log-groups?region=eu-west-1&q=payment&pageSize=25`
```json
{
  "items": [
    { "logGroupName": "/aws/lambda/payment-service", "arn": "arn:aws:logs:..." }
  ],
  "nextToken": "..."
}
```

`GET /incident-contexts/discovery/metrics?region=eu-west-1&namespace=AWS/Lambda&q=CPU`
```json
{
  "items": [
    {
      "namespace": "AWS/Lambda",
      "metricName": "Duration",
      "dimensions": [{ "name": "ClusterName", "value": "main" }]
    }
  ],
  "nextToken": "..."
}
```

`GET /incident-contexts/discovery/xray/services?region=eu-west-1&lookbackMinutes=180`
```json
{
  "items": [
    { "serviceName": "checkout-service" },
    { "serviceName": "auth-service" }
  ],
  "nextToken": "..."
}
```

X-Ray note:
- this endpoint returns service names observed in recent trace summaries within the lookback window.
- it is not an authoritative account-wide service inventory.

---

## Error Responses

All errors follow this shape:
```json
{
  "error": "NOT_FOUND",
  "message": "Investigation inc-xxx not found"
}
```

| HTTP Status | Error Code | When |
|---|---|---|
| 400 | `VALIDATION_ERROR` | Missing or invalid request fields |
| 401 | *(authorizer response)* | Missing, expired, or invalid Cognito access token |
| 404 | `NOT_FOUND` | Investigation ID does not exist |
| 202 | *(no error envelope)* | Investigation exists but is still running; response body is `{ "incidentId", "status" }` |
| 500 | `INTERNAL_ERROR` | Unexpected failure |

For incident-context endpoints:
- 400 validation failures use the same `{ error, message }` envelope
- 404 is returned both for missing contexts and non-owner access

Security note:
- For read endpoints (`GET /investigations/{incidentId}` and `/report`), unauthorized and missing incidents intentionally return the same `404 NOT_FOUND` envelope to avoid incident-ID enumeration across services.

## Implementation Notes
- API Gateway REST API with Lambda proxy integration
- One Lambda handles all three routes (or split per route — team preference)
- Input validation at the Lambda boundary (not API Gateway models)
- CORS enabled for the frontend origin
- Cognito authentication is enforced at API Gateway using a User Pool authorizer
- Authorization is enforced in Lambda handlers using Cognito `cognito:groups` claims and the `svc-<serviceName>` convention

## Frontend Usage
- On submit: `POST /investigations` → receive `incidentId` → navigate to status page
- Status page: poll `GET /investigations/{id}` every few seconds until `COMPLETED`
- On complete: `GET /investigations/{id}/report` → render the report

---
