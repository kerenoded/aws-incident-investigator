# Security

This document describes the current security posture of the Incident Investigator system, what controls are in place, and what is intentionally deferred.

---

## Current protection level

### CORS origin restriction

API Gateway and both API Lambda functions return a configurable `Access-Control-Allow-Origin` header instead of `*`.

The allowed origin is set at deploy time via CDK context:

```bash
cdk deploy -c allowed_cors_origin=https://your-deployed-frontend-url
```

The default when no context value is provided is `http://localhost:5173` (the Vite dev server default).

**Important: CORS is not authentication.**  
CORS headers instruct browsers to block cross-origin requests from unauthorized origins, but they do not protect the API from direct HTTP access by non-browser clients (curl, scripts, other servers). Any client that does not enforce CORS — which includes all non-browser HTTP clients — can still reach the API endpoints freely. Do not rely on CORS as your primary access control mechanism.

### S3 artifacts bucket

The S3 bucket already has:
- `BlockPublicAccess.BLOCK_ALL` — no public access of any kind
- `enforce_ssl=True` — HTTP requests are denied; only HTTPS allowed

### Bedrock IAM scope

The `ai_hypothesis_evaluator` Lambda's Bedrock permissions are scoped to the specific profile/model resources used by the evaluator:

```
arn:aws:bedrock:{region}:{account}:inference-profile/eu.amazon.nova-micro-v1:0
arn:aws:bedrock:*::foundation-model/amazon.nova-micro-v1:0
```

If the model ID changes, update the ARN in `infra/incident_investigator_stack.py` and the `BEDROCK_MODEL_ID` env var in `infra/lambda_constructs.py` together.

### DynamoDB and S3 IAM grants

Lambda functions are granted access to DynamoDB and S3 via CDK's `grant_*` methods, which scope permissions to the specific table ARN and bucket ARN respectively. No Lambda has broad DynamoDB or S3 permissions.

### IAM reductions

The following IAM tightenings were applied:

- Metrics worker: removed `cloudwatch:GetMetricData` (current implementation uses `GetMetricStatistics`).
- Logs worker: removed `logs:StopQuery` (current implementation uses `StartQuery` + `GetQueryResults`).
- API Lambda S3 read narrowed to:
  - `arn:aws:s3:::<artifacts-bucket>/investigations/*/final-report.json`

---

## API authentication and authorization

See [API_CONTRACT.md](API_CONTRACT.md) for full authentication and authorization rules, including Cognito token requirements, service-group authorization model, and ownership enforcement for incident contexts.

---

## Intentionally non-production items

| Item | Status | Notes |
|---|---|---|
| **Fine-grained API authorization** | Partially implemented | Incident contexts are owner-managed, and investigation runtime authorization uses Cognito groups (`svc-<serviceName>`) derived from the selected context; broader RBAC is deferred |
| **WAF** | Deferred | ~$5–10/month base cost per WebACL; disproportionate for demo |
| **VPC / private networking** | Deferred | Significant scope and NAT gateway cost |
| **CloudWatch Logs IAM (`logs:*`)** | `resources=["*"]` | Log group ARNs are determined dynamically from incident context snapshots; cannot be statically scoped without enumerating all possible groups at deploy time |
| **CloudWatch Metrics IAM** | `resources=["*"]` | AWS does not support resource-level restrictions for `GetMetricStatistics` |
| **DynamoDB TTL deletion timing** | Best-effort, asynchronous | TTL is now populated on trigger writes; DynamoDB TTL cleanup is not immediate |
| **No deployment pipeline in this repo** | Intentionally excluded | CI exists for test/build/synth validation only; deployments are handled outside this repository |

---

## Summary

| Control | State |
|---|---|
| CORS origin restriction | Configurable; no wildcard in production deploy |
| S3 bucket public access | Blocked |
| S3 HTTPS enforcement | Enforced |
| Bedrock IAM scope | Specific inference-profile and foundation-model ARNs |
| DynamoDB / S3 Lambda grants | Scoped to specific resource ARNs |
| API authentication | Cognito User Pool authorizer (access-token bearer auth) |
| API authorization model | Context ownership for context CRUD; service-group authorization for investigation runtime |
| WAF | Not deployed |
| VPC | Not deployed |

---
