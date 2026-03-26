"""
Logs Worker — core logic.

Runs targeted CloudWatch Logs Insights queries against scoped log groups,
comparing error pattern counts between incident and baseline windows.
Returns structured findings for significant pattern spikes.

- ADR-002: deterministic worker, no AI
- ADR-004: baseline comparison (preceding equal-duration window)
- ADR-007: evidenceId stable by (log group × pattern) position (1-indexed)
- ADR-009: per-(log_group, pattern) error handling; errors go to errors[],
           processing continues

Logging:
    Emits deterministic breadcrumbs at start, key decision points, and
    completion to make evidence collection debugging easier in CloudWatch logs.
"""

import time
import logging
from datetime import datetime

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Minimum change ratio to produce a finding (1.5 = 50% increase over baseline).
ANOMALY_THRESHOLD = 1.5

# Maximum poll iterations when waiting for a CWL Insights query to complete.
# Each iteration sleeps 1 second; this bounds the per-query wait to ~30 seconds.
MAX_POLL_ITERATIONS = 30

# Hardcoded error patterns for MVP.
# Each entry:
#   name:              human-readable label used in finding summary and details
#   filter_expression: complete CWL Insights boolean expression placed after
#                      'filter' in the query — must be a valid predicate, e.g.
#                      '@message like /pattern/' (not a bare string or field name).
PREDEFINED_PATTERNS = [
    {"name": "ERROR", "filter_expression": '@message like "ERROR"'},
    {"name": "Exception", "filter_expression": '@message like "Exception"'},
    {"name": "timeout", "filter_expression": '@message like "timeout"'},
    {"name": "connection refused", "filter_expression": '@message like "connection refused"'},
]

# Patterns for which we collect a small sample of concrete exception messages.
# Only ERROR and Exception are likely to carry actionable exception text.
_SAMPLE_PATTERNS = {"ERROR", "Exception"}
# Maximum number of exception message samples to collect (incident window only).
_MAX_EXCEPTION_SAMPLES = 3
# Maximum characters per sample — keeps evidence payloads tightly bounded.
_MAX_EXCEPTION_MSG_LEN = 200

# Patterns applied exclusively to API Gateway log groups (JSON-structured access logs).
# These use CWL Insights regex (=~) to match the status field rather than plain-text
# keywords. Applied only when _is_apigw_log_group() returns True.
APIGW_PATTERNS = [
    {
        "name": "HTTP 4XX",
        "filter_expression": r'@message =~ /"status"\s*:\s*"?4[0-9]{2}/',
    },
    {
        "name": "HTTP 5XX",
        "filter_expression": r'@message =~ /"status"\s*:\s*"?5[0-9]{2}/',
    },
]

# Total evidence ID slots reserved per log group.
# Every log group reserves slots for both PREDEFINED_PATTERNS and APIGW_PATTERNS
# so that evidence IDs are stable regardless of which patterns actually run (ADR-007).
# For non-API-Gateway groups the APIGW slots are silently skipped (no query, no finding).
#
# Slot layout per group (0-indexed pat_idx within group):
#   0..len(PREDEFINED_PATTERNS)-1               → PREDEFINED_PATTERNS
#   len(PREDEFINED_PATTERNS).._TOTAL_PATTERN_SLOTS-1 → APIGW_PATTERNS (only run for API GW groups)
#
# Impact on evidence IDs when log groups > 1:
#   First group (lg_idx=0):  ev-logs-001..004 (PREDEFINED) — UNCHANGED from before this
#                            change, as long as PREDEFINED_PATTERNS stays the same.
#   Subsequent groups shift by _TOTAL_PATTERN_SLOTS per group instead of
#   len(PREDEFINED_PATTERNS). Example with 4 predefined + 2 APIGW:
#     lg_idx=1 PREDEFINED: ev-logs-007..010  (was ev-logs-005..008 before this change).
_TOTAL_PATTERN_SLOTS = len(PREDEFINED_PATTERNS) + len(APIGW_PATTERNS)


def collect_logs(scope: dict, logs_client) -> dict:
    """Collect CloudWatch Logs Insights findings for the given scope.

    For each log group, applies PREDEFINED_PATTERNS unconditionally and also
    applies APIGW_PATTERNS when the log group is an API Gateway log group
    (detected via _is_apigw_log_group()). For each (log_group, pattern) pair,
    runs two CWL Insights count queries (incident and baseline windows) and
    compares results.

    evidenceId is based on the (log_group_idx, pattern_idx) position within
    _TOTAL_PATTERN_SLOTS slots per group so it is stable across re-runs for the
    same scope (ADR-007). A pair that produces no finding still consumes its
    position slot; the ID is simply absent from findings[].

    Per-pair failures are caught and appended to errors[]; remaining pairs
    continue processing (ADR-009).

    Args:
        scope:       Scope dict from the Scope Builder
                     (see schemas/examples/scope.example.json).
        logs_client: Pre-built boto3 CloudWatch Logs client.

    Returns:
        Worker output dict: { incidentId, source, findings, errors }
    """
    incident_id = scope["incidentId"]
    service = scope["service"]
    incident_window = scope["incidentWindow"]
    baseline_window = scope["baselineWindow"]
    log_groups = scope.get("logGroups", [])

    incident_start = _parse_iso(incident_window["start"])
    incident_end = _parse_iso(incident_window["end"])
    baseline_start = _parse_iso(baseline_window["start"])
    baseline_end = _parse_iso(baseline_window["end"])

    findings = []
    errors = []
    log_groups_count = len(log_groups)
    pairs_scanned = 0
    pairs_skipped_no_incident_signal = 0
    pairs_skipped_below_threshold = 0

    logger.info(
        "logs_collect_start",
        extra={
            "source": "logs",
            "incidentId": incident_id,
            "service": service,
            "logGroupsCount": log_groups_count,
            "totalPatternSlotsPerGroup": _TOTAL_PATTERN_SLOTS,
            "anomalyThreshold": ANOMALY_THRESHOLD,
        },
    )

    for lg_idx, log_group in enumerate(log_groups):
        # Apply PREDEFINED_PATTERNS to all log groups; additionally apply
        # APIGW_PATTERNS to API Gateway log groups only. Evidence ID slots
        # are reserved for _TOTAL_PATTERN_SLOTS per group regardless, so IDs
        # are stable across runs for the same scope (ADR-007).
        patterns_for_group = (
            [*PREDEFINED_PATTERNS, *APIGW_PATTERNS]
            if _is_apigw_log_group(log_group)
            else list(PREDEFINED_PATTERNS)
        )
        for pat_idx, pattern in enumerate(patterns_for_group):
            pairs_scanned += 1
            # Slot index within group uses _TOTAL_PATTERN_SLOTS so APIGW slots
            # are always reserved even for groups that skip APIGW_PATTERNS.
            evidence_id = f"ev-logs-{lg_idx * _TOTAL_PATTERN_SLOTS + pat_idx + 1:03d}"
            source_label = f"{log_group}:{pattern['name']}"

            try:
                incident_count = _query_count(
                    logs_client,
                    log_group,
                    pattern["filter_expression"],
                    incident_start,
                    incident_end,
                    source_label,
                )
                baseline_count = _query_count(
                    logs_client,
                    log_group,
                    pattern["filter_expression"],
                    baseline_start,
                    baseline_end,
                    source_label,
                )
            except _QueryError as exc:
                logger.warning("Query error for %s: %s", source_label, exc)
                errors.append({"source": source_label, "reason": str(exc)})
                logger.info(
                    "logs_pattern_scan_result evidenceId=%s logGroup=%s pattern=%s incidentCount=%s baselineCount=%s classification=%s",
                    evidence_id,
                    log_group,
                    pattern["name"],
                    "n/a",
                    "n/a",
                    "query_error",
                )
                continue
            except ClientError as exc:
                error_code = exc.response["Error"]["Code"]
                error_msg = exc.response["Error"].get("Message", "")
                logger.warning(
                    "CloudWatch error for %s: %s — %s", source_label, error_code, error_msg
                )
                errors.append({
                    "source": source_label,
                    "reason": f"cloudwatch_error: {error_code}",
                })
                logger.info(
                    "logs_pattern_scan_result evidenceId=%s logGroup=%s pattern=%s incidentCount=%s baselineCount=%s classification=%s",
                    evidence_id,
                    log_group,
                    pattern["name"],
                    "n/a",
                    "n/a",
                    "query_error",
                )
                continue
            except Exception as exc:  # noqa: BLE001
                logger.warning("Unexpected error for %s: %s", source_label, exc)
                errors.append({
                    "source": source_label,
                    "reason": f"unexpected_error: {exc}",
                })
                logger.info(
                    "logs_pattern_scan_result evidenceId=%s logGroup=%s pattern=%s incidentCount=%s baselineCount=%s classification=%s",
                    evidence_id,
                    log_group,
                    pattern["name"],
                    "n/a",
                    "n/a",
                    "query_error",
                )
                continue

            if incident_count == 0:
                # No occurrences in incident window — no signal.
                pairs_skipped_no_incident_signal += 1
                logger.info(
                    "logs_pattern_scan_result evidenceId=%s logGroup=%s pattern=%s incidentCount=%s baselineCount=%s classification=%s",
                    evidence_id,
                    log_group,
                    pattern["name"],
                    incident_count,
                    baseline_count,
                    "no_incident_signal",
                )
                continue

            if baseline_count == 0:
                # Pattern absent in baseline but present in incident.
                finding_type = "new_error_pattern"
                change_ratio = None
                score_ratio = ANOMALY_THRESHOLD
            else:
                change_ratio = incident_count / baseline_count
                if change_ratio < ANOMALY_THRESHOLD:
                    logger.debug(
                        "Log pattern below anomaly threshold",
                        extra={
                            "source": "logs",
                            "incidentId": incident_id,
                            "label": source_label,
                            "changeRatio": round(change_ratio, 4),
                            "threshold": ANOMALY_THRESHOLD,
                        },
                    )
                    pairs_skipped_below_threshold += 1
                    logger.info(
                        "logs_pattern_scan_result evidenceId=%s logGroup=%s pattern=%s incidentCount=%s baselineCount=%s classification=%s",
                        evidence_id,
                        log_group,
                        pattern["name"],
                        incident_count,
                        baseline_count,
                        "below_threshold",
                    )
                    continue
                finding_type = "error_log_spike"
                score_ratio = change_ratio

            finding = _build_finding(
                evidence_id=evidence_id,
                service=service,
                log_group=log_group,
                pattern_name=pattern["name"],
                finding_type=finding_type,
                incident_count=incident_count,
                baseline_count=baseline_count,
                change_ratio=change_ratio,
                score=_score(score_ratio),
                incident_window=incident_window,
                baseline_window=baseline_window,
            )
            # For relevant error patterns, collect a small sample of exception messages.
            if pattern["name"] in _SAMPLE_PATTERNS:
                samples = _query_sample_messages(
                    logs_client,
                    log_group,
                    pattern["filter_expression"],
                    incident_start,
                    incident_end,
                    source_label,
                )
                if samples:
                    finding["details"]["exceptionSamples"] = samples
            findings.append(finding)
            logger.info(
                "logs_pattern_scan_result evidenceId=%s logGroup=%s pattern=%s incidentCount=%s baselineCount=%s classification=%s",
                evidence_id,
                log_group,
                pattern["name"],
                incident_count,
                baseline_count,
                finding_type,
            )

    logger.info(
        "logs_collect_complete",
        extra={
            "source": "logs",
            "incidentId": incident_id,
            "pairsScanned": pairs_scanned,
            "findingsCount": len(findings),
            "errorsCount": len(errors),
            "skippedNoIncidentSignal": pairs_skipped_no_incident_signal,
            "skippedBelowThreshold": pairs_skipped_below_threshold,
        },
    )

    return {
        "incidentId": incident_id,
        "source": "logs",
        "findings": findings,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

class _QueryError(Exception):
    """Raised when a CWL Insights query does not complete successfully."""


def _query_sample_messages(
    logs_client,
    log_group: str,
    filter_expression: str,
    start: datetime,
    end: datetime,
    source_label: str,
) -> list[str]:
    """Fetch up to _MAX_EXCEPTION_SAMPLES concrete exception message samples.

    Issues: filter <filter_expression> | fields @message | limit <n>
    Returns a list of message strings, each truncated to _MAX_EXCEPTION_MSG_LEN.
    Returns [] on any error — failures here never block a finding from being recorded.
    Called only for patterns in _SAMPLE_PATTERNS after incident_count > 0 is confirmed.
    """
    query_string = (
        f"filter {filter_expression} | fields @message | limit {_MAX_EXCEPTION_SAMPLES}"
    )
    try:
        response = logs_client.start_query(
            logGroupName=log_group,
            startTime=int(start.timestamp()),
            endTime=int(end.timestamp()),
            queryString=query_string,
        )
        query_id = response["queryId"]

        for _ in range(MAX_POLL_ITERATIONS):
            result = logs_client.get_query_results(queryId=query_id)
            status = result.get("status", "")
            if status == "Complete":
                samples = []
                for row in result.get("results", []):
                    for field in row:
                        if field.get("field") == "@message":
                            msg = str(field["value"])
                            samples.append(msg[:_MAX_EXCEPTION_MSG_LEN])
                            break
                return samples
            if status in ("Failed", "Cancelled", "Timeout"):
                logger.debug(
                    "Exception sample query %s for %s", status.lower(), source_label
                )
                return []
            time.sleep(1)

        logger.debug("Exception sample query poll timeout for %s", source_label)
        return []
    except Exception:  # noqa: BLE001
        logger.debug("Exception sample query failed silently for %s", source_label)
        return []


def _query_count(
    logs_client,
    log_group: str,
    filter_expression: str,
    start: datetime,
    end: datetime,
    source_label: str,
) -> int:
    """Run a CWL Insights query and return the count of matching log events.

    Issues `filter <filter_expression> | stats count(*) as count` and polls
    until the query completes. Returns 0 if no log lines match.

    Args:
        logs_client:       boto3 CloudWatch Logs client.
        log_group:         CWL log group name.
        filter_expression: CWL Insights filter string.
        start:             Query window start (timezone-aware datetime).
        end:               Query window end (timezone-aware datetime).
        source_label:      Label used in error messages.

    Returns:
        Integer count of matching log lines (0 if none).

    Raises:
        _QueryError: If the query fails, is cancelled, times out, or
                     the poll loop exceeds MAX_POLL_ITERATIONS.
        ClientError: Propagated on AWS API errors (except time-range
                     MalformedQueryException, which returns 0).
    """
    query_string = f"filter {filter_expression} | stats count(*) as count"

    try:
        response = logs_client.start_query(
            logGroupName=log_group,
            startTime=int(start.timestamp()),
            endTime=int(end.timestamp()),
            queryString=query_string,
        )
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        error_msg = exc.response["Error"].get("Message", "")
        if error_code == "MalformedQueryException" and (
            "before the log groups creation time" in error_msg
            or "exceeds the log groups log retention" in error_msg
        ):
            logger.debug(
                "Time window outside log group range for %s (%s), returning 0",
                source_label,
                error_msg,
            )
            return 0
        raise

    query_id = response["queryId"]

    for _ in range(MAX_POLL_ITERATIONS):
        result = logs_client.get_query_results(queryId=query_id)
        status = result.get("status", "")

        if status == "Complete":
            return _parse_count(result.get("results", []))

        if status in ("Failed", "Cancelled", "Timeout"):
            raise _QueryError(f"query_{status.lower()}: {source_label}")

        time.sleep(1)

    raise _QueryError(f"query_poll_timeout: {source_label}")


def _parse_count(results: list) -> int:
    """Extract the count value from CWL Insights stats query results.

    Results are a list of rows; each row is a list of field dicts.
    For `stats count(*) as count`, the first row carries
    `{"field": "count", "value": "<n>"}`.
    """
    if not results:
        return 0
    for row in results:
        for field in row:
            if field.get("field") == "count":
                try:
                    return int(field["value"])
                except (ValueError, TypeError):
                    return 0
    return 0


def _build_finding(
    *,
    evidence_id: str,
    service: str,
    log_group: str,
    pattern_name: str,
    finding_type: str,
    incident_count: int,
    baseline_count: int,
    change_ratio: float | None,
    score: float,
    incident_window: dict,
    baseline_window: dict,
) -> dict:
    resource_type, resource_name = _classify_resource_from_log_group(
        log_group=log_group,
        fallback_service=service,
    )

    return {
        "evidenceId": evidence_id,
        "source": "logs",
        "resourceType": resource_type,
        "resourceName": resource_name,
        "findingType": finding_type,
        "summary": _build_summary(pattern_name, finding_type, change_ratio),
        "score": score,
        "details": {
            "logGroup": log_group,
            "pattern": pattern_name,
            "incidentCount": incident_count,
            "baselineCount": baseline_count,
            "changeRatio": round(change_ratio, 4) if change_ratio is not None else None,
            "incidentWindow": incident_window,
            "baselineWindow": baseline_window,
        },
    }


def _score(ratio: float) -> float:
    """Map a change ratio to a 0–1 anomaly score; caps at 1.0 above 5x deviation.

    Consistent with the Metrics Worker scoring formula.
    """
    return round(min(ratio / 5.0, 1.0), 4)


def _build_summary(
    pattern_name: str,
    finding_type: str,
    change_ratio: float | None,
) -> str:
    if finding_type == "new_error_pattern":
        return (
            f'"{pattern_name}" appears in incident window with no baseline '
            f"occurrences [new_error_pattern]"
        )
    return (
        f'"{pattern_name}" log count changed {change_ratio:.1f}x vs baseline '
        f"[error_log_spike]"
    )


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _is_apigw_log_group(log_group: str) -> bool:
    """Return True if the log group is an API Gateway log group.

    API Gateway access logs are optionally published to CloudWatch under two
    naming conventions:
      - /aws/apigateway/<api-id>   (CDK/SAM managed groups)
      - apigateway/<api-name>      (user-defined stage access-log destination)

    Both conventions produce JSON-structured access log entries, which require
    APIGW_PATTERNS instead of PREDEFINED_PATTERNS.
    """
    if not isinstance(log_group, str):
        return False
    return log_group.startswith("/aws/apigateway/") or log_group.startswith("apigateway/")


def _classify_resource_from_log_group(*, log_group: str, fallback_service: str) -> tuple[str, str]:
    """Infer evidence resource classification from CloudWatch log group name.

    Recognised prefixes (checked in order):
      /aws/apigateway/<api-id>  → api-gateway, <api-id>  (CDK/SAM managed)
      apigateway/<api-name>     → api-gateway, <api-name> (user-defined)
      /aws/lambda/<function>    → lambda-function, <function>

    All other log groups fall back to the scope service name as a lambda-function.
    """
    if isinstance(log_group, str):
        for apigw_prefix in ("/aws/apigateway/", "apigateway/"):
            if log_group.startswith(apigw_prefix):
                name = log_group[len(apigw_prefix):].strip()
                if name:
                    return "api-gateway", name

        lambda_prefix = "/aws/lambda/"
        if log_group.startswith(lambda_prefix):
            function_name = log_group[len(lambda_prefix):].strip()
            if function_name:
                return "lambda-function", function_name

    return "lambda-function", fallback_service
