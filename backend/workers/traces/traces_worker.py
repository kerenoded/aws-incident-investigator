"""
Trace Worker — core logic.

Queries scoped AWS X-Ray traces for incident and baseline windows and returns
bounded, deterministic trace findings.

- ADR-002: deterministic worker, no AI
- ADR-004: compares incident vs baseline windows
- ADR-007: evidenceId stable by (trace_service_idx, finding_slot)
- ADR-009: per-service error handling; errors go to errors[], processing continues

Logging:
    Emits deterministic breadcrumbs at start, key decision points, and
    completion to make evidence collection debugging easier in CloudWatch logs.
"""

import json
import logging
from datetime import datetime, timezone

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

LATENCY_SPIKE_THRESHOLD = 1.5
MAX_TRACE_SUMMARIES_PER_WINDOW = 20
MAX_TRACE_IDS_FOR_SEGMENT_ANALYSIS = 10
MAX_TOTAL_FINDINGS = 9

# Minimum ratio of incident fault/error rate to baseline rate to consider the
# status change significant enough to emit a trace_error_status finding.
# A ratio of 2.0 means the incident rate must be at least 2× the baseline rate.
_ERROR_STATUS_BASELINE_RATIO_THRESHOLD = 2.0

# X-Ray namespaces that represent actual downstream service calls.
# namespace=local means the SDK wrapper segment — it propagates fault/error flags
# from the inner aws/remote call and should not independently drive strong scoring.
_DOWNSTREAM_NAMESPACES = frozenset({"aws", "remote"})


def _log_event(level: int, event: str, **fields) -> None:
    """Emit a deterministic log line with JSON payload visible in CloudWatch."""
    try:
        payload = json.dumps(fields, sort_keys=True, default=str)
    except Exception:  # noqa: BLE001
        payload = "{}"
    logger.log(level, "%s %s", event, payload)


def collect_traces(scope: dict, xray_client) -> dict:
    """Collect X-Ray findings for trace services in the provided scope."""
    incident_id = scope["incidentId"]
    trace_services = scope.get("traceServices", [])
    incident_window = scope["incidentWindow"]
    baseline_window = scope["baselineWindow"]

    _log_event(
        logging.INFO,
        "traces_collect_start",
        source="traces",
        incidentId=incident_id,
        traceServicesCount=len(trace_services),
        latencyThreshold=LATENCY_SPIKE_THRESHOLD,
    )

    # Approved behavior: silent empty output when no trace services are scoped.
    if not trace_services:
        _log_event(
            logging.INFO,
            "traces_collect_no_services",
            source="traces",
            incidentId=incident_id,
        )
        return {
            "incidentId": incident_id,
            "source": "traces",
            "findings": [],
            "errors": [],
        }

    incident_start = _parse_iso(incident_window["start"])
    incident_end = _parse_iso(incident_window["end"])
    baseline_start = _parse_iso(baseline_window["start"])
    baseline_end = _parse_iso(baseline_window["end"])

    findings = []
    errors = []
    services_scanned = 0

    for service_idx, trace_service in enumerate(trace_services):
        services_scanned += 1
        source_label = f"xray:{trace_service}"
        _log_event(
            logging.DEBUG,
            "traces_service_scan_start",
            source="traces",
            incidentId=incident_id,
            traceService=trace_service,
            serviceIndex=service_idx,
        )
        latency_slot_id = _evidence_id(service_idx, 0)
        segment_slot_id = _evidence_id(service_idx, 1)
        fault_slot_id = _evidence_id(service_idx, 2)
        error_status_slot_id = _evidence_id(service_idx, 3)

        try:
            incident_summaries = _get_trace_summaries(
                xray_client=xray_client,
                trace_service=trace_service,
                start=incident_start,
                end=incident_end,
                max_results=MAX_TRACE_SUMMARIES_PER_WINDOW,
            )
            baseline_summaries = _get_trace_summaries(
                xray_client=xray_client,
                trace_service=trace_service,
                start=baseline_start,
                end=baseline_end,
                max_results=MAX_TRACE_SUMMARIES_PER_WINDOW,
            )
            incident_status = _summarize_trace_status(incident_summaries)
            baseline_status = _summarize_trace_status(baseline_summaries)
            _log_event(
                logging.DEBUG,
                "traces_service_summaries_collected",
                source="traces",
                incidentId=incident_id,
                traceService=trace_service,
                incidentTraceSummaries=len(incident_summaries),
                baselineTraceSummaries=len(baseline_summaries),
                incidentTraceStatus=incident_status,
                baselineTraceStatus=baseline_status,
            )
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            _log_event(
                logging.WARNING,
                "traces_service_scan_xray_error",
                source="traces",
                incidentId=incident_id,
                traceService=trace_service,
                errorCode=error_code,
            )
            errors.append({
                "source": source_label,
                "reason": f"xray_error: {error_code}",
            })
            continue
        except Exception as exc:  # noqa: BLE001
            _log_event(
                logging.WARNING,
                "traces_service_scan_unexpected_error",
                source="traces",
                incidentId=incident_id,
                traceService=trace_service,
                reason=str(exc),
            )
            errors.append({
                "source": source_label,
                "reason": f"unexpected_error: {exc}",
            })
            continue

        # Fetch trace documents early — used for root-service detection (Finding 1)
        # and segment analysis (Findings 2+3). Fetched once; shared across all findings.
        trace_ids = [
            s.get("Id") for s in incident_summaries if s.get("Id")
        ][:MAX_TRACE_IDS_FOR_SEGMENT_ANALYSIS]
        _log_event(
            logging.DEBUG,
            "traces_service_segment_analysis_scope",
            source="traces",
            incidentId=incident_id,
            traceService=trace_service,
            traceIdsSampledCount=len(trace_ids),
            traceIdsSampleLimit=MAX_TRACE_IDS_FOR_SEGMENT_ANALYSIS,
        )
        traces = []
        if trace_ids:
            try:
                traces = _batch_get_traces(xray_client, trace_ids)
                _log_event(
                    logging.DEBUG,
                    "traces_service_documents_collected",
                    source="traces",
                    incidentId=incident_id,
                    traceService=trace_service,
                    traceDocumentsCount=len(traces),
                )
            except ClientError as exc:
                error_code = exc.response["Error"]["Code"]
                _log_event(
                    logging.WARNING,
                    "traces_segment_analysis_xray_error",
                    source="traces",
                    incidentId=incident_id,
                    traceService=trace_service,
                    errorCode=error_code,
                )
                errors.append({
                    "source": source_label,
                    "reason": f"xray_error: {error_code}",
                })
            except Exception as exc:  # noqa: BLE001
                _log_event(
                    logging.WARNING,
                    "traces_segment_analysis_unexpected_error",
                    source="traces",
                    incidentId=incident_id,
                    traceService=trace_service,
                    reason=str(exc),
                )
                errors.append({
                    "source": source_label,
                    "reason": f"unexpected_error: {exc}",
                })
        else:
            _log_event(
                logging.DEBUG,
                "traces_segment_analysis_skipped",
                source="traces",
                incidentId=incident_id,
                traceService=trace_service,
                reason="no_trace_ids_from_incident_summaries",
            )

        # Finding 1 (max one per service): trace_latency_spike.
        # Only emitted for root services — services that appear as root segments in
        # their own traces. For downstream dependencies (DynamoDB, SQS, etc.),
        # ResponseTime in trace summaries reflects the root trace's end-to-end
        # duration, not the dependency's own latency, making the finding misleading.
        if _is_root_service(traces, trace_service):
            latency_finding = _build_latency_spike_finding(
                evidence_id=latency_slot_id,
                trace_service=trace_service,
                incident_summaries=incident_summaries,
                baseline_summaries=baseline_summaries,
                incident_window=incident_window,
                baseline_window=baseline_window,
                incident_status=incident_status,
                baseline_status=baseline_status,
            )
            if latency_finding:
                findings.append(latency_finding)
                _log_event(
                    logging.DEBUG,
                    "traces_finding_emitted",
                    source="traces",
                    incidentId=incident_id,
                    traceService=trace_service,
                    findingType=latency_finding.get("findingType"),
                    evidenceId=latency_finding.get("evidenceId"),
                    score=latency_finding.get("score"),
                )
            else:
                _log_event(
                    logging.DEBUG,
                    "traces_latency_finding_skipped",
                    source="traces",
                    incidentId=incident_id,
                    traceService=trace_service,
                    reason="no_latency_spike_or_missing_incident_response_time",
                )
        else:
            _log_event(
                logging.DEBUG,
                "traces_latency_finding_skipped",
                source="traces",
                incidentId=incident_id,
                traceService=trace_service,
                reason="not_a_root_service",
            )

        # Finding 4 (max one per service): trace_error_status
        # Emitted when X-Ray trace summaries show fault/error flags in the incident
        # window that were absent (or significantly lower) in the baseline.
        # This catches Lambda-level errors (timeouts, unhandled exceptions) that do
        # not manifest as faulted downstream subsegments.
        error_status_finding = _build_trace_error_status_finding(
            evidence_id=error_status_slot_id,
            trace_service=trace_service,
            incident_status=incident_status,
            baseline_status=baseline_status,
            incident_window=incident_window,
            baseline_window=baseline_window,
        )
        if error_status_finding:
            findings.append(error_status_finding)
            _log_event(
                logging.INFO,
                "traces_finding_emitted",
                source="traces",
                incidentId=incident_id,
                traceService=trace_service,
                findingType=error_status_finding.get("findingType"),
                evidenceId=error_status_finding.get("evidenceId"),
                score=error_status_finding.get("score"),
                incidentFaultCount=incident_status["faultCount"],
                incidentErrorCount=incident_status["errorCount"],
                baselineFaultCount=baseline_status["faultCount"],
                baselineErrorCount=baseline_status["errorCount"],
            )
        else:
            _log_event(
                logging.DEBUG,
                "traces_error_status_finding_skipped",
                source="traces",
                incidentId=incident_id,
                traceService=trace_service,
                incidentFaultCount=incident_status["faultCount"],
                incidentErrorCount=incident_status["errorCount"],
                incidentFaultRate=incident_status["faultRate"],
                incidentErrorRate=incident_status["errorRate"],
                baselineFaultCount=baseline_status["faultCount"],
                baselineErrorCount=baseline_status["errorCount"],
                baselineFaultRate=baseline_status["faultRate"],
                baselineErrorRate=baseline_status["errorRate"],
                reason="no_new_error_or_fault_signal_vs_baseline",
            )

        # Findings 2+3 (max one each per service): latency_dominant_segment and
        # trace_dependency_fault. Both consume the trace documents already fetched
        # above — no additional X-Ray API requests are made.
        #
        # latency_dominant_segment is only emitted for root services. For downstream
        # services, _aggregate_segment_durations_ms skips non-root segments, so the
        # dominant segment is always the root Lambda — identical to the Lambda's own
        # finding and misleading when attributed to a downstream resource name.
        if traces:
            if _is_root_service(traces, trace_service):
                dominant_segment_finding = _build_dominant_segment_finding(
                    evidence_id=segment_slot_id,
                    trace_service=trace_service,
                    traces=traces,
                    incident_window=incident_window,
                )
                if dominant_segment_finding:
                    findings.append(dominant_segment_finding)
                    _log_event(
                        logging.DEBUG,
                        "traces_finding_emitted",
                        source="traces",
                        incidentId=incident_id,
                        traceService=trace_service,
                        findingType=dominant_segment_finding.get("findingType"),
                        evidenceId=dominant_segment_finding.get("evidenceId"),
                        score=dominant_segment_finding.get("score"),
                    )
            else:
                _log_event(
                    logging.DEBUG,
                    "traces_dominant_segment_finding_skipped",
                    source="traces",
                    incidentId=incident_id,
                    traceService=trace_service,
                    reason="not_a_root_service",
                )

            fault_finding = _build_dependency_fault_finding(
                evidence_id=fault_slot_id,
                trace_service=trace_service,
                traces=traces,
                incident_window=incident_window,
            )
            if fault_finding:
                findings.append(fault_finding)
                _log_event(
                    logging.DEBUG,
                    "traces_finding_emitted",
                    source="traces",
                    incidentId=incident_id,
                    traceService=trace_service,
                    findingType=fault_finding.get("findingType"),
                    evidenceId=fault_finding.get("evidenceId"),
                    score=fault_finding.get("score"),
                )
            elif (
                incident_status["faultCount"] > 0
                or incident_status["errorCount"] > 0
                or incident_status["throttleCount"] > 0
            ):
                _log_event(
                    logging.DEBUG,
                    "traces_status_without_dependency_fault",
                    source="traces",
                    incidentId=incident_id,
                    traceService=trace_service,
                    incidentTraceStatus=incident_status,
                    reason=(
                        "trace summaries indicate error/fault/throttle but sampled "
                        "segment documents had no matching faulted dependency subsegments"
                    ),
                )

        _log_event(
            logging.DEBUG,
            "traces_service_scan_complete",
            source="traces",
            incidentId=incident_id,
            traceService=trace_service,
            findingsSoFar=len(findings),
            errorsSoFar=len(errors),
        )

        if len(findings) >= MAX_TOTAL_FINDINGS:
            _log_event(
                logging.INFO,
                "traces_findings_cap_reached",
                source="traces",
                incidentId=incident_id,
                maxTotalFindings=MAX_TOTAL_FINDINGS,
            )
            findings = findings[:MAX_TOTAL_FINDINGS]
            break

    _log_event(
        logging.INFO,
        "traces_collect_complete",
        source="traces",
        incidentId=incident_id,
        servicesScanned=services_scanned,
        findingsCount=len(findings),
        errorsCount=len(errors),
    )

    return {
        "incidentId": incident_id,
        "source": "traces",
        "findings": findings,
        "errors": errors,
    }


def _build_latency_spike_finding(
    *,
    evidence_id: str,
    trace_service: str,
    incident_summaries: list,
    baseline_summaries: list,
    incident_window: dict,
    baseline_window: dict,
    incident_status: dict,
    baseline_status: dict,
) -> dict | None:
    incident_avg_ms = _average_response_time_ms(incident_summaries)
    baseline_avg_ms = _average_response_time_ms(baseline_summaries)

    if incident_avg_ms is None:
        return None

    if baseline_avg_ms is None or baseline_avg_ms == 0:
        if incident_avg_ms <= 0:
            return None
        ratio = LATENCY_SPIKE_THRESHOLD
    else:
        ratio = incident_avg_ms / baseline_avg_ms
        if ratio < LATENCY_SPIKE_THRESHOLD:
            _log_event(
                logging.DEBUG,
                "traces_latency_below_threshold",
                source="traces",
                traceService=trace_service,
                changeRatio=round(ratio, 4),
                threshold=LATENCY_SPIKE_THRESHOLD,
            )
            return None

    return {
        "evidenceId": evidence_id,
        "source": "traces",
        "resourceType": "xray-service",
        "resourceName": trace_service,
        "findingType": "trace_latency_spike",
        "summary": (
            f"X-Ray response time for {trace_service} changed {ratio:.1f}x vs baseline "
            "[trace_latency_spike]"
        ),
        "score": _score(ratio),
        "details": {
            "traceService": trace_service,
            "incidentTraceCount": len(incident_summaries),
            "baselineTraceCount": len(baseline_summaries),
            "incidentAvgResponseTimeMs": round(incident_avg_ms, 2),
            "baselineAvgResponseTimeMs": round(baseline_avg_ms, 2)
            if baseline_avg_ms is not None
            else None,
            "changeRatio": round(ratio, 4),
            "incidentTraceStatus": incident_status,
            "baselineTraceStatus": baseline_status,
            "incidentWindow": incident_window,
            "baselineWindow": baseline_window,
        },
    }


def _summarize_trace_status(summaries: list) -> dict:
    """Aggregate trace summary status flags for operator-facing diagnostics."""
    trace_count = len(summaries)
    fault_count = sum(1 for s in summaries if bool(s.get("HasFault", False)))
    error_count = sum(1 for s in summaries if bool(s.get("HasError", False)))
    throttle_count = sum(1 for s in summaries if bool(s.get("HasThrottle", False)))

    if trace_count > 0:
        fault_rate = round(fault_count / trace_count, 4)
        error_rate = round(error_count / trace_count, 4)
        throttle_rate = round(throttle_count / trace_count, 4)
    else:
        fault_rate = 0.0
        error_rate = 0.0
        throttle_rate = 0.0

    return {
        "traceCount": trace_count,
        "faultCount": fault_count,
        "errorCount": error_count,
        "throttleCount": throttle_count,
        "faultRate": fault_rate,
        "errorRate": error_rate,
        "throttleRate": throttle_rate,
    }


def _build_dominant_segment_finding(
    *,
    evidence_id: str,
    trace_service: str,
    traces: list,
    incident_window: dict,
) -> dict | None:
    segment_totals_ms = _aggregate_segment_durations_ms(traces)
    if not segment_totals_ms:
        return None

    total_ms = sum(segment_totals_ms.values())
    if total_ms <= 0:
        return None

    dominant_name, dominant_ms = max(segment_totals_ms.items(), key=lambda kv: kv[1])
    dominant_pct = (dominant_ms / total_ms) * 100.0

    return {
        "evidenceId": evidence_id,
        "source": "traces",
        "resourceType": "xray-service",
        "resourceName": trace_service,
        "findingType": "latency_dominant_segment",
        "summary": (
            f"Dominant segment in sampled {trace_service} traces is {dominant_name} "
            f"({dominant_pct:.0f}% of sampled segment time)"
        ),
        "score": round(min(dominant_pct / 100.0, 1.0), 4),
        "details": {
            "traceService": trace_service,
            "sampledTraceCount": len(traces),
            "dominantSegment": dominant_name,
            "dominantSegmentMs": round(dominant_ms, 2),
            "dominantSegmentPct": round(dominant_pct, 2),
            "topSegmentsMs": _top_segments(segment_totals_ms, limit=3),
            "incidentWindow": incident_window,
        },
    }


def _get_trace_summaries(*, xray_client, trace_service: str, start: datetime, end: datetime, max_results: int) -> list:
    """Query X-Ray GetTraceSummaries for one service and bounded results."""
    next_token = None
    summaries: list = []
    filter_expression = f'service("{trace_service}")'

    _MAX_PAGES = 10  # Hard cap on API calls per query to protect quotas and timeouts.
    pages = 0
    while len(summaries) < max_results and pages < _MAX_PAGES:
        kwargs = {
            "StartTime": start,
            "EndTime": end,
            "Sampling": False,
            "FilterExpression": filter_expression,
        }
        if next_token:
            kwargs["NextToken"] = next_token

        response = xray_client.get_trace_summaries(**kwargs)
        summaries.extend(response.get("TraceSummaries", []))
        pages += 1
        next_token = response.get("NextToken")
        if not next_token:
            break

    if pages == _MAX_PAGES and next_token:
        logger.warning(
            "X-Ray pagination hit page limit; some traces may be omitted",
            extra={"service": filter_expression, "pagesRead": pages},
        )

    return summaries[:max_results]


def _batch_get_traces(xray_client, trace_ids: list[str]) -> list:
    """Fetch trace documents in bounded batches."""
    traces = []
    for i in range(0, len(trace_ids), 5):
        batch = trace_ids[i : i + 5]
        response = xray_client.batch_get_traces(TraceIds=batch)
        traces.extend(response.get("Traces", []))
    return traces


def _aggregate_segment_durations_ms(traces: list) -> dict[str, float]:
    """Aggregate root-segment durations by segment name across sampled traces."""
    totals: dict[str, float] = {}
    for trace in traces:
        for segment_wrapper in trace.get("Segments", []):
            document_str = segment_wrapper.get("Document")
            if not document_str:
                continue
            try:
                doc = json.loads(document_str)
            except json.JSONDecodeError:
                continue

            # Favor root segments to reduce double-counting from nested subsegments.
            if doc.get("parent_id"):
                continue

            name = doc.get("name")
            start_time = doc.get("start_time")
            end_time = doc.get("end_time")
            if not name or start_time is None or end_time is None:
                continue

            duration_ms = max((float(end_time) - float(start_time)) * 1000.0, 0.0)
            totals[name] = totals.get(name, 0.0) + duration_ms
    return totals


def _is_root_service(traces: list, service_name: str) -> bool:
    """Return True if service_name appears as a root segment in any of the given traces.

    Root segments have no parent_id. Downstream dependencies (DynamoDB, SQS, etc.)
    only appear as subsegments within the root service's trace document — they never
    produce root segments of their own. This distinction matters for trace_latency_spike:
    ResponseTime in trace summaries is always the root trace's end-to-end duration,
    so it is only meaningful for root services.
    """
    for trace in traces:
        for segment_wrapper in trace.get("Segments", []):
            document_str = segment_wrapper.get("Document")
            if not document_str:
                continue
            try:
                doc = json.loads(document_str)
            except json.JSONDecodeError:
                continue
            if not doc.get("parent_id") and doc.get("name") == service_name:
                return True
    return False


def _average_response_time_ms(summaries: list) -> float | None:
    values_ms = []
    for summary in summaries:
        response_time_seconds = summary.get("ResponseTime")
        if response_time_seconds is None:
            continue
        values_ms.append(float(response_time_seconds) * 1000.0)
    if not values_ms:
        return None
    return sum(values_ms) / len(values_ms)


def _top_segments(segment_totals_ms: dict[str, float], limit: int) -> list[dict]:
    return [
        {"segment": name, "durationMs": round(duration_ms, 2)}
        for name, duration_ms in sorted(
            segment_totals_ms.items(), key=lambda kv: kv[1], reverse=True
        )[:limit]
    ]


def _extract_faulted_subsegments(doc: dict) -> list[dict]:
    """Recursively walk a root segment document and return all subsegments with fault/error/throttle.

    Works generically on any subsegment regardless of namespace or service name.
    Reads standard X-Ray boolean flags only — no service-specific logic.
    HTTP status is read from http.response.status (AWS SDK standard location).
    """
    results = []
    for sub in doc.get("subsegments", []):
        name = sub.get("name")
        if not name:
            continue
        fault = bool(sub.get("fault", False))
        error = bool(sub.get("error", False))
        throttle = bool(sub.get("throttle", False))
        if fault or error or throttle:
            http_status = None
            http_resp = sub.get("http", {}).get("response", {})
            if http_resp:
                http_status = http_resp.get("status")
            results.append({
                "name": name,
                "namespace": sub.get("namespace"),
                "fault": fault,
                "error": error,
                "throttle": throttle,
                "httpStatus": int(http_status) if http_status is not None else None,
            })
        # Recurse into nested subsegments.
        results.extend(_extract_faulted_subsegments(sub))
    return results


def _build_dependency_fault_finding(
    *,
    evidence_id: str,
    trace_service: str,
    traces: list,
    incident_window: dict,
) -> dict | None:
    """Emit a finding when sampled traces contain faulted/errored dependency subsegments.

    Score reflects signal strength:
      0.90 — any subsegment with fault=true (5xx server error)
      0.75 — any subsegment with throttle=true (no fault)
      0.70 — any subsegment with error=true only (4xx client error)
    """
    # Aggregate faulted subsegments by name across all sampled traces.
    counts: dict[str, dict] = {}  # name -> aggregated entry
    sampled_count = len(traces)
    for trace in traces:
        seen_this_trace: set[str] = set()  # count each name once per trace
        for segment_wrapper in trace.get("Segments", []):
            document_str = segment_wrapper.get("Document")
            if not document_str:
                continue
            try:
                doc = json.loads(document_str)
            except json.JSONDecodeError:
                continue
            for sub in _extract_faulted_subsegments(doc):
                name = sub["name"]
                if name in seen_this_trace:
                    continue
                seen_this_trace.add(name)
                if name not in counts:
                    counts[name] = {**sub, "occurrences": 0}
                counts[name]["occurrences"] += 1
                # Escalate flags — once true, stays true across occurrences.
                if sub["fault"]:
                    counts[name]["fault"] = True
                if sub["throttle"]:
                    counts[name]["throttle"] = True

    if not counts:
        return None

    # Sort by occurrences descending; take top 3.
    top = sorted(counts.values(), key=lambda e: e["occurrences"], reverse=True)[:3]

    # Determine score from the worst signal seen on actual downstream service calls.
    # namespace=local subsegments are SDK wrappers — they propagate fault=true from
    # the inner aws/remote call but do not independently signal a downstream outage.
    # Only aws/remote namespaces drive the strong 0.90/0.75 scores.
    #
    # Additionally, the AWS SDK sets fault=true on 4xx responses (e.g. ValidationException).
    # A 4xx is a client/request error, not a downstream service failure, so we exclude
    # entries where httpStatus is in the 4xx range from the strong-fault check.
    # fault=true with no httpStatus is treated as strong (connectivity / timeout signal).
    def _is_strong_downstream_fault(e: dict) -> bool:
        if not e["fault"]:
            return False
        if e.get("namespace") not in _DOWNSTREAM_NAMESPACES:
            return False
        http_status = e.get("httpStatus")
        if http_status is not None and 400 <= http_status < 500:
            return False  # 4xx: client error, not downstream failure
        return True

    any_downstream_fault = any(_is_strong_downstream_fault(e) for e in top)
    any_downstream_throttle = any(
        e["throttle"] and e.get("namespace") in _DOWNSTREAM_NAMESPACES for e in top
    )
    if any_downstream_fault:
        score = 0.90
    elif any_downstream_throttle:
        score = 0.75
    else:
        score = 0.70

    # Build a concise summary using the top faulted subsegment.
    primary = top[0]
    signal_label = "fault" if primary["fault"] else ("throttled" if primary["throttle"] else "error")
    http_part = f", HTTP {primary['httpStatus']}" if primary["httpStatus"] is not None else ""
    summary = (
        f"Faulted downstream call in sampled {trace_service} traces: "
        f"{primary['name']} ({signal_label}{http_part}, "
        f"{primary['occurrences']}/{sampled_count} traces)"
    )

    return {
        "evidenceId": evidence_id,
        "source": "traces",
        "resourceType": "xray-subsegment",
        "resourceName": trace_service,
        "findingType": "trace_dependency_fault",
        "summary": summary,
        "score": score,
        "details": {
            "traceService": trace_service,
            "sampledTraceCount": sampled_count,
            "faultedSubsegments": top,
            "incidentWindow": incident_window,
        },
    }


def _build_trace_error_status_finding(
    *,
    evidence_id: str,
    trace_service: str,
    incident_status: dict,
    baseline_status: dict,
    incident_window: dict,
    baseline_window: dict,
) -> dict | None:
    """Emit a finding when X-Ray trace summaries show new fault/error status in the incident window.

    This catches Lambda-level errors (timeouts, unhandled exceptions) that do not
    manifest as faulted downstream subsegments — e.g. a Lambda timeout sets HasFault=True
    on the trace summary but creates no faulted subsegment in the segment documents.

    Score reflects signal strength:
      0.85 — fault rate > 50% of incident traces
      0.70 — any fault present (rate ≤ 50%)
      0.60 — error only (no fault), any error present
    """
    incident_fault_count = incident_status.get("faultCount", 0)
    incident_error_count = incident_status.get("errorCount", 0)
    incident_trace_count = incident_status.get("traceCount", 0)

    # No error/fault signal in incident window → skip.
    if incident_fault_count == 0 and incident_error_count == 0:
        return None

    if incident_trace_count == 0:
        return None

    incident_fault_rate = incident_status.get("faultRate", 0.0)
    incident_error_rate = incident_status.get("errorRate", 0.0)
    baseline_fault_rate = baseline_status.get("faultRate", 0.0)
    baseline_error_rate = baseline_status.get("errorRate", 0.0)

    # If baseline had similar fault/error rates, this is not a new signal.
    # Incident combined rate must be at least _ERROR_STATUS_BASELINE_RATIO_THRESHOLD×
    # the baseline combined rate, or the baseline must have been zero.
    incident_combined_rate = incident_fault_rate + incident_error_rate
    baseline_combined_rate = baseline_fault_rate + baseline_error_rate
    if (
        baseline_combined_rate > 0
        and incident_combined_rate < baseline_combined_rate * _ERROR_STATUS_BASELINE_RATIO_THRESHOLD
    ):
        return None

    # Determine primary signal type and score.
    if incident_fault_count > 0:
        signal_type = "fault"
        count = incident_fault_count
        if incident_fault_rate > 0.5:
            score = 0.85
        else:
            score = 0.70
    else:
        signal_type = "error"
        count = incident_error_count
        score = 0.60

    baseline_fault_count = baseline_status.get("faultCount", 0)
    baseline_error_count = baseline_status.get("errorCount", 0)
    baseline_trace_count = baseline_status.get("traceCount", 0)

    summary = (
        f"X-Ray traces for {trace_service} show {count}/{incident_trace_count} "
        f"traces with {signal_type} status in incident window "
        f"(baseline: {baseline_fault_count + baseline_error_count}/{baseline_trace_count}) "
        f"[trace_error_status]"
    )

    return {
        "evidenceId": evidence_id,
        "source": "traces",
        "resourceType": "xray-service",
        "resourceName": trace_service,
        "findingType": "trace_error_status",
        "summary": summary,
        "score": score,
        "details": {
            "traceService": trace_service,
            "signalType": signal_type,
            "incidentTraceStatus": incident_status,
            "baselineTraceStatus": baseline_status,
            "incidentWindow": incident_window,
            "baselineWindow": baseline_window,
        },
    }


def _evidence_id(service_idx: int, finding_slot: int) -> str:
    # Four reserved finding slots per trace service:
    #   slot 0 => trace_latency_spike
    #   slot 1 => latency_dominant_segment
    #   slot 2 => trace_dependency_fault
    #   slot 3 => trace_error_status
    index = service_idx * 4 + finding_slot + 1
    return f"ev-traces-{index:03d}"


def _parse_iso(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _score(change_ratio: float) -> float:
    # Consistent with existing metrics/logs scoring style.
    return round(min(change_ratio / 5.0, 1.0), 4)
