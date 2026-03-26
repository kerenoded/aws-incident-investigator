"""
Metrics Worker — core logic.

Fetches CloudWatch metric statistics for the incident and baseline windows,
compares them, and produces structured findings.

- ADR-002: deterministic worker, no AI
- ADR-004: baseline comparison (preceding equal-duration window)
- ADR-007: evidenceId stable by metric position in scope["metrics"] (1-indexed)
- ADR-009: per-metric error handling; errors go to errors[], processing continues

Logging:
    Emits deterministic breadcrumbs at start, key decision points, and
    completion to make evidence collection debugging easier in CloudWatch logs.
"""

import json
import math
import logging
from datetime import datetime

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Minimum change ratio to produce a finding (1.5 = 50% increase over baseline).
ANOMALY_THRESHOLD = 1.5
APIGW_LATENCY_ANOMALY_THRESHOLD = 1.2
MAX_APIGW_DIMENSION_DISCOVERY_PAGES = 2
MAX_APIGW_DIMENSION_RETRIES = 8

# Maps CloudWatch metricName → machine-readable findingType.
FINDING_TYPE_MAP = {
    "TargetResponseTime": "latency_spike",
    "Latency": "latency_spike",
    "IntegrationLatency": "latency_spike",
    "Duration": "latency_spike",
    "CPUUtilization": "cpu_spike",
    "MemoryUtilization": "memory_spike",
    "HTTPCode_Target_5XX_Count": "error_rate_spike",
    "HTTPCode_ELB_5XX_Count": "error_rate_spike",
    "Errors": "error_rate_spike",
    "4XXError": "error_rate_spike",
    "5XXError": "error_rate_spike",
    "4xx": "error_rate_spike",
    "5xx": "error_rate_spike",
    "Throttles": "throttle_spike",
    "DatabaseConnections": "db_connection_spike",
}

# Maps CloudWatch namespace → resource type label.
RESOURCE_TYPE_MAP = {
    "AWS/ApplicationELB": "alb",
    "AWS/ELB": "alb",
    "AWS/Lambda": "lambda",
    "AWS/RDS": "rds-instance",
    "AWS/DynamoDB": "dynamodb-table",
    "AWS/ApiGateway": "api-gateway",
}

SPARSE_COUNTER_METRICS_ZERO_IMPUTE = {
    ("AWS/ApiGateway", "4XXError", "Sum"),
    ("AWS/ApiGateway", "5XXError", "Sum"),
    ("AWS/ApiGateway", "4xx", "Sum"),
    ("AWS/ApiGateway", "5xx", "Sum"),
}

ERROR_SIGNAL_METRIC_NAMES = {
    "Errors",
    "4XXError",
    "5XXError",
    "4xx",
    "5xx",
    "HTTPCode_Target_5XX_Count",
    "HTTPCode_ELB_5XX_Count",
}


def collect_metrics(scope: dict, cw_client) -> dict:
    """Collect CloudWatch metric findings for the given scope.

    Iterates over scope["metrics"] and, for each metric, fetches the aggregate
    statistic for both the incident and baseline windows. A finding is produced
    when the change ratio is >= ANOMALY_THRESHOLD.

    evidenceId is based on the metric's 1-indexed position in scope["metrics"]
    so it is stable across runs for the same scope (ADR-007). A metric that
    produces no finding still consumes its position slot; the ID is simply not
    present in findings[].

    Failures on individual metrics are caught and appended to errors[]; the
    remaining metrics are still processed (ADR-009).

    Args:
        scope:     Scope dict from the Scope Builder
                   (see schemas/examples/scope.example.json).
        cw_client: Pre-built boto3 CloudWatch client.

    Returns:
        Worker output dict: { incidentId, source, findings, errors }
    """
    incident_id = scope["incidentId"]
    service = scope["service"]
    incident_window = scope["incidentWindow"]
    baseline_window = scope["baselineWindow"]

    incident_start = _parse_iso(incident_window["start"])
    incident_end = _parse_iso(incident_window["end"])
    baseline_start = _parse_iso(baseline_window["start"])
    baseline_end = _parse_iso(baseline_window["end"])

    duration_seconds = (incident_end - incident_start).total_seconds()
    period = _round_period(duration_seconds)

    findings = []
    errors = []
    metrics_scanned = len(scope.get("metrics", []))
    metrics_skipped_no_data = 0
    metrics_skipped_below_threshold = 0
    apigw_discovery_cache: dict[str, list[dict]] = {}
    error_signal_diagnostics: list[dict] = []

    logger.info(
        "metrics_collect_start",
        extra={
            "source": "metrics",
            "incidentId": incident_id,
            "service": service,
            "metricsCount": metrics_scanned,
            "anomalyThreshold": ANOMALY_THRESHOLD,
        },
    )
    logger.debug(
        "metrics_collect_start_details %s",
        _compact_json(
            {
                "incidentId": incident_id,
                "service": service,
                "metricsCount": metrics_scanned,
                "anomalyThreshold": ANOMALY_THRESHOLD,
                "incidentWindow": incident_window,
                "baselineWindow": baseline_window,
            }
        ),
    )

    for idx, metric in enumerate(scope.get("metrics", [])):
        # evidenceId is based on position, not finding order (ADR-007).
        evidence_id = f"ev-metrics-{idx + 1:03d}"
        metric_label = f"{metric.get('namespace', '')}/{metric.get('metricName', '')}"

        try:
            current_value, current_unit, incident_query_meta = _fetch_metric_value(
                cw_client,
                metric,
                incident_start,
                incident_end,
                period,
                apigw_discovery_cache=apigw_discovery_cache,
            )
            baseline_value, _, baseline_query_meta = _fetch_metric_value(
                cw_client,
                metric,
                baseline_start,
                baseline_end,
                period,
                apigw_discovery_cache=apigw_discovery_cache,
            )
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            logger.warning("CloudWatch error for %s: %s", metric_label, error_code)
            errors.append({
                "source": metric_label,
                "reason": f"cloudwatch_error: {error_code}",
            })
            continue
        except Exception as exc:  # noqa: BLE001
            logger.warning("Unexpected error fetching %s: %s", metric_label, exc)
            errors.append({
                "source": metric_label,
                "reason": f"unexpected_error: {exc}",
            })
            continue

        current_imputed_zero = False
        baseline_imputed_zero = False

        if current_value is None and _should_impute_zero_for_sparse_counter(metric):
            current_value = 0.0
            current_unit = "Count"
            current_imputed_zero = True
            logger.debug(
                "metrics_zero_imputed %s",
                _compact_json(
                    {
                        "incidentId": incident_id,
                        "metric": metric_label,
                        "window": "incident",
                        "reason": "sparse_metric_zero_implied",
                        "namespace": metric.get("namespace"),
                        "metricName": metric.get("metricName"),
                        "stat": metric.get("stat"),
                    }
                ),
            )

        if baseline_value is None and _should_impute_zero_for_sparse_counter(metric):
            baseline_value = 0.0
            baseline_imputed_zero = True
            logger.debug(
                "metrics_zero_imputed %s",
                _compact_json(
                    {
                        "incidentId": incident_id,
                        "metric": metric_label,
                        "window": "baseline",
                        "reason": "sparse_metric_zero_implied",
                        "namespace": metric.get("namespace"),
                        "metricName": metric.get("metricName"),
                        "stat": metric.get("stat"),
                    }
                ),
            )

        if _is_error_signal_metric(metric):
            error_signal_diagnostics.append(
                {
                    "metric": metric_label,
                    "currentValue": current_value,
                    "baselineValue": baseline_value,
                    "currentImputedZero": current_imputed_zero,
                    "baselineImputedZero": baseline_imputed_zero,
                    "currentNoDatapoints": current_value is None,
                    "baselineNoDatapoints": baseline_value is None,
                    "stat": metric.get("stat"),
                    "resourceType": _resource_type(metric.get("namespace", "")),
                }
            )

        if current_value is None:
            # No datapoints in incident window — not an error, just no signal.
            is_apigw = metric.get("namespace") == "AWS/ApiGateway"
            is_apigw_latency = _is_apigw_latency_metric(metric)
            no_data_payload = {
                "incidentId": incident_id,
                "metric": metric_label,
                "startTime": incident_start.isoformat(),
                "endTime": incident_end.isoformat(),
                "period": period,
                "dimensions": metric.get("dimensions", {}),
                "stat": metric.get("stat"),
                "resourceType": _resource_type(metric.get("namespace", "")),
                "attemptedDimensions": incident_query_meta.get("attemptedDimensions", []),
                "attemptedStageValues": incident_query_meta.get("attemptedStageValues", []),
                "fallbackUsed": incident_query_meta.get("fallbackUsed", False),
                "discoveredRetryUsed": incident_query_meta.get("discoveredRetryUsed", False),
                "discoveredDimensionCandidates": incident_query_meta.get(
                    "discoveredDimensionCandidates", []
                ),
                "reason": (
                    "no_matching_datapoints_after_apigw_reconciliation"
                    if is_apigw and incident_query_meta.get("discoveredRetryUsed", False)
                    else "no_datapoints_for_exact_metric_dimensions"
                ),
                "hint": (
                    "No data for exact metric+dimension set in window. "
                    "For API Gateway this often means Stage/ApiName mismatch or no traffic."
                    if is_apigw
                    else "No data for exact metric+dimension set in window."
                ),
            }
            logger.debug(
                "No datapoints for %s in incident window",
                metric_label,
                extra={
                    "source": "metrics",
                    "incidentId": incident_id,
                    "metric": metric_label,
                    "startTime": incident_start.isoformat(),
                    "endTime": incident_end.isoformat(),
                    "period": period,
                    "dimensions": metric.get("dimensions", {}),
                    "stat": metric.get("stat"),
                    "resourceType": no_data_payload["resourceType"],
                    "attemptedDimensions": no_data_payload["attemptedDimensions"],
                    "attemptedStageValues": no_data_payload["attemptedStageValues"],
                    "fallbackUsed": no_data_payload["fallbackUsed"],
                    "discoveredRetryUsed": no_data_payload["discoveredRetryUsed"],
                    "discoveredDimensionCandidates": no_data_payload["discoveredDimensionCandidates"],
                    "hint": no_data_payload["hint"],
                },
            )
            logger.debug("metrics_no_datapoints %s", _compact_json(no_data_payload))

            if is_apigw_latency:
                findings.append(
                    _build_metric_collection_gap_finding(
                        evidence_id=evidence_id,
                        metric=metric,
                        service=service,
                        incident_window=incident_window,
                        baseline_window=baseline_window,
                        reason=str(no_data_payload.get("reason") or "no_datapoints"),
                        diagnostics={
                            "attemptedDimensions": no_data_payload.get("attemptedDimensions", []),
                            "attemptedStageValues": no_data_payload.get("attemptedStageValues", []),
                            "discoveredDimensionCandidates": no_data_payload.get(
                                "discoveredDimensionCandidates", []
                            ),
                            "fallbackUsed": no_data_payload.get("fallbackUsed", False),
                            "discoveredRetryUsed": no_data_payload.get("discoveredRetryUsed", False),
                        },
                    )
                )

            metrics_skipped_no_data += 1
            continue

        if baseline_value == 0 and current_value == 0:
            logger.debug(
                "metrics_zero_both_windows %s",
                _compact_json(
                    {
                        "incidentId": incident_id,
                        "metric": metric_label,
                        "reason": "both_windows_zero_or_imputed_zero",
                    }
                ),
            )
            metrics_skipped_below_threshold += 1
            continue

        if baseline_value is None or baseline_value == 0:
            baseline_missing_payload = {
                "incidentId": incident_id,
                "metric": metric_label,
                "baselineValue": baseline_value,
                "currentValue": current_value,
                "baselineAttemptedDimensions": baseline_query_meta.get("attemptedDimensions", []),
                "baselineAttemptedStageValues": baseline_query_meta.get("attemptedStageValues", []),
                "baselineFallbackUsed": baseline_query_meta.get("fallbackUsed", False),
                "baselineDiscoveredRetryUsed": baseline_query_meta.get("discoveredRetryUsed", False),
                "baselineDiscoveredDimensionCandidates": baseline_query_meta.get(
                    "discoveredDimensionCandidates", []
                ),
            }
            logger.debug(
                "Baseline missing/zero for %s",
                metric_label,
                extra={
                    "source": "metrics",
                    "incidentId": incident_id,
                    "metric": metric_label,
                    "baselineValue": baseline_missing_payload["baselineValue"],
                    "currentValue": baseline_missing_payload["currentValue"],
                    "baselineAttemptedDimensions": baseline_missing_payload[
                        "baselineAttemptedDimensions"
                    ],
                    "baselineAttemptedStageValues": baseline_missing_payload[
                        "baselineAttemptedStageValues"
                    ],
                    "baselineFallbackUsed": baseline_missing_payload["baselineFallbackUsed"],
                    "baselineDiscoveredRetryUsed": baseline_missing_payload[
                        "baselineDiscoveredRetryUsed"
                    ],
                    "baselineDiscoveredDimensionCandidates": baseline_missing_payload[
                        "baselineDiscoveredDimensionCandidates"
                    ],
                },
            )
            logger.debug(
                "metrics_baseline_missing %s",
                _compact_json(baseline_missing_payload),
            )
            if current_value > 0:
                # Metric was absent/zero in baseline but present now — flag it
                # at the threshold so scoring is consistent.
                change_ratio = ANOMALY_THRESHOLD
                baseline_absent = True
            else:
                continue
        else:
            change_ratio = current_value / baseline_value
            baseline_absent = False

        threshold = _anomaly_threshold(metric)
        if change_ratio < threshold:
            logger.debug(
                "Metric below anomaly threshold",
                extra={
                    "source": "metrics",
                    "incidentId": incident_id,
                    "metric": metric_label,
                    "changeRatio": round(change_ratio, 4),
                    "threshold": threshold,
                },
            )
            metrics_skipped_below_threshold += 1
            continue

        finding = _build_finding(
            evidence_id=evidence_id,
            metric=metric,
            current_value=current_value,
            baseline_value=baseline_value,
            unit=_normalize_metric_unit(current_unit, metric),
            change_ratio=change_ratio,
            baseline_absent=baseline_absent,
            incident_window=incident_window,
            baseline_window=baseline_window,
            service=service,
        )
        findings.append(finding)

    findings = _deduplicate_metric_findings(findings)

    _append_error_spike_fallback_finding_if_needed(
        findings=findings,
        scope=scope,
        service=service,
        error_signal_diagnostics=error_signal_diagnostics,
    )

    logger.info(
        "metrics_collect_complete",
        extra={
            "source": "metrics",
            "incidentId": incident_id,
            "metricsScanned": metrics_scanned,
            "findingsCount": len(findings),
            "errorsCount": len(errors),
            "skippedNoData": metrics_skipped_no_data,
            "skippedBelowThreshold": metrics_skipped_below_threshold,
        },
    )
    logger.debug(
        "metrics_collect_complete_details %s",
        _compact_json(
            {
                "incidentId": incident_id,
                "metricsScanned": metrics_scanned,
                "findingsCount": len(findings),
                "errorsCount": len(errors),
                "skippedNoData": metrics_skipped_no_data,
                "skippedBelowThreshold": metrics_skipped_below_threshold,
            }
        ),
    )

    return {
        "incidentId": incident_id,
        "source": "metrics",
        "findings": findings,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fetch_metric_value(
    cw_client,
    metric: dict,
    start: datetime,
    end: datetime,
    period: int,
    *,
    apigw_discovery_cache: dict[str, list[dict]] | None = None,
):
    """Fetch a single aggregate metric value over the given time window.

    Returns (value, unit) where value is the float statistic or None if there
    are no datapoints. Using period == window duration returns one aggregated
    datapoint rather than a time series.

    Percentile stats (e.g. "p95", "p99") require ExtendedStatistics; all other
    stats (e.g. "Average", "Sum") use Statistics.
    """
    stat = metric.get("stat", "Average")
    namespace = metric["namespace"]
    metric_name = metric["metricName"]
    dimensions = metric.get("dimensions", {}) if isinstance(metric.get("dimensions"), dict) else {}

    dimension_variants = _build_dimension_variants(namespace, dimensions)
    attempted_dimensions = []
    attempted_stage_values = []
    discovered_dimension_candidates = []
    discovered_retry_used = False
    seen_dimension_keys = set()

    for dims in dimension_variants:
        dim_key = _dimensions_key(dims)
        if dim_key in seen_dimension_keys:
            continue
        seen_dimension_keys.add(dim_key)
        dimensions_list = [{"Name": k, "Value": v} for k, v in dims.items()]
        attempted_dimensions.append(dims)
        if "Stage" in dims:
            if dims["Stage"] not in attempted_stage_values:
                attempted_stage_values.append(dims["Stage"])

        query_period = 60 if _should_use_sparse_counter_1m_aggregation(metric) else period

        common_kwargs = dict(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=dimensions_list,
            StartTime=start,
            EndTime=end,
            Period=query_period,
        )

        if _is_percentile_stat(stat):
            response = cw_client.get_metric_statistics(
                **common_kwargs,
                ExtendedStatistics=[stat],
            )
            datapoints = response.get("Datapoints", [])
            if datapoints:
                dp = datapoints[0]
                value = dp.get("ExtendedStatistics", {}).get(stat)
                return value, dp.get("Unit"), {
                    "attemptedDimensions": attempted_dimensions,
                    "attemptedStageValues": attempted_stage_values,
                    "fallbackUsed": len(attempted_dimensions) > 1,
                    "discoveredRetryUsed": discovered_retry_used,
                    "discoveredDimensionCandidates": discovered_dimension_candidates,
                }
        else:
            response = cw_client.get_metric_statistics(
                **common_kwargs,
                Statistics=[stat],
            )
            datapoints = response.get("Datapoints", [])
            if datapoints:
                dp = datapoints[0]
                if _should_use_sparse_counter_1m_aggregation(metric) and stat == "Sum":
                    aggregate_sum = _sum_stat_across_datapoints(datapoints, stat)
                    logger.debug(
                        "metrics_sparse_counter_window %s",
                        _compact_json(
                            {
                                "metric": f"{namespace}/{metric_name}",
                                "dimensions": dims,
                                "startTime": start.isoformat(),
                                "endTime": end.isoformat(),
                                "period": query_period,
                                "stat": stat,
                                "aggregatedValue": aggregate_sum,
                                "datapoints": _extract_stat_datapoints(datapoints, stat),
                            }
                        ),
                    )
                    return aggregate_sum, dp.get("Unit"), {
                        "attemptedDimensions": attempted_dimensions,
                        "attemptedStageValues": attempted_stage_values,
                        "fallbackUsed": len(attempted_dimensions) > 1,
                        "discoveredRetryUsed": discovered_retry_used,
                        "discoveredDimensionCandidates": discovered_dimension_candidates,
                    }
                return dp.get(stat), dp.get("Unit"), {
                    "attemptedDimensions": attempted_dimensions,
                    "attemptedStageValues": attempted_stage_values,
                    "fallbackUsed": len(attempted_dimensions) > 1,
                    "discoveredRetryUsed": discovered_retry_used,
                    "discoveredDimensionCandidates": discovered_dimension_candidates,
                }

    if namespace == "AWS/ApiGateway":
        discovery_cache_key = f"{metric_name}::{_dimensions_key(dimensions)}"
        if apigw_discovery_cache is not None and discovery_cache_key in apigw_discovery_cache:
            discovered_dimension_candidates = apigw_discovery_cache[discovery_cache_key]
        else:
            discovered_dimension_candidates = _discover_apigw_dimension_candidates(
                cw_client,
                metric_name,
                dimensions,
            )
            if apigw_discovery_cache is not None:
                apigw_discovery_cache[discovery_cache_key] = discovered_dimension_candidates

        for dims in discovered_dimension_candidates[:MAX_APIGW_DIMENSION_RETRIES]:
            dim_key = _dimensions_key(dims)
            if dim_key in seen_dimension_keys:
                continue
            seen_dimension_keys.add(dim_key)

            discovered_retry_used = True
            dimensions_list = [{"Name": k, "Value": v} for k, v in dims.items()]
            attempted_dimensions.append(dims)
            if "Stage" in dims and dims["Stage"] not in attempted_stage_values:
                attempted_stage_values.append(dims["Stage"])

            query_period = 60 if _should_use_sparse_counter_1m_aggregation(metric) else period

            common_kwargs = dict(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=dimensions_list,
                StartTime=start,
                EndTime=end,
                Period=query_period,
            )

            if _is_percentile_stat(stat):
                response = cw_client.get_metric_statistics(
                    **common_kwargs,
                    ExtendedStatistics=[stat],
                )
                datapoints = response.get("Datapoints", [])
                if datapoints:
                    dp = datapoints[0]
                    value = dp.get("ExtendedStatistics", {}).get(stat)
                    return value, dp.get("Unit"), {
                        "attemptedDimensions": attempted_dimensions,
                        "attemptedStageValues": attempted_stage_values,
                        "fallbackUsed": len(attempted_dimensions) > 1,
                        "discoveredRetryUsed": discovered_retry_used,
                        "discoveredDimensionCandidates": discovered_dimension_candidates,
                    }
            else:
                response = cw_client.get_metric_statistics(
                    **common_kwargs,
                    Statistics=[stat],
                )
                datapoints = response.get("Datapoints", [])
                if datapoints:
                    dp = datapoints[0]
                    if _should_use_sparse_counter_1m_aggregation(metric) and stat == "Sum":
                        aggregate_sum = _sum_stat_across_datapoints(datapoints, stat)
                        logger.debug(
                            "metrics_sparse_counter_window %s",
                            _compact_json(
                                {
                                    "metric": f"{namespace}/{metric_name}",
                                    "dimensions": dims,
                                    "startTime": start.isoformat(),
                                    "endTime": end.isoformat(),
                                    "period": query_period,
                                    "stat": stat,
                                    "aggregatedValue": aggregate_sum,
                                    "datapoints": _extract_stat_datapoints(datapoints, stat),
                                }
                            ),
                        )
                        return aggregate_sum, dp.get("Unit"), {
                            "attemptedDimensions": attempted_dimensions,
                            "attemptedStageValues": attempted_stage_values,
                            "fallbackUsed": len(attempted_dimensions) > 1,
                            "discoveredRetryUsed": discovered_retry_used,
                            "discoveredDimensionCandidates": discovered_dimension_candidates,
                        }
                    return dp.get(stat), dp.get("Unit"), {
                        "attemptedDimensions": attempted_dimensions,
                        "attemptedStageValues": attempted_stage_values,
                        "fallbackUsed": len(attempted_dimensions) > 1,
                        "discoveredRetryUsed": discovered_retry_used,
                        "discoveredDimensionCandidates": discovered_dimension_candidates,
                    }

    alias_used = metric.get("_metricNameAliasUsed")
    if (
        alias_used is None
        and namespace == "AWS/ApiGateway"
        and metric_name in {"5XXError", "4XXError", "5xx", "4xx"}
    ):
        alias_metric_name = _apigw_metric_name_alias(metric_name)
        if alias_metric_name and alias_metric_name != metric_name:
            alias_metric = dict(metric)
            alias_metric["metricName"] = alias_metric_name
            alias_metric["_metricNameAliasUsed"] = alias_metric_name
            alias_value, alias_unit, alias_meta = _fetch_metric_value(
                cw_client,
                alias_metric,
                start,
                end,
                period,
                apigw_discovery_cache=apigw_discovery_cache,
            )
            if alias_value is not None:
                alias_meta = dict(alias_meta)
                alias_meta["metricNameAliasUsed"] = alias_metric_name
                logger.debug(
                    "metrics_apigw_metric_name_alias_used %s",
                    _compact_json(
                        {
                            "fromMetricName": metric_name,
                            "toMetricName": alias_metric_name,
                            "startTime": start.isoformat(),
                            "endTime": end.isoformat(),
                        }
                    ),
                )
                return alias_value, alias_unit, alias_meta

    return None, None, {
        "attemptedDimensions": attempted_dimensions,
        "attemptedStageValues": attempted_stage_values,
        "fallbackUsed": len(attempted_dimensions) > 1,
        "discoveredRetryUsed": discovered_retry_used,
        "discoveredDimensionCandidates": discovered_dimension_candidates,
    }


def _build_dimension_variants(namespace: str, dimensions: dict) -> list[dict]:
    """Build query dimension variants for known case-mismatch scenarios.

    For API Gateway metrics, Stage dimension casing can differ from what is
    stored in incident context. We first try the exact descriptor, then safe
    casing variants (e.g. Prod <-> prod) if a Stage dimension is present.
    """
    if not isinstance(dimensions, dict):
        return [{}]

    base = {str(k): str(v) for k, v in dimensions.items()}
    variants = [base]

    if namespace != "AWS/ApiGateway":
        return variants

    stage = base.get("Stage")
    if not isinstance(stage, str) or stage.strip() == "":
        return variants

    stage = stage.strip()
    candidate_values = []
    for candidate in (stage, stage.lower(), stage.upper(), stage.capitalize(), "$default"):
        if candidate not in candidate_values:
            candidate_values.append(candidate)

    for candidate in candidate_values:
        if candidate == stage:
            continue
        variant = dict(base)
        variant["Stage"] = candidate
        variants.append(variant)

    return variants


def _discover_apigw_dimension_candidates(cw_client, metric_name: str, original_dimensions: dict) -> list[dict]:
    """Discover candidate dimension sets for API Gateway metric reconciliation.

    Uses bounded list_metrics calls and returns candidate dimensions ordered by
    likely relevance to the original descriptor.
    """
    original = original_dimensions if isinstance(original_dimensions, dict) else {}
    original_api_name = str(original.get("ApiName", "")).strip().lower()
    original_api_id = str(original.get("ApiId", "")).strip().lower()
    original_stage = str(original.get("Stage", "")).strip().lower()

    candidates_with_score: list[tuple[int, dict]] = []

    filtered_dimensions = None
    if original_api_name:
        filtered_dimensions = [{"Name": "ApiName", "Value": str(original.get("ApiName", ""))}]
    elif original_api_id:
        filtered_dimensions = [{"Name": "ApiId", "Value": str(original.get("ApiId", ""))}]

    filtered_candidates = _list_apigw_metric_dimensions(
        cw_client,
        metric_name,
        dimensions_filter=filtered_dimensions,
    )
    fallback_candidates = []
    # For API Gateway latency metrics, include an unfiltered discovery pass even
    # when ApiName-filtered candidates exist. Some accounts expose latency
    # series primarily under ApiId-based dimensions.
    should_include_unfiltered_discovery = (
        metric_name in {"Latency", "IntegrationLatency"}
        or (not filtered_candidates and filtered_dimensions is not None)
    )
    if should_include_unfiltered_discovery:
        fallback_candidates = _list_apigw_metric_dimensions(
            cw_client,
            metric_name,
            dimensions_filter=None,
        )

    for dims in filtered_candidates + fallback_candidates:
        candidate_api_name = str(dims.get("ApiName", "")).strip().lower()
        candidate_api_id = str(dims.get("ApiId", "")).strip().lower()
        candidate_stage = str(dims.get("Stage", "")).strip().lower()

        score = 0
        if original_api_name and candidate_api_name == original_api_name:
            score += 2
        if original_api_id and candidate_api_id == original_api_id:
            score += 2
        if original_stage and candidate_stage == original_stage:
            score += 2
        if "ApiName" in dims:
            score += 1
        if "ApiId" in dims:
            score += 1
        if "Stage" in dims:
            score += 1

        candidates_with_score.append((score, dims))

    # Order by relevance score then by deterministic key.
    candidates_with_score.sort(
        key=lambda t: (-t[0], _dimensions_key(t[1]))
    )

    deduped: list[dict] = []
    seen = set()
    for _, dims in candidates_with_score:
        key = _dimensions_key(dims)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dims)

    logger.debug(
        "metrics_apigw_dimension_discovery_result %s",
        _compact_json(
            {
                "metricName": metric_name,
                "originalDimensions": original,
                "discoveryFilter": filtered_dimensions,
                "candidateCount": len(deduped),
                "topCandidates": deduped[:3],
            }
        ),
    )

    return deduped[:MAX_APIGW_DIMENSION_RETRIES]


def _list_apigw_metric_dimensions(
    cw_client,
    metric_name: str,
    *,
    dimensions_filter: list[dict] | None,
) -> list[dict]:
    discovered: list[dict] = []
    next_token = None

    for _ in range(MAX_APIGW_DIMENSION_DISCOVERY_PAGES):
        req = {
            "Namespace": "AWS/ApiGateway",
            "MetricName": metric_name,
        }
        if dimensions_filter:
            req["Dimensions"] = dimensions_filter
        if next_token:
            req["NextToken"] = next_token

        try:
            response = cw_client.list_metrics(**req)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "Unknown")
            logger.warning(
                "metrics_apigw_dimension_discovery_failed %s",
                _compact_json(
                    {
                        "metricName": metric_name,
                        "request": req,
                        "errorCode": error_code,
                        "reason": "CloudWatch ListMetrics failed while attempting API Gateway dimension reconciliation.",
                    }
                ),
            )
            break

        for metric in response.get("Metrics", []):
            dims_list = metric.get("Dimensions", [])
            if not isinstance(dims_list, list):
                continue
            dims = {
                str(d.get("Name")): str(d.get("Value"))
                for d in dims_list
                if isinstance(d, dict) and d.get("Name") and d.get("Value")
            }
            if dims:
                discovered.append(dims)

        next_token = response.get("NextToken")
        if not next_token:
            break

    return discovered


def _dimensions_key(dimensions: dict) -> str:
    if not isinstance(dimensions, dict):
        return ""
    return "|".join(
        f"{k}={v}"
        for k, v in sorted((str(k), str(v)) for k, v in dimensions.items())
    )


def _compact_json(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _should_impute_zero_for_sparse_counter(metric: dict) -> bool:
    namespace = str(metric.get("namespace", ""))
    metric_name = str(metric.get("metricName", ""))
    stat = str(metric.get("stat", "Average"))
    return (namespace, metric_name, stat) in SPARSE_COUNTER_METRICS_ZERO_IMPUTE


def _anomaly_threshold(metric: dict) -> float:
    """Return metric-specific anomaly threshold.

    API Gateway latency surfaces often show meaningful regressions below 1.5x
    in short windows; use a slightly more sensitive threshold there.
    """
    namespace = str(metric.get("namespace", ""))
    metric_name = str(metric.get("metricName", ""))
    if namespace == "AWS/ApiGateway" and metric_name in {"Latency", "IntegrationLatency"}:
        return APIGW_LATENCY_ANOMALY_THRESHOLD
    return ANOMALY_THRESHOLD


def _apigw_metric_name_alias(metric_name: str) -> str | None:
    aliases = {
        "5XXError": "5xx",
        "4XXError": "4xx",
        "5xx": "5XXError",
        "4xx": "4XXError",
    }
    return aliases.get(metric_name)


def _should_use_sparse_counter_1m_aggregation(metric: dict) -> bool:
    return _should_impute_zero_for_sparse_counter(metric)


def _sum_stat_across_datapoints(datapoints: list[dict], stat: str) -> float:
    return float(sum(float(dp.get(stat, 0.0) or 0.0) for dp in datapoints if isinstance(dp, dict)))


def _extract_stat_datapoints(datapoints: list[dict], stat: str) -> list[dict]:
    extracted = []
    for dp in datapoints:
        if not isinstance(dp, dict):
            continue
        extracted.append(
            {
                "Timestamp": str(dp.get("Timestamp")),
                stat: dp.get(stat),
            }
        )
    extracted.sort(key=lambda x: x.get("Timestamp", ""))
    return extracted


def _is_error_signal_metric(metric: dict) -> bool:
    metric_name = str(metric.get("metricName", ""))
    return metric_name in ERROR_SIGNAL_METRIC_NAMES


def _is_apigw_latency_metric(metric: dict) -> bool:
    return (
        str(metric.get("namespace", "")) == "AWS/ApiGateway"
        and str(metric.get("metricName", "")) in {"Latency", "IntegrationLatency"}
    )


def _append_error_spike_fallback_finding_if_needed(
    *,
    findings: list,
    scope: dict,
    service: str,
    error_signal_diagnostics: list[dict],
) -> None:
    signal_type = str(scope.get("signalType", "")).strip()
    if signal_type != "error_spike":
        return

    has_error_rate_finding = any(
        isinstance(f, dict) and f.get("findingType") == "error_rate_spike"
        for f in findings
    )
    if has_error_rate_finding:
        return

    fallback_finding = {
        "evidenceId": "ev-metrics-fallback-001",
        "source": "metrics",
        "resourceType": "service",
        "resourceName": service,
        "findingType": "error_signal_not_confirmed_by_metrics",
        "summary": "Trigger signalType=error_spike but metrics did not confirm an error-rate spike in the selected windows.",
        "score": 0.15,
        "details": {
            "signalType": signal_type,
            "diagnosticType": "fallback_non_confirming_metrics",
            "checkedErrorMetrics": error_signal_diagnostics,
        },
    }
    findings.append(fallback_finding)
    logger.debug(
        "metrics_error_spike_fallback_finding %s",
        _compact_json(
            {
                "incidentId": scope.get("incidentId"),
                "findingType": fallback_finding["findingType"],
                "checkedErrorMetricsCount": len(error_signal_diagnostics),
            }
        ),
    )


def _build_metric_collection_gap_finding(
    *,
    evidence_id: str,
    metric: dict,
    service: str,
    incident_window: dict,
    baseline_window: dict,
    reason: str,
    diagnostics: dict,
) -> dict:
    metric_name = str(metric.get("metricName", ""))
    namespace = str(metric.get("namespace", ""))
    stat = str(metric.get("stat", "Average"))
    return {
        "evidenceId": evidence_id,
        "source": "metrics",
        "resourceType": _resource_type(namespace),
        "resourceName": _resource_name(metric, service),
        "findingType": "metric_collection_gap",
        "summary": (
            f"{metric_name} ({stat}) had no datapoints after API Gateway dimension reconciliation "
            "[metric_collection_gap]"
        ),
        "score": 0.05,
        "details": {
            "metricName": metric_name,
            "namespace": namespace,
            "stat": stat,
            "currentValue": None,
            "baselineValue": None,
            "unit": _default_unit_for_metric(metric),
            "changeRatio": None,
            "baselineAbsent": True,
            "incidentWindow": incident_window,
            "baselineWindow": baseline_window,
            "diagnosticType": "collection_gap",
            "collectionGapReason": reason,
            "attemptedDimensions": diagnostics.get("attemptedDimensions", []),
            "attemptedStageValues": diagnostics.get("attemptedStageValues", []),
            "discoveredDimensionCandidates": diagnostics.get(
                "discoveredDimensionCandidates", []
            ),
            "fallbackUsed": bool(diagnostics.get("fallbackUsed", False)),
            "discoveredRetryUsed": bool(diagnostics.get("discoveredRetryUsed", False)),
        },
    }


def _build_finding(
    evidence_id: str,
    metric: dict,
    current_value: float,
    baseline_value,
    unit: str,
    change_ratio: float,
    baseline_absent: bool,
    incident_window: dict,
    baseline_window: dict,
    service: str,
) -> dict:
    """Construct a single EvidenceFinding dict (see schemas/evidence.schema.json).

    Raw CloudWatch values are stored as-is with their unit string. No implicit
    unit conversion (e.g. seconds→ms) is applied here; consumers are responsible
    for interpreting the unit field.
    """
    metric_name = metric.get("metricName", "")
    namespace = metric.get("namespace", "")
    stat = metric.get("stat", "Average")

    score = _score_baseline_absent(metric, current_value) if baseline_absent else _score(change_ratio)

    return {
        "evidenceId": evidence_id,
        "source": "metrics",
        "resourceType": _resource_type(namespace),
        "resourceName": _resource_name(metric, service),
        "findingType": _finding_type(metric_name),
        "summary": _build_summary(metric_name, stat, change_ratio, baseline_absent),
        "score": score,
        "details": {
            "metricName": metric_name,
            "namespace": namespace,
            "stat": stat,
            "currentValue": current_value,
            "baselineValue": baseline_value,
            "unit": unit,
            "changeRatio": None if baseline_absent else round(change_ratio, 4),
            "baselineAbsent": baseline_absent,
            "incidentWindow": incident_window,
            "baselineWindow": baseline_window,
        },
    }


def _is_percentile_stat(stat: str) -> bool:
    """Return True for CloudWatch extended (percentile) stats, e.g. 'p95', 'p99.9'."""
    return stat.startswith("p") and stat[1:].replace(".", "").isdigit()


def _score(change_ratio: float) -> float:
    """Map change ratio to a 0–1 anomaly score; caps at 1.0 above 5x deviation."""
    return round(min(change_ratio / 5.0, 1.0), 4)


def _score_baseline_absent(metric: dict, current_value: float) -> float:
    """Score sparse baseline-absent findings by absolute incident magnitude.

    Keeps a floor of 0.30 (historical behavior) while allowing high incident
    values to score higher, so large baseline-zero spikes are not flattened.
    """
    if _should_impute_zero_for_sparse_counter(metric):
        return round(max(0.30, min(float(current_value) / 50.0, 1.0)), 4)
    return 0.30


def _default_unit_for_metric(metric: dict) -> str:
    if _should_impute_zero_for_sparse_counter(metric):
        return "Count"
    return "None"


def _normalize_metric_unit(unit, metric: dict) -> str:
    """Normalize CW unit values, treating null-like values as missing.

    CloudWatch may omit unit or return a literal "None" string for some
    sparse API Gateway counters. In both cases we apply deterministic defaults.
    """
    unit_text = str(unit).strip() if unit is not None else ""
    if not unit_text or unit_text.lower() == "none":
        return _default_unit_for_metric(metric)
    return unit_text


def _finding_type(metric_name: str) -> str:
    return FINDING_TYPE_MAP.get(metric_name, "metric_spike")


def _resource_type(namespace: str) -> str:
    return RESOURCE_TYPE_MAP.get(namespace, "aws-resource")


def _resource_name(metric: dict, service: str) -> str:
    """First dimension value as resource name; fall back to the service name."""
    dimensions = metric.get("dimensions", {})
    if dimensions:
        return next(iter(dimensions.values()))
    return service


def _build_summary(metric_name: str, stat: str, change_ratio: float, baseline_absent: bool) -> str:
    finding_type = _finding_type(metric_name)
    if baseline_absent:
        return (
            f"{metric_name} ({stat}) present in incident window with baseline at zero "
            f"[{finding_type}]"
        )
    return f"{metric_name} ({stat}) changed {change_ratio:.1f}x vs baseline [{finding_type}]"


def _deduplicate_metric_findings(findings: list[dict]) -> list[dict]:
    deduped = []
    seen = set()
    for finding in findings:
        details = finding.get("details", {}) if isinstance(finding.get("details"), dict) else {}
        key = (
            finding.get("resourceType"),
            finding.get("resourceName"),
            finding.get("findingType"),
            details.get("namespace"),
            details.get("metricName"),
            details.get("stat"),
            details.get("incidentWindow", {}).get("start")
            if isinstance(details.get("incidentWindow"), dict)
            else None,
            details.get("incidentWindow", {}).get("end")
            if isinstance(details.get("incidentWindow"), dict)
            else None,
            details.get("baselineWindow", {}).get("start")
            if isinstance(details.get("baselineWindow"), dict)
            else None,
            details.get("baselineWindow", {}).get("end")
            if isinstance(details.get("baselineWindow"), dict)
            else None,
            details.get("currentValue"),
            details.get("baselineValue"),
        )
        if key in seen:
            logger.debug(
                "metrics_duplicate_finding_dropped %s",
                _compact_json(
                    {
                        "droppedEvidenceId": finding.get("evidenceId"),
                        "resourceName": finding.get("resourceName"),
                        "metricName": details.get("metricName"),
                    }
                ),
            )
            continue
        seen.add(key)
        deduped.append(finding)
    return deduped


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _round_period(duration_seconds: float) -> int:
    """Round duration up to the nearest multiple of 60 (CloudWatch requirement)."""
    return max(60, math.ceil(duration_seconds / 60) * 60)
