"""
Final Report Builder Lambda handler.

Receives all investigation outputs from Step Functions, builds the final
incident report deterministically, and returns the result.

Storage to S3 is optional and secondary — the core behavior is building and
returning the report payload. If S3_BUCKET is set, final-report.json is
written under investigations/<incidentId>/. Step Functions dependency: #4.

Environment variables:
    S3_BUCKET  (optional) S3 bucket name for writing final-report.json.
                          If unset, the S3 write is skipped silently.
"""

import json
import logging
import os

import boto3

from report_builder import build_report

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

_DEFAULT_PAYLOAD_WARNING_THRESHOLD_BYTES = 180 * 1024


def lambda_handler(event: dict, context) -> dict:
    """Final Report Builder Lambda entry point.

    Args:
        event:   Dict with keys:
                   - "scope":           Scope dict from the Scope Builder.
                   - "workerOutputs":   Dict keyed by source name
                                        ("metrics", "logs", "traces").
                   - "hypotheses":      Hypothesis envelope from the
                                        Candidate Hypotheses Builder.
                   - "aiEvaluation":    Optional AI evaluation output (null-safe).
        context: Lambda context (unused).

    Returns:
        Final report dict conforming to schemas/final-report.schema.json.
    """
    scope = event["scope"]
    worker_outputs = event.get("workerOutputs", {})
    hypotheses = event.get("hypotheses", {"hypotheses": []})
    ai_evaluation = event.get("aiEvaluation")

    result = build_report(scope, worker_outputs, hypotheses, ai_evaluation)
    _validate_final_report(result)
    _warn_if_payload_large(
        payload=result,
        incident_id=result["incidentId"],
        step="final_report_builder",
    )

    bucket = os.environ.get("S3_BUCKET")
    if bucket:
        incident_id = result["incidentId"]
        key = f"investigations/{incident_id}/final-report.json"
        # Do NOT swallow S3 errors — if the report is not persisted, subsequent
        # API reads will return 500 while Step Functions shows SUCCEEDED, which
        # is a silent data-loss failure.  Let the exception propagate so
        # Step Functions can catch it via the FinalReportBuilder Catch → SaveFailedStatus.
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(result, indent=2),
            ContentType="application/json",
        )

    return result


def _validate_final_report(report: dict) -> None:
    """Lightweight runtime validation for the final report contract boundary."""
    if not isinstance(report, dict):
        raise ValueError("Final report must be a JSON object.")

    for field in ("incidentId", "summary", "topHypotheses", "evidenceHighlights"):
        if field not in report:
            raise ValueError(f"Final report missing required field: {field}")

    if not isinstance(report["incidentId"], str) or report["incidentId"].strip() == "":
        raise ValueError("Final report incidentId must be a non-empty string.")
    if not isinstance(report["summary"], str) or report["summary"].strip() == "":
        raise ValueError("Final report summary must be a non-empty string.")
    if not isinstance(report["topHypotheses"], list):
        raise ValueError("Final report topHypotheses must be a list.")
    if not isinstance(report["evidenceHighlights"], list):
        raise ValueError("Final report evidenceHighlights must be a list.")


def _payload_warning_threshold_bytes() -> int:
    raw = os.environ.get(
        "PAYLOAD_WARNING_THRESHOLD_BYTES",
        str(_DEFAULT_PAYLOAD_WARNING_THRESHOLD_BYTES),
    )
    try:
        value = int(raw)
    except ValueError:
        value = _DEFAULT_PAYLOAD_WARNING_THRESHOLD_BYTES
    return max(1, value)


def _warn_if_payload_large(*, payload: dict, incident_id: str, step: str) -> None:
    payload_bytes = len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    warning_threshold_bytes = _payload_warning_threshold_bytes()
    if payload_bytes >= warning_threshold_bytes:
        logger.warning(
            "Payload size warning threshold reached",
            extra={
                "incidentId": incident_id,
                "step": step,
                "action": "payload_size_warning",
                "payloadBytes": payload_bytes,
                "warningThresholdBytes": warning_threshold_bytes,
            },
        )
