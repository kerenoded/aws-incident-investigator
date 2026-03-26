"""
Candidate Hypotheses Builder Lambda handler.

Receives a scope and worker outputs (keyed by source name) from Step Functions,
builds candidate hypotheses deterministically, and returns the result.

Storage to S3 is optional and secondary — the core behavior is building and
returning the hypothesis envelope. If S3_BUCKET is set, hypotheses.json is
written under investigations/<incidentId>/. Step Functions dependency: #4.

Environment variables:
    S3_BUCKET  (optional) S3 bucket name for writing hypotheses.json.
                          If unset, the S3 write is skipped silently.
"""

import json
import logging
import os

import boto3

from hypotheses_builder import build_hypotheses

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def lambda_handler(event: dict, context) -> dict:
    """Candidate Hypotheses Builder Lambda entry point.

    Args:
        event:   Dict with keys:
                   - "scope":         Scope dict from the Scope Builder.
                   - "workerOutputs": Dict keyed by source name
                                      ("metrics", "logs", "traces").
        context: Lambda context (unused).

    Returns:
        Hypothesis envelope: { incidentId, hypotheses: [...] }
        Conforms to schemas/hypothesis.schema.json (per-item) and
        the envelope shape described in docs/INTERFACES.md §4.
    """
    scope = event["scope"]
    worker_outputs = event.get("workerOutputs", {})

    _require_usable_evidence(worker_outputs)

    result = build_hypotheses(scope, worker_outputs)

    bucket = os.environ.get("S3_BUCKET")
    if bucket:
        incident_id = result["incidentId"]
        key = f"investigations/{incident_id}/hypotheses.json"
        try:
            s3_client = boto3.client("s3")
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(result, indent=2),
                ContentType="application/json",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to write hypotheses.json to S3: %s", exc)

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_WORKER_SOURCES = ("metrics", "logs", "traces")


def _require_usable_evidence(worker_outputs: dict) -> None:
    """Raise if every worker errored and none produced any findings.

    A single finding from any worker is enough to proceed. Workers that
    returned no findings AND no errors (e.g., X-Ray disabled, empty window)
    are treated as absent — not as failures — and do not contribute to the
    all-errored check.

    Raises RuntimeError so the existing Catch → SaveFailedStatus path in
    Step Functions routes the execution to InvestigationFailed.
    """
    for source in _WORKER_SOURCES:
        if worker_outputs.get(source, {}).get("findings"):
            return  # at least one finding — proceed

    all_errored = all(
        bool(worker_outputs.get(src, {}).get("errors"))
        for src in _WORKER_SOURCES
    )
    if all_errored:
        raise RuntimeError(
            "All evidence workers reported errors with no findings — "
            "investigation cannot produce a meaningful report."
        )
