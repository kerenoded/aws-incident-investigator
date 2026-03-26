"""
AI Hypothesis Evaluator Lambda handler.

Receives the full Step Functions state, extracts scope/workerOutputs/hypotheses,
calls the evaluator, and returns the structured AI evaluation dict.

On any failure (Bedrock error, malformed output, missing state keys) this
handler returns {} so the workflow continues without the AI result.

Step Functions dependency: follows CandidateHypothesesBuilder.
result_path in workflow.py: "$.aiEvaluation"

Environment variables:
    BEDROCK_MODEL_ID  Bedrock model to invoke. Defaults to Amazon Nova Micro.
    S3_BUCKET         (unused here — present for consistency with other Lambdas)
"""

import logging
import os

from evaluator import evaluate

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

_DEFAULT_MODEL_ID = "eu.amazon.nova-micro-v1:0"


def lambda_handler(event: dict, context) -> dict:
    """AI Hypothesis Evaluator Lambda entry point.

    Args:
        event:   Full Step Functions state dict. Expected keys:
                   - "scope":         Scope dict from the Scope Builder.
                   - "workerOutputs": Dict keyed by source name.
                   - "hypotheses":    Hypothesis envelope from the
                                      Candidate Hypotheses Builder.
        context: Lambda context (unused).

    Returns:
        Evaluation dict: { topHypothesis, assessments, missingEvidence }
        On AI unavailability, returns { unavailableReason: "..." } so the
        workflow continues while preserving explainability.
    """
    incident_id = event.get("incidentId", "unknown")
    logger.info(
        "Starting AI hypothesis evaluation",
        extra={"step": "ai_hypothesis_evaluator", "action": "start", "incidentId": incident_id},
    )

    scope = event.get("scope", {})
    worker_outputs = event.get("workerOutputs", {})
    hypotheses = event.get("hypotheses", {"hypotheses": []})
    model_id = os.environ.get("BEDROCK_MODEL_ID", _DEFAULT_MODEL_ID)

    hypothesis_count = len(hypotheses.get("hypotheses", []))
    evidence_summary_count = sum(
        len(worker_outputs.get(src, {}).get("findings", []))
        for src in ("metrics", "logs", "traces")
    )
    logger.info(
        "Evaluating hypotheses",
        extra={
            "step": "ai_hypothesis_evaluator",
            "action": "prompt_inputs",
            "incidentId": incident_id,
            "service": scope.get("service"),
            "hypothesisCount": hypothesis_count,
            "evidenceSummaryCount": evidence_summary_count,
            "modelId": model_id,
        },
    )

    try:
        result = evaluate(scope, worker_outputs, hypotheses, model_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "AI hypothesis evaluator failed — returning empty evaluation",
            extra={"step": "ai_hypothesis_evaluator", "action": "fallback", "error": str(exc)},
        )
        return {"unavailableReason": "ai_evaluator_exception"}

    if result and result.get("assessments"):
        result["modelId"] = model_id
        logger.info(
            "AI hypothesis evaluation complete",
            extra={
                "step": "ai_hypothesis_evaluator",
                "action": "complete",
                "incidentId": incident_id,
                "topHypothesis": result.get("topHypothesis"),
                "assessmentCount": len(result.get("assessments", [])),
                "nextBestActionsCount": len(result.get("nextBestActions", [])) if isinstance(result, dict) else 0,
                "modelId": model_id,
            },
        )
    else:
        logger.warning(
            "AI hypothesis evaluation returned empty result",
            extra={
                "step": "ai_hypothesis_evaluator",
                "action": "empty",
                "incidentId": incident_id,
                "unavailableReason": result.get("unavailableReason") if isinstance(result, dict) else None,
            },
        )

    return result
