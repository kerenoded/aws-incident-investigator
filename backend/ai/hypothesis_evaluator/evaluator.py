"""
AI Hypothesis Evaluator — core logic.

Builds a concise structured prompt from deterministic hypotheses and
top evidence summaries, calls Amazon Bedrock, and parses the structured
JSON response.

AI output contract (strict — see docs/AI_DESIGN.md):
{
    "topHypothesis": "<cause string>",
    "assessments": [{ "cause": "...", "plausibility": 0.0–1.0, "reason": "..." }],
    "missingEvidence": ["..."],
    "nextBestActions": [
        {
            "action": "...",
            "why": "...",
            "evidenceIds": ["ev-logs-001"],
            "expectedSignal": "...",
            "confidence": 0.0
        }
    ]
}

On any failure (Bedrock error, malformed JSON, missing keys) this module
returns {} so the workflow can continue without the AI result.
"""

import json
import logging
import re

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

_EVIDENCE_ID_PATTERN = re.compile(r"^ev-[a-z0-9]+-\d+$")

# Maximum number of finding summaries to include per source in the prompt.
_MAX_FINDINGS_PER_SOURCE = 3

# Safety limits for free-text hypothesis fields interpolated into the prompt.
_MAX_CAUSE_LEN = 200
_MAX_RATIONALE_LEN = 500
_RAW_PREVIEW_CHARS = 300
_INITIAL_MAX_TOKENS = 512
_RETRY_MAX_TOKENS = 1024

_SYSTEM_PROMPT = (
    "You are an AWS incident investigation assistant. "
    "You receive structured evidence and candidate hypotheses about a production incident. "
    "Respond with ONLY a single JSON object — no prose, no markdown, no code fences. "
    'The JSON must include exactly three required keys: "topHypothesis" (string), '
    '"assessments" (array of objects each with "cause", "plausibility", "reason"), '
    'and "missingEvidence" (array of strings). '
    'You may optionally include a fourth key: "nextBestActions" (array, max 3) where '
    "each item has: action, why, evidenceIds, expectedSignal, confidence. "
    "Plausibility values must be between 0.0 and 1.0. "
    "nextBestActions confidence values must be between 0.0 and 1.0. "
    "Do not add any keys beyond these. "
    "In each assessment, echo the exact cause string from the candidate hypotheses — "
    "do not rephrase, merge, or invent alternative cause labels. "
    "When evidence includes named resources (service names, API names, Lambda functions, DynamoDB, etc.), "
    "each assessment reason should mention at least one concrete resource name. "
    'The "missingEvidence" array must contain human-readable descriptions of evidence '
    "types that are NOT currently available but would help confirm or rule out the hypotheses — "
    "keep this specific to the incident context and avoid generic boilerplate suggestions. "
    "Do NOT list evidence reference IDs (such as ev-logs-001) in missingEvidence. "
    "Keep each assessment reason concise (prefer 1 sentence). "
    "Return at most 3 missingEvidence items and at most 3 nextBestActions items."
)


def evaluate(scope: dict, worker_outputs: dict, hypotheses: dict, model_id: str) -> dict:
    """Run the AI hypothesis evaluation.

    Args:
        scope:          Scope dict (incidentId, service, incidentWindow).
        worker_outputs: Worker output dict keyed by source name.
        hypotheses:     Hypothesis envelope { incidentId, hypotheses: [...] }.
        model_id:       Bedrock model ID to invoke.

    Returns:
        Parsed evaluation dict on success.
        On failure, returns {"unavailableReason": "..."}.
    """
    prompt = _build_prompt(scope, worker_outputs, hypotheses)
    logger.debug(
        "Evaluator prepared prompt: incidentId=%s service=%s hypotheses=%s promptChars=%s",
        scope.get("incidentId", "unknown"),
        scope.get("service", "unknown"),
        len(hypotheses.get("hypotheses", [])),
        len(prompt),
    )
    raw = _call_bedrock(prompt, model_id, max_tokens=_INITIAL_MAX_TOKENS)
    if raw is None:
        return {"unavailableReason": "bedrock_call_failed"}

    logger.debug("Evaluator received Bedrock response text: responseChars=%s", len(raw))
    available_evidence_ids = _collect_evidence_ids_set(worker_outputs)

    parsed = _parse_response(raw)
    if parsed:
        _filter_next_best_actions_to_available_evidence(parsed, available_evidence_ids)
        assessments_summary = _summarize_assessments_for_log(parsed.get("assessments", []))
        logger.debug(
            "AI evaluator parse success: topHypothesis=%s assessments=%s missingEvidence=%s nextBestActions=%s assessmentSummary=%s",
            parsed.get("topHypothesis"),
            len(parsed.get("assessments", [])),
            len(parsed.get("missingEvidence", [])),
            len(parsed.get("nextBestActions", [])),
            assessments_summary,
        )
        return parsed

    if _looks_like_truncated_json(raw):
        logger.warning(
            "AI evaluator detected likely truncated JSON response, retrying Bedrock with higher token limit",
            extra={
                "action": "retry_bedrock",
                "initialMaxTokens": _INITIAL_MAX_TOKENS,
                "retryMaxTokens": _RETRY_MAX_TOKENS,
            },
        )
        retry_raw = _call_bedrock(prompt, model_id, max_tokens=_RETRY_MAX_TOKENS)
        if retry_raw is not None:
            logger.debug("Evaluator received Bedrock retry response text: responseChars=%s", len(retry_raw))
            retry_parsed = _parse_response(retry_raw)
            if retry_parsed:
                _filter_next_best_actions_to_available_evidence(
                    retry_parsed, available_evidence_ids
                )
                retry_assessment_summary = _summarize_assessments_for_log(
                    retry_parsed.get("assessments", [])
                )
                logger.debug(
                    "AI evaluator parse success after retry: topHypothesis=%s assessments=%s missingEvidence=%s nextBestActions=%s assessmentSummary=%s",
                    retry_parsed.get("topHypothesis"),
                    len(retry_parsed.get("assessments", [])),
                    len(retry_parsed.get("missingEvidence", [])),
                    len(retry_parsed.get("nextBestActions", [])),
                    retry_assessment_summary,
                )
                return retry_parsed

    return {"unavailableReason": "invalid_ai_response"}


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def _build_prompt(scope: dict, worker_outputs: dict, hypotheses: dict) -> str:
    """Build a concise structured prompt.

    Sends only hypothesis metadata and finding summary strings — never raw
    details objects or full log lines. Keeps the prompt well under token limits
    and avoids forwarding sensitive telemetry to the model.
    """
    service = scope.get("service", "unknown")
    window = scope.get("incidentWindow", {})
    window_str = f"{window.get('start', '?')} – {window.get('end', '?')}"

    lines = [
        f"Incident: service={service}, window={window_str}",
        "",
        "Candidate hypotheses:",
    ]
    for h in hypotheses.get("hypotheses", []):
        cause = _sanitize_text(h.get("cause"), _MAX_CAUSE_LEN)
        rationale = _sanitize_text(h.get("rationale"), _MAX_RATIONALE_LEN)
        lines.append(
            f"  - id={h.get('id')} cause=\"{cause}\" "
            f"confidence={h.get('confidence', 0):.2f} "
            f"evidence_ids={h.get('supportingEvidenceIds', [])}"
        )
        lines.append(f"    rationale: {rationale}")

    lines.append("")
    lines.append("Evidence summaries (top findings per source):")
    for source in ("metrics", "logs", "traces"):
        findings = _canonicalize_findings(worker_outputs.get(source, {}).get("findings", []))
        top = sorted(findings, key=lambda f: f.get("score", 0.0), reverse=True)
        top = top[:_MAX_FINDINGS_PER_SOURCE]
        for f in top:
            lines.append(f"  [{source}] {f.get('summary', '')}")

    evidence_ids = _collect_evidence_ids(worker_outputs)
    if evidence_ids:
        lines.append("")
        lines.append("Available evidence IDs for citation in nextBestActions:")
        lines.append(f"  {evidence_ids}")

    lines.append("")
    lines.append(
        "Evaluate the hypotheses. Return a single JSON object with keys: "
        '"topHypothesis", "assessments", "missingEvidence", and optional "nextBestActions". '
        "Use the exact cause strings from the candidates above in each assessment — no rephrasing. "
        "When available, mention at least one concrete resource name in each assessment reason. "
        'For "missingEvidence", list human-readable descriptions of evidence types not currently '
        "available that would strengthen the analysis — not evidence IDs. "
        'If you include "nextBestActions", provide at most 3 concise, operator-actionable steps and '
        "cite only evidence IDs from the available list. "
        "Keep assessment reasons concise and avoid long narratives."
    )
    return "\n".join(lines)


def _collect_evidence_ids(worker_outputs: dict) -> str:
    """Collect and serialize available evidence IDs for prompt grounding."""
    return ", ".join(_collect_evidence_ids_list(worker_outputs))


def _collect_evidence_ids_set(worker_outputs: dict) -> set[str]:
    """Collect available evidence IDs as a set for response-time validation."""
    return set(_collect_evidence_ids_list(worker_outputs))


def _collect_evidence_ids_list(worker_outputs: dict) -> list[str]:
    """Collect available evidence IDs in deterministic order (deduped)."""
    ids: list[str] = []
    for source in ("metrics", "logs", "traces"):
        findings = _canonicalize_findings(worker_outputs.get(source, {}).get("findings", []))
        for finding in findings:
            evidence_id = finding.get("evidenceId")
            if isinstance(evidence_id, str) and evidence_id and evidence_id not in ids:
                ids.append(evidence_id)
    return ids


def _filter_next_best_actions_to_available_evidence(
    parsed: dict,
    available_evidence_ids: set[str],
) -> None:
    """Drop unavailable evidence IDs from AI next actions.

    This prevents mirrored/deduped IDs from leaking into final output.
    """
    actions = parsed.get("nextBestActions")
    if not isinstance(actions, list):
        return

    filtered_actions: list[dict] = []
    for action in actions:
        if not isinstance(action, dict):
            continue
        evidence_ids = action.get("evidenceIds")
        if not isinstance(evidence_ids, list):
            continue
        allowed_ids = [ev for ev in evidence_ids if ev in available_evidence_ids]
        if not allowed_ids:
            continue
        filtered = dict(action)
        filtered["evidenceIds"] = allowed_ids
        filtered_actions.append(filtered)

    if filtered_actions:
        parsed["nextBestActions"] = filtered_actions
    else:
        parsed.pop("nextBestActions", None)


def _sanitize_text(text: str | None, max_length: int) -> str:
    """Sanitize free-text fields before interpolating them into a prompt.

    Replaces newlines and control characters with spaces to prevent prompt
    injection via hypothesis cause or rationale strings, then truncates.
    """
    if not isinstance(text, str):
        return ""
    sanitized = text.replace("\n", " ").replace("\r", " ")
    sanitized = "".join(c if c >= " " else " " for c in sanitized)
    return sanitized[:max_length]


def _canonicalize_findings(findings: list[dict]) -> list[dict]:
    """Drop equivalent mirrored trace findings before prompt selection."""
    canonical = []
    seen: set[tuple] = set()
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        key = _trace_equivalence_key(finding)
        if key is not None:
            if key in seen:
                continue
            seen.add(key)
        canonical.append(finding)
    return canonical


def _trace_equivalence_key(finding: dict) -> tuple | None:
    if finding.get("source") != "traces":
        return None

    finding_type = finding.get("findingType")
    details = finding.get("details", {}) if isinstance(finding.get("details"), dict) else {}
    incident = details.get("incidentWindow") if isinstance(details.get("incidentWindow"), dict) else {}
    baseline = details.get("baselineWindow") if isinstance(details.get("baselineWindow"), dict) else {}

    if finding_type == "trace_latency_spike":
        return (
            finding_type,
            incident.get("start"),
            incident.get("end"),
            baseline.get("start"),
            baseline.get("end"),
            round(_as_float(details.get("incidentAvgResponseTimeMs"), default=-1.0) or -1.0, 2),
            round(_as_float(details.get("baselineAvgResponseTimeMs"), default=-1.0) or -1.0, 2),
            round(_as_float(details.get("changeRatio"), default=-1.0) or -1.0, 4),
        )

    if finding_type == "latency_dominant_segment":
        return (
            finding_type,
            incident.get("start"),
            incident.get("end"),
            str(details.get("dominantSegment") or ""),
            round(_as_float(details.get("dominantSegmentPct"), default=-1.0) or -1.0, 3),
            round(_as_float(details.get("dominantSegmentMs"), default=-1.0) or -1.0, 3),
            int(_as_float(details.get("sampledTraceCount"), default=-1.0) or -1),
        )

    return None


def _as_float(value, *, default: float | None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Bedrock call
# ---------------------------------------------------------------------------

def _call_bedrock(prompt: str, model_id: str, *, max_tokens: int) -> str | None:
    """Invoke Bedrock converse API and return the raw text response.

    Returns None on any error.
    """
    client = boto3.client("bedrock-runtime")
    region = client.meta.region_name or "unknown"
    logger.debug(
        "Bedrock call start: modelId=%s region=%s promptChars=%s maxTokens=%s",
        model_id,
        region,
        len(prompt),
        max_tokens,
    )
    try:
        response = client.converse(
            modelId=model_id,
            system=[{"text": _SYSTEM_PROMPT}],
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"maxTokens": max_tokens, "temperature": 0.0},
        )
        output_text = response["output"]["message"]["content"][0]["text"]
        usage = response.get("usage", {}) if isinstance(response, dict) else {}
        input_tokens = usage.get("inputTokens") if isinstance(usage, dict) else None
        output_tokens = usage.get("outputTokens") if isinstance(usage, dict) else None
        total_tokens = usage.get("totalTokens") if isinstance(usage, dict) else None
        stop_reason = response.get("stopReason") if isinstance(response, dict) else None
        logger.debug(
            "Bedrock call success: modelId=%s region=%s responseChars=%s stopReason=%s inputTokens=%s outputTokens=%s totalTokens=%s",
            model_id,
            region,
            len(output_text),
            stop_reason,
            input_tokens,
            output_tokens,
            total_tokens,
        )
        return output_text
    except ClientError as exc:
        error_obj = exc.response.get("Error", {})
        metadata = exc.response.get("ResponseMetadata", {})
        error_code = error_obj.get("Code", "unknown")
        error_message = error_obj.get("Message", "")
        classification = _classify_bedrock_error(error_code, error_message)
        target_resource = _extract_target_resource(error_message)
        logger.warning(
            "Bedrock call failed: modelId=%s region=%s errorCode=%s classification=%s requestId=%s targetResource=%s message=%s",
            model_id,
            region,
            error_code,
            classification,
            metadata.get("RequestId", "unknown"),
            target_resource,
            error_message,
        )
        return None
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Bedrock call failed: modelId=%s region=%s exceptionType=%s error=%s",
            model_id,
            region,
            type(exc).__name__,
            str(exc),
        )
        logger.exception("Bedrock call exception traceback")
        return None


def _classify_bedrock_error(error_code: str, message: str) -> str:
    message_l = (message or "").lower()
    if error_code == "AccessDeniedException":
        if "legacy" in message_l:
            return "access_denied_legacy_model"
        if "not authorized" in message_l:
            return "access_denied_iam"
        if "use case" in message_l:
            return "access_denied_provider_use_case"
        return "access_denied_other"
    if error_code == "ValidationException":
        if "invalid" in message_l and "model" in message_l:
            return "invalid_model_id"
        if "inference profile" in message_l:
            return "inference_profile_required"
        return "validation_error"
    if error_code in ("ResourceNotFoundException",):
        return "resource_not_found"
    if error_code in ("ThrottlingException", "ServiceUnavailableException"):
        return "transient_service_error"
    return "other"


def _extract_target_resource(message: str) -> str:
    if not isinstance(message, str):
        return "unknown"
    match = re.search(r"(arn:aws:bedrock:[^\s]+)", message)
    return match.group(1) if match else "unknown"


# ---------------------------------------------------------------------------
# Response parser
# ---------------------------------------------------------------------------

def _parse_response(raw: str) -> dict:
    """Parse the model's JSON response into the evaluation dict.

    Returns {} on malformed output so the workflow continues cleanly.
    Expected keys: topHypothesis (str), assessments (list), missingEvidence (list).
    """
    raw_text = raw if isinstance(raw, str) else str(raw)
    logger.debug(
        "AI evaluator parse start: responseChars=%s preview=%s",
        len(raw_text),
        _safe_preview(raw_text),
    )

    candidate, extract_failure = _extract_json_candidate(raw_text)
    if candidate is None:
        logger.warning(
            "AI evaluator response does not contain parseable JSON object",
            extra={
                "action": "parse_response",
                "failureType": extract_failure or "no_json_object_found",
                "preview": _safe_preview(raw_text),
                "tailPreview": _safe_tail_preview(raw_text),
            },
        )
        return {}

    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError as exc:
        logger.warning(
            "AI evaluator returned non-JSON output",
            extra={
                "action": "parse_response",
                "failureType": "json_decode_error",
                "error": str(exc),
                "candidatePreview": _safe_preview(candidate),
            },
        )
        return {}

    if not isinstance(parsed, dict):
        logger.warning("AI evaluator response is not a JSON object")
        return {}

    required = {"topHypothesis", "assessments", "missingEvidence"}
    if not required.issubset(parsed.keys()):
        missing = required - parsed.keys()
        logger.warning(
            "AI evaluator response missing required keys",
            extra={"action": "parse_response", "missingKeys": list(missing)},
        )
        return {}

    # Validate types and bounds of each required field before accepting the response.
    if not isinstance(parsed["topHypothesis"], str) or parsed["topHypothesis"].strip() == "":
        logger.warning("AI evaluator: topHypothesis is not a string")
        return {}
    if not isinstance(parsed["assessments"], list):
        logger.warning("AI evaluator: assessments is not a list")
        return {}
    for item in parsed["assessments"]:
        if not isinstance(item, dict) or not all(k in item for k in ("cause", "plausibility", "reason")):
            logger.warning("AI evaluator: malformed assessment item", extra={"item": str(item)[:200]})
            return {}
        if not isinstance(item["cause"], str) or item["cause"].strip() == "":
            logger.warning("AI evaluator: assessment cause must be non-empty string")
            return {}
        if not isinstance(item["reason"], str) or item["reason"].strip() == "":
            logger.warning("AI evaluator: assessment reason must be non-empty string")
            return {}
        if not isinstance(item["plausibility"], (int, float)):
            logger.warning("AI evaluator: assessment plausibility must be numeric")
            return {}
        plausibility = float(item["plausibility"])
        if plausibility < 0.0 or plausibility > 1.0:
            logger.warning("AI evaluator: assessment plausibility out of range")
            return {}

    if not isinstance(parsed["missingEvidence"], list):
        logger.warning("AI evaluator: missingEvidence is not a list")
        return {}
    for evidence in parsed["missingEvidence"]:
        if not isinstance(evidence, str) or evidence.strip() == "":
            logger.warning("AI evaluator: missingEvidence items must be non-empty strings")
            return {}

    normalized_actions = _normalize_next_best_actions(parsed.get("nextBestActions"))

    result = {
        "topHypothesis": parsed["topHypothesis"].strip(),
        "assessments": [
            {
                "cause": item["cause"].strip(),
                "plausibility": float(item["plausibility"]),
                "reason": item["reason"].strip(),
            }
            for item in parsed["assessments"]
        ],
        "missingEvidence": [item.strip() for item in parsed["missingEvidence"]],
    }
    if normalized_actions:
        result["nextBestActions"] = normalized_actions
    return result


def _normalize_next_best_actions(raw_actions) -> list[dict]:
    """Validate and normalize optional nextBestActions array."""
    if raw_actions is None:
        return []
    if not isinstance(raw_actions, list):
        logger.warning("AI evaluator: nextBestActions must be a list when provided")
        return []

    normalized: list[dict] = []
    for item in raw_actions:
        if len(normalized) >= 3:
            break
        if not isinstance(item, dict):
            continue

        action = item.get("action")
        why = item.get("why")
        expected_signal = item.get("expectedSignal")
        confidence = item.get("confidence")
        evidence_ids = item.get("evidenceIds")

        if not isinstance(action, str) or not action.strip():
            continue
        if not isinstance(why, str) or not why.strip():
            continue
        if not isinstance(expected_signal, str) or not expected_signal.strip():
            continue
        if not isinstance(confidence, (int, float)):
            continue

        confidence_f = float(confidence)
        if confidence_f < 0.0 or confidence_f > 1.0:
            continue
        if not isinstance(evidence_ids, list):
            continue

        clean_ids: list[str] = []
        seen: set[str] = set()
        for evidence_id in evidence_ids:
            if not isinstance(evidence_id, str):
                continue
            normalized_id = evidence_id.strip()
            if not normalized_id:
                continue
            if not _EVIDENCE_ID_PATTERN.match(normalized_id):
                continue
            if normalized_id in seen:
                continue
            seen.add(normalized_id)
            clean_ids.append(normalized_id)

        if not clean_ids:
            continue

        normalized.append(
            {
                "action": action.strip(),
                "why": why.strip(),
                "evidenceIds": clean_ids,
                "expectedSignal": expected_signal.strip(),
                "confidence": confidence_f,
            }
        )

    return normalized


def _extract_json_candidate(raw: str) -> tuple[str | None, str | None]:
    """Extract a JSON object candidate from model output.

    Handles:
    - plain JSON object
    - markdown code fences
    - prose before/after JSON
    """
    text = raw.strip()
    if not text:
        return None, "empty_response"

    # Remove simple markdown fence wrappers if present.
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", text, count=1)
        text = re.sub(r"\s*```$", "", text, count=1)
        text = text.strip()

    # Fast path: already valid JSON object.
    if text.startswith("{") and text.endswith("}"):
        return text, None

    # Fallback: extract first balanced {...} block.
    start = text.find("{")
    if start == -1:
        return None, "no_opening_brace"

    depth = 0
    in_string = False
    escape = False
    for index in range(start, len(text)):
        ch = text[index]
        if in_string:
            if escape:
                escape = False
                continue
            if ch == "\\":
                escape = True
                continue
            if ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1], None

    return None, "unbalanced_braces"


def _looks_like_truncated_json(raw: str) -> bool:
    """Heuristic: JSON-like output that appears cut before closure."""
    if not isinstance(raw, str):
        return False
    text = raw.strip()
    return text.startswith("{") and text.count("{") > text.count("}")


def _safe_preview(value: str, limit: int = _RAW_PREVIEW_CHARS) -> str:
    """Create a single-line bounded preview for logs."""
    cleaned = value.replace("\n", "\\n").replace("\r", "\\r")
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[:limit]}..."


def _safe_tail_preview(value: str, limit: int = _RAW_PREVIEW_CHARS) -> str:
    """Create a bounded single-line tail preview for logs."""
    cleaned = value.replace("\n", "\\n").replace("\r", "\\r")
    if len(cleaned) <= limit:
        return cleaned
    return f"...{cleaned[-limit:]}"


def _summarize_assessments_for_log(assessments: list) -> list[dict]:
    """Return bounded per-assessment log summary for debugging AI resolution choices."""
    if not isinstance(assessments, list):
        return []
    summary = []
    for item in assessments[:3]:
        if not isinstance(item, dict):
            continue
        summary.append(
            {
                "cause": str(item.get("cause", ""))[:120],
                "plausibility": _as_float(item.get("plausibility"), default=None),
                "reasonPreview": _safe_preview(str(item.get("reason", "")), limit=120),
            }
        )
    return summary
