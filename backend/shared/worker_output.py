"""
Shared worker output validation.

Lightweight runtime boundary validation for the standard worker output envelope:
{ incidentId, source, findings, errors }.

Used by all three worker handlers (metrics, logs, traces) to enforce the
contract defined in schemas/worker-output.schema.json.
"""


def validate_worker_output(output: dict, *, expected_source: str) -> None:
    """Validate the standard worker output envelope.

    Args:
        output:          The dict returned by the worker business-logic function.
        expected_source: The source name this handler expects (e.g. "metrics").

    Raises:
        ValueError: If the output does not conform to the expected envelope shape.
    """
    if not isinstance(output, dict):
        raise ValueError("Worker output must be a JSON object.")

    for field in ("incidentId", "source", "findings", "errors"):
        if field not in output:
            raise ValueError(f"Worker output missing required field: {field}")

    if not isinstance(output["incidentId"], str) or output["incidentId"].strip() == "":
        raise ValueError("Worker output incidentId must be a non-empty string.")
    if output["source"] != expected_source:
        raise ValueError(
            f"Worker output source must be '{expected_source}', got '{output['source']}'."
        )
    if not isinstance(output["findings"], list):
        raise ValueError("Worker output findings must be a list.")
    if not isinstance(output["errors"], list):
        raise ValueError("Worker output errors must be a list.")
