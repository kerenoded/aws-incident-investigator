"""
Metrics Worker Lambda handler.

Receives the scope output from Step Functions, fetches CloudWatch metrics for
the incident and baseline windows, and returns structured findings.

Environment variables:
    (none required — region is taken from the scope payload)

Logging:
    Emits start/complete boundary logs with incidentId and summary counts
    to make worker invocation debugging easier in CloudWatch logs.
"""

import logging

import boto3
from workers.metrics.metrics_worker import collect_metrics
from shared.worker_output import validate_worker_output

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def lambda_handler(event: dict, context) -> dict:
    """Metrics Worker Lambda entry point.

    Args:
        event:   Scope output from Step Functions
                 (see schemas/examples/scope.example.json).
        context: Lambda context (unused).

    Returns:
        Worker output dict conforming to schemas/worker-output.schema.json.
    """
    region = event.get("region", "eu-west-1")
    incident_id = event.get("incidentId", "unknown")
    logger.info(
        "metrics_handler_start",
        extra={
            "source": "metrics",
            "incidentId": incident_id,
            "region": region,
            "metricsCount": len(event.get("metrics", [])),
        },
    )

    cw_client = boto3.client("cloudwatch", region_name=region)
    output = collect_metrics(event, cw_client)
    validate_worker_output(output, expected_source="metrics")
    logger.info(
        "metrics_handler_complete",
        extra={
            "source": "metrics",
            "incidentId": output.get("incidentId", incident_id),
            "findingsCount": len(output.get("findings", [])),
            "errorsCount": len(output.get("errors", [])),
        },
    )
    return output
