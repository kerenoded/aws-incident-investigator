"""
Logs Worker Lambda handler.

Receives the scope output from Step Functions, runs targeted CloudWatch Logs
Insights queries for the incident and baseline windows, and returns structured
findings.

Environment variables:
    (none required — region is taken from the scope payload)

Logging:
    Emits start/complete boundary logs with incidentId and summary counts
    to make worker invocation debugging easier in CloudWatch logs.
"""

import logging

import boto3
from workers.logs.logs_worker import collect_logs
from shared.worker_output import validate_worker_output

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def lambda_handler(event: dict, context) -> dict:
    """Logs Worker Lambda entry point.

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
        "logs_handler_start",
        extra={
            "source": "logs",
            "incidentId": incident_id,
            "region": region,
            "logGroupsCount": len(event.get("logGroups", [])),
        },
    )

    logs_client = boto3.client("logs", region_name=region)
    output = collect_logs(event, logs_client)
    validate_worker_output(output, expected_source="logs")
    logger.info(
        "logs_handler_complete",
        extra={
            "source": "logs",
            "incidentId": output.get("incidentId", incident_id),
            "findingsCount": len(output.get("findings", [])),
            "errorsCount": len(output.get("errors", [])),
        },
    )
    return output
