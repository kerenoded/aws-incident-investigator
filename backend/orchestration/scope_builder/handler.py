"""
Scope Builder Lambda handler.

Entry point for the Step Functions workflow. Receives an incident payload,
builds the investigation scope from context snapshot data, writes scope.json
to S3, and returns the scope dict to the state machine.

This is a critical workflow step — failures should propagate immediately.
Step Functions will not continue without a valid scope.

Environment variables:
    S3_BUCKET         (required) S3 bucket name for storing investigation artifacts.
"""

import json
import os
import boto3
from scope import (
    build_scope_from_context_snapshot,
)


def lambda_handler(event: dict, context) -> dict:
    """Scope Builder Lambda handler.

    Args:
        event:   Incident payload from Step Functions
                 (see schemas/incident.schema.json).
        context: Lambda context (unused).

    Returns:
        Scope dict written to S3 at investigations/<incidentId>/scope.json
        and passed downstream by Step Functions.

    Raises:
        ValueError:  If the incident payload is missing the required 'contextSnapshot' object.
        KeyError:    If S3_BUCKET environment variable is not set.
    """
    bucket = os.environ["S3_BUCKET"]
    scope = build_scope_from_context_snapshot(event)

    s3_client = boto3.client("s3")
    key = f"investigations/{scope['incidentId']}/scope.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(scope, indent=2),
        ContentType="application/json",
    )

    return scope
