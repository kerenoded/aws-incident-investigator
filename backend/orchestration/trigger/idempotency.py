"""
Idempotency helpers for the trigger Lambda.

Prevents duplicate Step Functions executions when the same
Idempotency-Key header is submitted more than once.
"""

from botocore.exceptions import ClientError


def extract_idempotency_key(event: dict) -> str | None:
    """Extract and validate the Idempotency-Key header.

    Returns None when no key is present.
    Raises ValueError when the key exceeds the allowed length.
    """
    headers = event.get("headers") or {}
    if not isinstance(headers, dict):
        return None

    raw = None
    for key in ("Idempotency-Key", "idempotency-key"):
        if key in headers:
            raw = headers.get(key)
            break

    if raw is None:
        return None

    value = str(raw).strip()
    if value == "":
        return None
    if len(value) > 128:
        raise ValueError("Idempotency-Key must be <= 128 characters.")
    return value


def idempotency_pk(idempotency_scope: str, idempotency_key: str) -> str:
    return f"IDEMPOTENCY#{idempotency_scope}#{idempotency_key}"


def reserve_or_get_existing_incident(
    *,
    table,
    idempotency_scope: str,
    idempotency_key: str,
    incident_id: str,
    created_at: str,
    ttl: int,
) -> str | None:
    """Reserve an idempotency key or return the previously associated incidentId.

    Returns:
        None when the key was reserved for this request (new execution).
        Existing incidentId when this is a duplicate request.
    """
    pk = idempotency_pk(idempotency_scope, idempotency_key)
    try:
        table.put_item(
            Item={
                "PK": pk,
                "SK": "REQUEST",
                "incidentId": incident_id,
                "scope": idempotency_scope,
                "createdAt": created_at,
                "ttl": ttl,
            },
            ConditionExpression="attribute_not_exists(PK) AND attribute_not_exists(SK)",
        )
        return None
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code != "ConditionalCheckFailedException":
            raise

    response = table.get_item(Key={"PK": pk, "SK": "REQUEST"})
    item = response.get("Item") or {}
    existing_incident_id = item.get("incidentId")
    if isinstance(existing_incident_id, str) and existing_incident_id.strip() != "":
        return existing_incident_id
    return None


def get_incident_status(table, incident_id: str) -> str | None:
    """Fetch the current status of an investigation from DynamoDB."""
    try:
        response = table.get_item(Key={"PK": f"INCIDENT#{incident_id}", "SK": "META"})
    except ClientError:
        return None
    item = response.get("Item") or {}
    status = item.get("status")
    return status if isinstance(status, str) and status.strip() != "" else None
