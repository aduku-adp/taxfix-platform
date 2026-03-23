"""
Business-rule validation for validated CDC events.

Rules:
    INSERT / UPDATE: payload must contain _id plus at least one other field.
    DELETE:          payload must contain only _id.

validate_payload() returns a list of violation strings.
An empty list means the event passes all rules.
Events with violations are skipped and counted; the pipeline does not crash.
"""
from validation_models import CdcEvent


def validate_payload(event: CdcEvent) -> list[str]:
    violations: list[str] = []
    change_type = event.source_metadata.change_type
    payload_keys = set(event.payload.keys())

    if change_type in ("INSERT", "UPDATE"):
        if "_id" not in payload_keys:
            violations.append(f"{change_type} payload missing required field '_id'")
        elif len(payload_keys) < 2:
            violations.append(
                f"{change_type} payload must contain '_id' plus at least one other field"
            )
    elif change_type == "DELETE":
        extra = payload_keys - {"_id"}
        if extra:
            violations.append(
                f"DELETE payload must contain only '_id'; found extra fields: {sorted(extra)}"
            )

    return violations
