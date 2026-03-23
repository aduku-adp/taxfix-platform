import pytest

from business_rules import validate_payload
from validation_models import CdcEvent


def _event(change_type: str, payload: dict) -> CdcEvent:
    return CdcEvent.model_validate(
        {
            "uuid": "test-uuid",
            "source_timestamp": "2026-01-01T00:00:00Z",
            "read_timestamp": "2026-01-01T00:00:00Z",
            "source_metadata": {"change_type": change_type},
            "payload": payload,
        }
    )


def test_insert_with_full_payload_passes():
    assert validate_payload(_event("INSERT", {"_id": "u1", "email": "a@b.com"})) == []


def test_update_with_full_payload_passes():
    assert validate_payload(_event("UPDATE", {"_id": "u1", "email": "a@b.com"})) == []


def test_insert_with_id_only_fails():
    violations = validate_payload(_event("INSERT", {"_id": "u1"}))
    assert len(violations) == 1
    assert "INSERT" in violations[0]


def test_delete_with_id_only_passes():
    assert validate_payload(_event("DELETE", {"_id": "u1"})) == []


def test_delete_with_extra_fields_fails():
    violations = validate_payload(_event("DELETE", {"_id": "u1", "email": "a@b.com"}))
    assert len(violations) == 1
    assert "DELETE" in violations[0]


def test_multiple_violations_returned():
    # UPDATE with no _id — missing _id is one violation; no other fields possible check merges
    # Use DELETE with multiple extra fields to confirm all are reported in one violation string
    violations = validate_payload(_event("DELETE", {"_id": "u1", "a": 1, "b": 2}))
    assert len(violations) == 1
    assert "a" in violations[0] and "b" in violations[0]
