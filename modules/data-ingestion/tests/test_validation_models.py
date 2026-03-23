from datetime import datetime

import pytest
from pydantic import ValidationError

from validation_models import CdcEvent


def test_valid_insert_event(sample_insert_event):
    event = CdcEvent.model_validate(sample_insert_event)
    assert event.uuid == "aaaa-0001"
    assert event.source_metadata.change_type == "INSERT"
    assert event.payload == {"_id": "user-001", "email": "alice@example.com"}


def test_valid_update_event(sample_update_event):
    event = CdcEvent.model_validate(sample_update_event)
    assert event.source_metadata.change_type == "UPDATE"


def test_valid_delete_event(sample_delete_event):
    event = CdcEvent.model_validate(sample_delete_event)
    assert event.source_metadata.change_type == "DELETE"
    assert event.payload == {"_id": "user-001"}


def test_missing_uuid_raises(sample_insert_event):
    del sample_insert_event["uuid"]
    with pytest.raises(ValidationError):
        CdcEvent.model_validate(sample_insert_event)


def test_missing_source_timestamp_raises(sample_insert_event):
    del sample_insert_event["source_timestamp"]
    with pytest.raises(ValidationError):
        CdcEvent.model_validate(sample_insert_event)


def test_invalid_change_type_raises(sample_insert_event):
    sample_insert_event["source_metadata"]["change_type"] = "UPSERT"
    with pytest.raises(ValidationError):
        CdcEvent.model_validate(sample_insert_event)


def test_unknown_field_allowed(sample_insert_event):
    sample_insert_event["new_field"] = "extra"
    event = CdcEvent.model_validate(sample_insert_event)
    assert event.uuid == "aaaa-0001"


def test_source_timestamp_parsed_as_datetime(sample_insert_event):
    event = CdcEvent.model_validate(sample_insert_event)
    assert isinstance(event.source_timestamp, datetime)
