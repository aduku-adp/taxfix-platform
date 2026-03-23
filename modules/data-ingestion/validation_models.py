"""
Pydantic schema validation for CDC events.

CdcEvent validates required fields and types for each incoming event.
Uses extra='allow' so unknown fields pass silently (schema evolution safe).
Validation errors are captured and counted; they do not raise exceptions.
"""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict


class SourceMetadata(BaseModel):
    model_config = ConfigDict(extra="allow")

    change_type: Literal["INSERT", "UPDATE", "DELETE"]


class CdcEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    uuid: str
    source_timestamp: datetime
    read_timestamp: datetime
    source_metadata: SourceMetadata
    payload: dict
