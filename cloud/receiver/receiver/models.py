from datetime import datetime
from typing import Annotated

from pydantic import AwareDatetime, BaseModel, Field

DeviceId = Annotated[str, Field(pattern=r"^[A-Za-z0-9._-]{1,64}$")]
OutboxId = Annotated[str, Field(pattern=r"^[A-Za-z0-9-]{1,64}$")]
SignalName = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")]

MAX_RECORDS_PER_BATCH = 10_000


class Record(BaseModel):
    seq: int = Field(ge=0, le=2**63 - 1)
    ts: AwareDatetime
    signal: SignalName
    value: float = Field(allow_inf_nan=False)


class Batch(BaseModel):
    device_id: DeviceId
    outbox_id: OutboxId
    batch_id: str = Field(min_length=1, max_length=64)
    dropped_records: int = Field(default=0, ge=0)
    records: list[Record] = Field(max_length=MAX_RECORDS_PER_BATCH)


class IngestResult(BaseModel):
    accepted: int
    inserted: int
    duplicates: int


class LatestValue(BaseModel):
    value: float
    ts: datetime
    seq: int


class Latest(BaseModel):
    device_id: str
    signals: dict[str, LatestValue]
