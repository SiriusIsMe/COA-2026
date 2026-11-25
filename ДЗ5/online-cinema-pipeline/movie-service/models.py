"""Модель входящего события. Повторяет Avro-схему из schemas/movie_event.avsc
и добавляет бизнес-правила, которые Avro выразить не может."""
from datetime import datetime, timedelta, timezone
from enum import Enum
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

AVRO_INT_MAX = 2**31 - 1
MAX_CLOCK_SKEW = timedelta(minutes=5)


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


VIEW_EVENTS = {
    EventType.VIEW_STARTED,
    EventType.VIEW_FINISHED,
    EventType.VIEW_PAUSED,
    EventType.VIEW_RESUMED,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class MovieEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # event_id и timestamp можно не передавать — сервис проставит сам
    event_id: UUID = Field(default_factory=uuid4)
    user_id: str = Field(min_length=1, max_length=128)
    movie_id: str = Field(min_length=1, max_length=128)
    event_type: EventType
    timestamp: AwareDatetime = Field(default_factory=utc_now)
    device_type: DeviceType
    session_id: str = Field(min_length=1, max_length=128)
    progress_seconds: int | None = Field(default=None, ge=0, le=AVRO_INT_MAX)

    @field_validator("timestamp")
    @classmethod
    def to_utc(cls, v: datetime) -> datetime:
        v = v.astimezone(timezone.utc)
        if v > utc_now() + MAX_CLOCK_SKEW:
            raise ValueError("timestamp is in the future")
        return v

    @model_validator(mode="after")
    def check_progress(self) -> "MovieEvent":
        if self.event_type in VIEW_EVENTS and self.progress_seconds is None:
            raise ValueError(f"progress_seconds is required for {self.event_type.value}")
        if self.event_type not in VIEW_EVENTS and self.progress_seconds is not None:
            raise ValueError(f"progress_seconds must be null for {self.event_type.value}")
        return self

    def to_avro(self) -> dict:
        """Словарь в форме, которую ожидает Avro-сериализатор."""
        return {
            "event_id": str(self.event_id),
            "user_id": self.user_id,
            "movie_id": self.movie_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp,  # timestamp-millis: datetime с tz
            "device_type": self.device_type.value,
            "session_id": self.session_id,
            "progress_seconds": self.progress_seconds,
        }
