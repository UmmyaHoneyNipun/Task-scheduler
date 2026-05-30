import uuid
from datetime import UTC, datetime

from sqlalchemy import JSON, Column, DateTime, Integer, String

from scheduler.database import Base


def utc_now() -> datetime:
    return datetime.now(UTC)


class Job(Base):
    __tablename__ = "jobs"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()), index=True)
    job_type = Column(String, nullable=False, index=True)
    payload = Column(JSON, nullable=False)
    priority = Column(Integer, nullable=False, default=1, index=True)
    status = Column(String, nullable=False, default="PENDING", index=True)
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=1)
    worker_id = Column(String, nullable=True, index=True)
    error_message = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), default=utc_now, index=True)
    updated_at = Column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, index=True)
