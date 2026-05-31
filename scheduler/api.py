from contextlib import asynccontextmanager
import json
import logging
from datetime import datetime, UTC
import time
from typing import Dict, Any
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import Response
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

try:
    from redis import RedisError
except ModuleNotFoundError:  # pragma: no cover - runtime dependency, not needed for unit tests
    RedisError = RuntimeError

from scheduler.config import settings
from scheduler.database import get_db, engine, Base
from scheduler.metrics import CONTENT_TYPE_LATEST, metrics_supported, render_metrics
from scheduler.models import Job
from scheduler.redis_queue import (
    ZSET_RUNNING,
    deserialize_job_message,
    get_redis,
    heartbeat_key,
    priority_queue_names,
    processing_key,
    queue_name_for_priority,
    serialize_job_message,
)

logging.basicConfig(level=logging.INFO, format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}')
logger = logging.getLogger("api")


@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title=settings.APP_NAME, version="1.0.0", lifespan=lifespan)


class JobCreate(BaseModel):
    job_type: str = Field(..., json_schema_extra={"example": "http"})
    payload: Dict[str, Any] = Field(
        ...,
        json_schema_extra={"example": {"url": "https://someurl.org/get", "method": "GET"}},
    )
    priority: int = Field(1, ge=1, le=10)
    max_retries: int = Field(1, ge=0)


class WorkerRequest(BaseModel):
    worker_id: str = Field(..., min_length=1, json_schema_extra={"example": "worker-alpha"})


class JobFailureRequest(WorkerRequest):
    error_message: str = Field(..., min_length=1, json_schema_extra={"example": "Request timed out"})


def get_job_or_404(job_id: str, db: Session) -> Job:
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


def get_running_job_for_worker(id: str, worker_id: str, db: Session) -> Job:
    job = get_job_or_404(id, db)
    if job.status != "RUNNING":
        raise HTTPException(status_code=400, detail="Only running jobs can be updated")
    if job.worker_id != worker_id:
        raise HTTPException(status_code=403, detail="Job is owned by a different worker")
    return job


def queue_pending_jobs(redis_client, db: Session) -> int:
    pending_jobs = (
        db.query(Job)
        .filter(Job.status == "PENDING")
        .order_by(Job.priority.desc(), Job.created_at.asc())
        .all()
    )

    for job in reversed(pending_jobs):
        raw_message = serialize_job_message(
            job_id=job.id,
            job_type=job.job_type,
            payload=job.payload,
            priority=job.priority,
            max_retries=job.max_retries,
            retry_count=job.retry_count,
        )
        redis_client.lpush(queue_name_for_priority(job.priority), raw_message)

    return len(pending_jobs)


def claim_next_queued_job(redis_client, worker_id: str, db: Session) -> Job | None:
    proc_key = processing_key(worker_id)

    while True:
        raw = None
        for queue_name in priority_queue_names():
            raw = redis_client.rpoplpush(queue_name, proc_key)
            if raw is not None:
                break
        if raw is None:
            return None

        try:
            job_data = deserialize_job_message(raw)
            job_id = job_data["id"]
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            logger.warning(f"Discarding malformed queued payload for {worker_id}: {exc}")
            redis_client.lrem(proc_key, 1, raw)
            continue

        job = db.query(Job).filter(Job.id == job_id).first()

        if job is None:
            logger.warning(f"Skipping stale queued job {job_id} because it no longer exists")
            redis_client.lrem(proc_key, 1, raw)
            continue

        if job.status != "PENDING":
            logger.warning(
                f"Skipping queued job {job.id} because its status is {job.status}, not PENDING"
            )
            redis_client.lrem(proc_key, 1, raw)
            continue

        job.status = "RUNNING"
        job.worker_id = worker_id
        job.updated_at = datetime.now(UTC)
        db.commit()

        redis_client.delete(heartbeat_key(job.id))
        redis_client.zadd(
            ZSET_RUNNING,
            {job.id: time.time() + settings.JOB_TIMEOUT_SECONDS},
        )

        logger.info(f"Dispatched Job {job.id} to worker {worker_id}")
        return job


@app.post("/jobs", status_code=status.HTTP_201_CREATED)
def create_job(job_in: JobCreate, db: Session = Depends(get_db)):
    db_job = Job(
        job_type=job_in.job_type,
        payload=job_in.payload,
        priority=job_in.priority,
        max_retries=job_in.max_retries,
        status="PENDING"
    )
    db.add(db_job)

    redis_client = get_redis(settings.REDIS_URL)

    try:
        db.flush()
        raw_message = serialize_job_message(
            job_id=db_job.id,
            job_type=db_job.job_type,
            payload=db_job.payload,
            priority=db_job.priority,
            max_retries=db_job.max_retries,
            retry_count=db_job.retry_count,
        )
        queue_name = queue_name_for_priority(db_job.priority)
        redis_client.lpush(queue_name, raw_message)
        db.commit()
    except RedisError as exc:
        db.rollback()
        raise HTTPException(status_code=503, detail="Redis queue is unavailable") from exc
    except Exception:
        if "raw_message" in locals():
            redis_client.lrem(queue_name, 1, raw_message)
        db.rollback()
        raise

    db.refresh(db_job)
    logger.info(f"Job registered: {db_job.id} [Priority: {db_job.priority}]")
    return db_job

@app.get("/jobs/{id}")
def get_job(id: str, db: Session = Depends(get_db)):
    return get_job_or_404(id, db)


@app.get("/metrics", include_in_schema=False)
def metrics():
    if not metrics_supported():
        raise HTTPException(status_code=503, detail="Prometheus metrics are unavailable")
    return Response(content=render_metrics(), media_type=CONTENT_TYPE_LATEST)


@app.post("/next-job")
def get_next_job(worker: WorkerRequest, db: Session = Depends(get_db)):
    try:
        redis_client = get_redis(settings.REDIS_URL)
        claimed_job = claim_next_queued_job(redis_client, worker.worker_id, db)
        if claimed_job is not None:
            return claimed_job

        synced_count = queue_pending_jobs(redis_client, db)
        if synced_count == 0:
            raise HTTPException(status_code=404, detail="No pending jobs available")

        logger.warning(
            f"Redis queue was empty for worker {worker.worker_id}; re-queued {synced_count} pending jobs from PostgreSQL"
        )
        claimed_job = claim_next_queued_job(redis_client, worker.worker_id, db)
        if claimed_job is not None:
            return claimed_job

        raise HTTPException(status_code=404, detail="No pending jobs available")
    except RedisError as exc:
        logger.error(f"Could not claim the next job because Redis is unavailable: {exc}")
        raise HTTPException(status_code=503, detail="Redis queue is unavailable") from exc

@app.post("/jobs/{id}/complete")
def complete_job(id: str, worker: WorkerRequest, db: Session = Depends(get_db)):
    job = get_job_or_404(id, db)

    if job.status == "COMPLETED" and job.worker_id == worker.worker_id:
        return {"status": "success", "id": id}

    if job.status != "RUNNING":
        raise HTTPException(status_code=400, detail="Only running jobs can be completed")
    if job.worker_id != worker.worker_id:
        raise HTTPException(status_code=403, detail="Job is owned by a different worker")

    job.status = "COMPLETED"
    job.updated_at = datetime.now(UTC)
    db.commit()
    logger.info(f"Job completed: {id}")
    return {"status": "success", "id": id}

@app.post("/jobs/{id}/fail")
def fail_job(id: str, failure: JobFailureRequest, db: Session = Depends(get_db)):
    job = get_running_job_for_worker(id, failure.worker_id, db)
    err_msg = failure.error_message
    now = datetime.now(UTC)

    if job.retry_count < job.max_retries:
        job.retry_count += 1
        job.status = "PENDING"
        job.worker_id = None
        job.error_message = f"Fail attempt {job.retry_count}: {err_msg}"
        logger.warning(f"Rescheduling Job {id} for retry {job.retry_count}/{job.max_retries}")
    else:
        job.status = "FAILED"
        job.worker_id = None
        job.error_message = err_msg
        logger.error(f"Job {id} completely failed. Error: {err_msg}")

    job.updated_at = now
    db.commit()
    return {
        "status": "processed",
        "job_state": job.status,
        "retry_count": job.retry_count,
        "max_retries": job.max_retries,
    }
