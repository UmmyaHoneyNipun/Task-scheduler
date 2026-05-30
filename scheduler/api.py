from contextlib import asynccontextmanager
import logging
from datetime import datetime, UTC
from typing import Dict, Any
from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import update
from sqlalchemy.orm import Session

from scheduler.config import settings
from scheduler.database import get_db, engine, Base
from scheduler.models import Job

logging.basicConfig(level=logging.INFO, format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}')
logger = logging.getLogger("api")


@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title=settings.APP_NAME, version="1.0.0", lifespan=lifespan)

class JobCreate(BaseModel):
    job_type: str = Field(..., example="http")
    payload: Dict[str, Any] = Field(..., example={"url": "https://someurl.org/get", "method": "GET"})
    priority: int = Field(1, ge=1, le=10)
    max_retries: int = Field(1, ge=0)


class WorkerRequest(BaseModel):
    worker_id: str = Field(..., min_length=1, example="worker-alpha")


class JobFailureRequest(WorkerRequest):
    error_message: str = Field(..., min_length=1, example="Request timed out")


def get_running_job_for_worker(id: str, worker_id: str, db: Session) -> Job:
    job = db.query(Job).filter(Job.id == id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != "RUNNING":
        raise HTTPException(status_code=400, detail="Only running jobs can be updated")
    if job.worker_id != worker_id:
        raise HTTPException(status_code=403, detail="Job is owned by a different worker")
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
    db.commit()
    db.refresh(db_job)
    logger.info(f"Job registered: {db_job.id} [Priority: {db_job.priority}]")
    return db_job

@app.get("/jobs/{id}")
def get_job(id: str, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.post("/next-job")
def get_next_job(worker: WorkerRequest, db: Session = Depends(get_db)):
    query = (
        db.query(Job)
        .filter(Job.status == "PENDING")
        .order_by(Job.priority.desc(), Job.created_at.asc())
    )

    worker_id = worker.worker_id
    is_postgres = db.get_bind().dialect.name == "postgresql"
    if is_postgres:
        query = query.with_for_update(skip_locked=True)

    candidates = query.limit(5).all()
    if not candidates:
        raise HTTPException(status_code=404, detail="No pending jobs available")

    now = datetime.now(UTC)
    assigned_job = None

    for candidate in candidates:
        if is_postgres:
            candidate.status = "RUNNING"
            candidate.worker_id = worker_id
            candidate.updated_at = now
            db.commit()
            assigned_job = candidate
            break
        else:
            stmt = (
                update(Job)
                .where(Job.id == candidate.id, Job.status == "PENDING")
                .values(status="RUNNING", worker_id=worker_id, updated_at=now)
            )
            result = db.execute(stmt)
            db.commit()
            if result.rowcount > 0:
                assigned_job = db.query(Job).filter(Job.id == candidate.id).first()
                break

    if not assigned_job:
        raise HTTPException(status_code=404, detail="No pending jobs available")

    logger.info(f"Dispatched Job {assigned_job.id} to worker {worker_id}")
    return assigned_job

@app.post("/jobs/{id}/complete")
def complete_job(id: str, worker: WorkerRequest, db: Session = Depends(get_db)):
    job = get_running_job_for_worker(id, worker.worker_id, db)
    job.status = "COMPLETED"
    job.worker_id = None
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
        job.error_message = err_msg
        logger.error(f"Job {id} completely failed. Error: {err_msg}")

    job.updated_at = now
    db.commit()
    return {"status": "processed", "job_state": job.status}
