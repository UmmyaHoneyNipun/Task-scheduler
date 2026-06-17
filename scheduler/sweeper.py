import logging
import time
from datetime import datetime, UTC

from scheduler.config import settings
from scheduler.database import Base, SessionLocal, engine
from scheduler.models import Job
from scheduler.redis_queue import (
    ZSET_RUNNING,
    get_redis,
    heartbeat_key,
    queue_name_for_priority,
    remove_processing_entry,
    serialize_job_message,
)

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
)
logger = logging.getLogger("sweeper")


def run_timeout_sweeper():
    logger.info("Starting Redis-backed timeout sweeper...")
    Base.metadata.create_all(bind=engine)
    redis_client = get_redis(settings.REDIS_URL)

    while True:
        sweep(redis_client)
        time.sleep(settings.SWEEPER_INTERVAL_SECONDS)


def sweep(redis_client):
    now = time.time()
    timed_out_ids = redis_client.zrangebyscore(ZSET_RUNNING, 0, now)
    if not timed_out_ids:
        return

    db = SessionLocal()
    try:
        for job_id in timed_out_ids:
            if redis_client.exists(heartbeat_key(job_id)):
                redis_client.zadd(ZSET_RUNNING, {job_id: now + settings.JOB_TIMEOUT_SECONDS})
                continue

            job = db.query(Job).filter(Job.id == job_id).first()
            if job is None:
                redis_client.zrem(ZSET_RUNNING, job_id)
                continue

            if job.status != "RUNNING":
                redis_client.zrem(ZSET_RUNNING, job_id)
                remove_processing_entry(redis_client, job.worker_id or "", job_id)
                continue

            logger.warning(f"Timeout confirmed for Job {job.id} (worker: {job.worker_id})")
            current_ts = datetime.now(UTC)
            original_worker_id = job.worker_id or ""

            if job.retry_count < job.max_retries:
                job.retry_count += 1
                job.status = "PENDING"
                job.worker_id = None
                job.error_message = (
                    f"Execution timed out (limit: {settings.JOB_TIMEOUT_SECONDS}s)"
                )
                job.updated_at = current_ts

                raw = serialize_job_message(
                    job_id=job.id,
                    job_type=job.job_type,
                    payload=job.payload,
                    priority=job.priority,
                    max_retries=job.max_retries,
                    retry_count=job.retry_count,
                )
                redis_client.lpush(queue_name_for_priority(job.priority), raw)
                logger.info(
                    f"Job {job.id} re-queued. Retry {job.retry_count}/{job.max_retries}"
                )
            else:
                job.status = "FAILED"
                job.worker_id = None
                job.error_message = "Execution timed out and exceeded retry limits."
                job.updated_at = current_ts
                logger.error(f"Job {job.id} permanently FAILED — retries exhausted")

            redis_client.zrem(ZSET_RUNNING, job_id)
            remove_processing_entry(redis_client, original_worker_id, job_id)

        db.commit()

    except Exception as exc:
        logger.error(f"Sweeper error: {exc}")
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    run_timeout_sweeper()
