import time
import logging
from datetime import datetime, timedelta, UTC

from scheduler.config import settings
from scheduler.database import SessionLocal
from scheduler.models import Job

logging.basicConfig(level=logging.INFO, format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}')
logger = logging.getLogger("sweeper")

def run_timeout_sweeper():
    logger.info("Starting Worker Timeout Sweeper...")
    while True:
        db = SessionLocal()
        try:
            now = datetime.now(UTC)
            cutoff = now - timedelta(seconds=settings.JOB_TIMEOUT_SECONDS)

            stalled_jobs = (
                db.query(Job)
                .filter(Job.status == "RUNNING", Job.updated_at < cutoff)
                .all()
            )

            for job in stalled_jobs:
                logger.warning(f"Timeout detected on Job {job.id} assigned to worker {job.worker_id}")
                
                if job.retry_count < job.max_retries:
                    job.retry_count += 1
                    job.status = "PENDING"
                    job.worker_id = None
                    job.error_message = f"Execution timed out (Limit: {settings.JOB_TIMEOUT_SECONDS}s)"
                    logger.info(f"Job {job.id} reset back to PENDING. Retry: {job.retry_count}")
                else:
                    job.status = "FAILED"
                    job.error_message = "Execution timed out and exceeded retry limits."
                    logger.error(f"Job {job.id} marked FAILED. All retries exhausted.")
                
                job.updated_at = now
            
            if stalled_jobs:
                db.commit()

        except Exception as e:
            logger.error(f"Error in sweeper run execution: {e}")
        finally:
            db.close()
        
        time.sleep(settings.SWEEPER_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_timeout_sweeper()
