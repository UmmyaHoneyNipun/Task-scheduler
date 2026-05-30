import sys
import time
import uuid
import logging
import os
import requests
from scheduler.config import settings

logging.basicConfig(level=logging.INFO, format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}')
logger = logging.getLogger("worker")


class TaskWorker:
    def __init__(self, api_url: str, worker_id: str | None = None):
        self.api_url = api_url.rstrip("/")
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        
    def start(self):
        logger.info(f"Worker daemon booted. ID: {self.worker_id}. Polling endpoint: {self.api_url}")
        while True:
            try:
                self.poll_and_execute()
            except Exception as e:
                logger.error(f"Unexpected cycle error in worker pool loop: {e}")
            time.sleep(settings.WORKER_POLL_INTERVAL_SECONDS)

    def poll_and_execute(self):
        try:
            resp = requests.post(
                f"{self.api_url}/next-job",
                json={"worker_id": self.worker_id},
                timeout=5,
            )
        except requests.RequestException as e:
            logger.warning(f"Could not connect to scheduler API: {e}")
            return

        if resp.status_code == 404:
            return
        
        if resp.status_code != 200:
            logger.error(f"Unexpected scheduler server response: {resp.status_code}")
            return

        job_data = resp.json()
        job_id = job_data["id"]
        job_type = job_data["job_type"]
        payload = job_data["payload"]

        logger.info(f"Successfully claimed Job {job_id} [{job_type}]. Executing...")

        try:
            self.execute(job_type, payload)
        except Exception as e:
            err_msg = str(e)
            logger.error(f"Error executing Job {job_id}: {err_msg}")
            self.report_failure(job_id, err_msg)
            return

        try:
            self.report_completion(job_id)
            logger.info(f"Reported completion for Job {job_id}")
        except requests.RequestException as e:
            logger.error(f"Job {job_id} executed but completion callback failed after retries: {e}")

    def report_completion(self, job_id: str):
        last_error = None
        for attempt in range(1, 4):
            try:
                complete_resp = requests.post(
                    f"{self.api_url}/jobs/{job_id}/complete",
                    json={"worker_id": self.worker_id},
                    timeout=5,
                )
                complete_resp.raise_for_status()
                return
            except requests.RequestException as e:
                last_error = e
                logger.warning(
                    f"Completion callback attempt {attempt}/3 failed for Job {job_id}: {e}"
                )
                if attempt < 3:
                    time.sleep(1)

        if last_error is not None:
            raise last_error

    def report_failure(self, job_id: str, err_msg: str):
        try:
            fail_resp = requests.post(
                f"{self.api_url}/jobs/{job_id}/fail",
                json={
                    "worker_id": self.worker_id,
                    "error_message": err_msg,
                },
                timeout=5,
            )
            fail_resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to report error for Job {job_id}: {e}")

    def execute(self, job_type: str, payload: dict):
        if job_type == "sleep":
            duration = payload.get("duration", 5)
            logger.info(f"Sleeping for {duration} seconds...")
            time.sleep(duration)
        elif job_type == "http":
            url = payload.get("url")
            method = payload.get("method", "GET").upper()
            if not url:
                raise ValueError("Payload missing required target 'url'")
            logger.info(f"Executing web request -> [{method}] {url}")
            resp = requests.request(method, url, timeout=10)
            resp.raise_for_status()
        else:
            raise NotImplementedError(f"Job execution style not supported: '{job_type}'")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--worker-id", default=os.environ.get("WORKER_ID"))
    args = parser.parse_args()
    
    worker = TaskWorker(api_url=args.api_url, worker_id=args.worker_id)
    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info("Worker process terminating gracefully...")
        sys.exit(0)
