import os
import sys
import time
import uuid
import logging
import threading

import requests

from scheduler.config import settings
from scheduler.redis_queue import (
    ZSET_RUNNING,
    deserialize_job_message,
    get_redis,
    heartbeat_key,
    processing_key,
    queue_name_for_priority,
    serialize_job_message,
)

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
)
logger = logging.getLogger("worker")


class TaskWorker:
    def __init__(
        self,
        api_url: str,
        redis_url: str,
        worker_id: str | None = None,
    ):
        self.api_url = api_url.rstrip("/")
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.redis = get_redis(redis_url)
        self._stop_event = threading.Event()
        self._heartbeat_events: dict[str, threading.Event] = {}

    def start(self):
        logger.info(f"Worker daemon booted. ID: {self.worker_id}")
        self._recover_processing_queue()
        while not self._stop_event.is_set():
            try:
                self._claim_and_execute()
            except Exception as exc:
                logger.error(f"Unexpected cycle error: {exc}")

    def stop(self):
        self._stop_event.set()
        for stop_event in self._heartbeat_events.values():
            stop_event.set()

    def _recover_processing_queue(self):
        proc_key = processing_key(self.worker_id)
        while True:
            raw = self.redis.rpop(proc_key)
            if raw is None:
                break

            job_data = deserialize_job_message(raw)
            job_id = job_data["id"]

            self.redis.delete(heartbeat_key(job_id))
            self.redis.zrem(ZSET_RUNNING, job_id)

            try:
                result = self.report_failure(job_id, "Worker restarted before finishing the job.")
            except requests.RequestException as exc:
                logger.error(f"Could not recover interrupted job {job_id}: {exc}")
                self.redis.rpush(proc_key, raw)
                break

            if result["job_state"] == "PENDING":
                retry_raw = serialize_job_message(
                    job_id=job_data["id"],
                    job_type=job_data["job_type"],
                    payload=job_data["payload"],
                    priority=job_data["priority"],
                    max_retries=result["max_retries"],
                    retry_count=result["retry_count"],
                )
                self.redis.lpush(queue_name_for_priority(job_data["priority"]), retry_raw)
                logger.warning(f"Recovered job {job_id} back to the queue")
            else:
                logger.error(f"Recovered job {job_id} is now marked FAILED")

    def _claim_and_execute(self):
        job_data = self.request_next_job()
        if job_data is None:
            return

        proc_key = processing_key(self.worker_id)
        raw = serialize_job_message(
            job_id=job_data["id"],
            job_type=job_data["job_type"],
            payload=job_data.get("payload", {}),
            priority=job_data["priority"],
            max_retries=job_data["max_retries"],
            retry_count=job_data["retry_count"],
        )
        job_id = job_data["id"]
        job_type = job_data["job_type"]
        payload = job_data.get("payload", {})

        logger.info(f"Claimed Job {job_id} [{job_type}]")

        deadline = time.time() + settings.JOB_TIMEOUT_SECONDS
        self.redis.zadd(ZSET_RUNNING, {job_id: deadline})
        self._start_heartbeat(job_id)

        try:
            self.execute(job_type, payload)
        except Exception as exc:
            err_msg = str(exc)
            logger.error(f"Job {job_id} failed: {err_msg}")
            self._finalize_failure(job_data, raw, proc_key, err_msg)
            return

        self._finalize_success(job_id, raw, proc_key)

    def _finalize_success(self, job_id: str, raw: str, proc_key: str):
        try:
            self.report_completion(job_id)
        except requests.RequestException as exc:
            logger.error(f"Job {job_id} executed, but completion callback failed: {exc}")
            self._abandon_inflight(job_id)
            return

        logger.info(f"Job {job_id} completed successfully")
        self._cleanup_tracking(job_id, raw, proc_key)

    def _finalize_failure(self, job_data: dict, raw: str, proc_key: str, error: str):
        job_id = job_data["id"]
        try:
            result = self.report_failure(job_id, error)
        except requests.RequestException as exc:
            logger.error(f"Job {job_id} failed, but failure callback also failed: {exc}")
            self._abandon_inflight(job_id)
            return

        self._cleanup_tracking(job_id, raw, proc_key)

        if result["job_state"] == "PENDING":
            backoff = 2 ** max(result["retry_count"], 1)
            logger.warning(
                f"Job {job_id} will retry in {backoff}s "
                f"(attempt {result['retry_count']}/{result['max_retries']})"
            )
            time.sleep(backoff)
            retry_raw = serialize_job_message(
                job_id=job_id,
                job_type=job_data["job_type"],
                payload=job_data["payload"],
                priority=job_data["priority"],
                max_retries=result["max_retries"],
                retry_count=result["retry_count"],
            )
            self.redis.lpush(queue_name_for_priority(job_data["priority"]), retry_raw)
        else:
            logger.error(f"Job {job_id} permanently failed")

    def _cleanup_tracking(self, job_id: str, raw: str, proc_key: str):
        stop_event = self._heartbeat_events.pop(job_id, None)
        if stop_event is not None:
            stop_event.set()
        self.redis.delete(heartbeat_key(job_id))
        self.redis.zrem(ZSET_RUNNING, job_id)
        self.redis.lrem(proc_key, 1, raw)

    def _abandon_inflight(self, job_id: str):
        stop_event = self._heartbeat_events.pop(job_id, None)
        if stop_event is not None:
            stop_event.set()
        self.redis.delete(heartbeat_key(job_id))
        self.redis.zadd(ZSET_RUNNING, {job_id: time.time()})

    def request_next_job(self) -> dict | None:
        try:
            response = requests.post(
                f"{self.api_url}/next-job",
                json={"worker_id": self.worker_id},
                timeout=5,
            )
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            logger.error(f"Could not claim the next job: {exc}")
            return None

    def report_completion(self, job_id: str):
        self._post_with_retry(
            f"{self.api_url}/jobs/{job_id}/complete",
            {"worker_id": self.worker_id},
        )

    def report_failure(self, job_id: str, err_msg: str) -> dict:
        response = self._post_with_retry(
            f"{self.api_url}/jobs/{job_id}/fail",
            {"worker_id": self.worker_id, "error_message": err_msg},
        )
        return response.json()

    def _post_with_retry(self, url: str, body: dict):
        last_error = None
        for attempt in range(1, 4):
            try:
                response = requests.post(url, json=body, timeout=5)
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last_error = exc
                logger.warning(f"Request attempt {attempt}/3 failed for {url}: {exc}")
                if attempt < 3:
                    time.sleep(1)

        if last_error is not None:
            raise last_error

    def _start_heartbeat(self, job_id: str):
        stop_event = threading.Event()
        self._heartbeat_events[job_id] = stop_event
        thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(job_id, stop_event),
            daemon=True,
        )
        thread.start()

    def _heartbeat_loop(self, job_id: str, stop_event: threading.Event):
        interval = settings.HEARTBEAT_INTERVAL_SECONDS
        ttl = interval * 2
        key = heartbeat_key(job_id)

        while not self._stop_event.is_set() and not stop_event.is_set():
            self.redis.setex(key, ttl, self.worker_id)
            stop_event.wait(interval)

    def execute(self, job_type: str, payload: dict):
        if job_type == "sleep":
            duration = payload.get("duration", 5)
            logger.info(f"Sleeping for {duration}s...")
            time.sleep(duration)
        elif job_type == "http":
            url = payload.get("url")
            method = payload.get("method", "GET").upper()
            if not url:
                raise ValueError("Payload missing required target 'url'")
            logger.info(f"HTTP [{method}] → {url}")
            response = requests.request(method, url, timeout=10)
            response.raise_for_status()
        else:
            raise NotImplementedError(f"Unsupported job type: '{job_type}'")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default=os.environ.get("SCHEDULER_API_URL", settings.SCHEDULER_API_URL))
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", settings.REDIS_URL))
    parser.add_argument("--worker-id", default=os.environ.get("WORKER_ID"))
    args = parser.parse_args()

    worker = TaskWorker(
        api_url=args.api_url,
        redis_url=args.redis_url,
        worker_id=args.worker_id,
    )
    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info("Worker shutting down gracefully...")
        worker.stop()
        sys.exit(0)
