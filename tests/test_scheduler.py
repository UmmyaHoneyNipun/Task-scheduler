import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
from fastapi import HTTPException
from pydantic import ValidationError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault("DATABASE_URL", "sqlite:///./unit_test.db")

from scheduler import api
import scheduler.worker as worker_module
from scheduler.api import JobCreate, JobFailureRequest, WorkerRequest
from scheduler.models import Job
from scheduler.redis_queue import (
    ZSET_RUNNING,
    deserialize_job_message,
    processing_key,
    queue_name_for_priority,
    serialize_job_message,
)


class InMemoryRedis:
    def __init__(self):
        self._lists: dict[str, list[str]] = {}
        self._values: dict[str, str] = {}
        self._zsets: dict[str, dict[str, float]] = {}

    def lpush(self, key: str, *values: str) -> int:
        items = self._lists.setdefault(key, [])
        for value in values:
            items.insert(0, value)
        return len(items)

    def rpush(self, key: str, *values: str) -> int:
        items = self._lists.setdefault(key, [])
        items.extend(values)
        return len(items)

    def lrange(self, key: str, start: int, end: int) -> list[str]:
        items = self._lists.get(key, [])
        if end == -1:
            end = len(items) - 1
        if end < start:
            return []
        return items[start : end + 1]

    def rpop(self, key: str) -> str | None:
        items = self._lists.get(key, [])
        if not items:
            return None
        return items.pop()

    def rpoplpush(self, source: str, destination: str) -> str | None:
        value = self.rpop(source)
        if value is None:
            return None
        self.lpush(destination, value)
        return value

    def brpoplpush(self, source: str, destination: str, timeout: int = 0) -> str | None:
        return self.rpoplpush(source, destination)

    def lrem(self, key: str, count: int, value: str) -> int:
        items = self._lists.get(key, [])
        if not items:
            return 0

        if count < 0:
            items = list(reversed(items))
            count = abs(count)
            reverse_after = True
        else:
            reverse_after = False

        removed = 0
        remaining: list[str] = []
        for item in items:
            should_remove = item == value and (count == 0 or removed < count)
            if should_remove:
                removed += 1
                continue
            remaining.append(item)

        if reverse_after:
            remaining.reverse()

        self._lists[key] = remaining
        return removed

    def delete(self, *keys: str) -> int:
        removed = 0
        for key in keys:
            for store in (self._lists, self._values, self._zsets):
                if key in store:
                    del store[key]
                    removed += 1
        return removed

    def exists(self, key: str) -> int:
        return int(
            key in self._lists
            or key in self._values
            or key in self._zsets
        )

    def setex(self, key: str, ttl: int, value: str) -> bool:
        self._values[key] = value
        return True

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        items = self._zsets.setdefault(key, {})
        for member, score in mapping.items():
            items[member] = score
        return len(mapping)

    def zrem(self, key: str, *members: str) -> int:
        items = self._zsets.get(key, {})
        removed = 0
        for member in members:
            if member in items:
                del items[member]
                removed += 1
        return removed

    def zrange(self, key: str, start: int, end: int) -> list[str]:
        items = sorted(self._zsets.get(key, {}).items(), key=lambda item: (item[1], item[0]))
        members = [member for member, _score in items]
        if end == -1:
            end = len(members) - 1
        if end < start:
            return []
        return members[start : end + 1]

    def zrangebyscore(self, key: str, minimum: float, maximum: float) -> list[str]:
        items = sorted(self._zsets.get(key, {}).items(), key=lambda item: (item[1], item[0]))
        return [member for member, score in items if minimum <= score <= maximum]


@pytest.fixture
def fake_redis():
    return InMemoryRedis()


def build_worker(monkeypatch, fake_redis, worker_id="worker-alpha"):
    monkeypatch.setattr(worker_module, "get_redis", lambda _: fake_redis)
    return worker_module.TaskWorker(
        api_url="http://scheduler.test",
        redis_url="redis://unit-test",
        worker_id=worker_id,
    )


def make_job(**overrides):
    job = Job(
        id="job-1",
        job_type="sleep",
        payload={"duration": 1},
        priority=5,
        status="RUNNING",
        retry_count=0,
        max_retries=1,
        worker_id="worker-alpha",
        error_message=None,
    )
    job.created_at = datetime.now(UTC)
    job.updated_at = datetime.now(UTC)
    for key, value in overrides.items():
        setattr(job, key, value)
    return job


def queue_job(redis_client, *, job_id="job-1", priority=5, job_type="sleep", payload=None, retry_count=0, max_retries=3):
    raw = serialize_job_message(
        job_id=job_id,
        job_type=job_type,
        payload=payload or {"duration": 1},
        priority=priority,
        max_retries=max_retries,
        retry_count=retry_count,
    )
    queue_name = queue_name_for_priority(priority)
    redis_client.lpush(queue_name, raw)
    return raw, queue_name


def test_job_creation_accepts_a_valid_payload():
    job = JobCreate(
        job_type="sleep",
        payload={"duration": 3},
        priority=7,
        max_retries=2,
    )

    assert job.job_type == "sleep"
    assert job.payload == {"duration": 3}
    assert job.priority == 7
    assert job.max_retries == 2


@pytest.mark.parametrize("priority", [0, 11])
def test_job_creation_rejects_priority_outside_the_allowed_range(priority):
    with pytest.raises(ValidationError):
        JobCreate(
            job_type="sleep",
            payload={"duration": 1},
            priority=priority,
            max_retries=1,
        )


def test_creating_a_job_stores_it_and_pushes_it_to_the_redis_queue(monkeypatch, fake_redis):
    db = MagicMock()
    created_jobs = []

    monkeypatch.setattr(api, "get_redis", lambda _: fake_redis)

    def capture_add(job):
        created_jobs.append(job)

    def assign_identity():
        created_jobs[0].id = "job-created"
        created_jobs[0].retry_count = 0
        created_jobs[0].worker_id = None

    def hydrate_job(job):
        job.id = "job-created"
        job.retry_count = 0
        job.worker_id = None

    db.add.side_effect = capture_add
    db.flush.side_effect = assign_identity
    db.refresh.side_effect = hydrate_job

    result = api.create_job(
        JobCreate(job_type="sleep", payload={"duration": 2}, priority=9, max_retries=3),
        db=db,
    )

    assert result.id == "job-created"
    assert len(created_jobs) == 1
    created_job = created_jobs[0]
    assert created_job.status == "PENDING"
    assert created_job.priority == 9
    db.flush.assert_called_once()
    db.commit.assert_called_once()

    queued = fake_redis.lrange(queue_name_for_priority(9), 0, -1)
    assert len(queued) == 1
    message = deserialize_job_message(queued[0])
    assert message["id"] == "job-created"
    assert message["priority"] == 9


def test_next_job_claims_strict_priority_order(monkeypatch, fake_redis):
    db = MagicMock()
    job_ten = make_job(id="job-ten", status="PENDING", worker_id=None, priority=10)
    job_nine = make_job(id="job-nine", status="PENDING", worker_id=None, priority=9)
    job_two = make_job(id="job-two", status="PENDING", worker_id=None, priority=2)
    db.query.return_value.filter.return_value.first.side_effect = [job_ten, job_nine, job_two]
    monkeypatch.setattr(api, "get_redis", lambda _: fake_redis)

    queue_job(fake_redis, job_id="job-two", priority=2)
    queue_job(fake_redis, job_id="job-nine", priority=9)
    queue_job(fake_redis, job_id="job-ten", priority=10)

    first = api.get_next_job(WorkerRequest(worker_id="worker-alpha"), db=db)
    second = api.get_next_job(WorkerRequest(worker_id="worker-alpha"), db=db)
    third = api.get_next_job(WorkerRequest(worker_id="worker-alpha"), db=db)

    assert [first.id, second.id, third.id] == ["job-ten", "job-nine", "job-two"]
    assert job_ten.worker_id == "worker-alpha"
    assert job_nine.worker_id == "worker-alpha"
    assert job_two.worker_id == "worker-alpha"
    assert job_ten.status == "RUNNING"
    assert job_nine.status == "RUNNING"
    assert job_two.status == "RUNNING"
    assert fake_redis.lrange(queue_name_for_priority(10), 0, -1) == []
    assert fake_redis.lrange(queue_name_for_priority(9), 0, -1) == []
    assert fake_redis.lrange(queue_name_for_priority(2), 0, -1) == []
    assert len(fake_redis.lrange(processing_key("worker-alpha"), 0, -1)) == 3
    assert set(fake_redis.zrange(ZSET_RUNNING, 0, -1)) == {"job-ten", "job-nine", "job-two"}
    assert db.commit.call_count == 3


def test_next_job_returns_not_found_when_the_queue_is_empty(monkeypatch, fake_redis):
    monkeypatch.setattr(api, "get_redis", lambda _: fake_redis)
    db = MagicMock()

    with pytest.raises(HTTPException) as exc_info:
        api.get_next_job(WorkerRequest(worker_id="worker-alpha"), db=db)

    assert exc_info.value.status_code == 404


def test_next_job_resyncs_pending_jobs_from_the_database_when_redis_is_empty(monkeypatch, fake_redis):
    monkeypatch.setattr(api, "get_redis", lambda _: fake_redis)
    claimed_job = make_job(id="job-resynced", status="RUNNING", worker_id="worker-alpha", priority=10)
    claim = MagicMock(side_effect=[None, claimed_job])
    sync = MagicMock(return_value=1)
    monkeypatch.setattr(api, "claim_next_queued_job", claim)
    monkeypatch.setattr(api, "queue_pending_jobs", sync)
    db = MagicMock()

    result = api.get_next_job(WorkerRequest(worker_id="worker-alpha"), db=db)

    assert result is claimed_job
    sync.assert_called_once_with(fake_redis, db)
    assert claim.call_count == 2


def test_next_job_skips_malformed_queue_entries_before_claiming_a_valid_job(monkeypatch, fake_redis):
    db = MagicMock()
    job = make_job(id="job-valid", status="PENDING", worker_id=None, priority=9)
    db.query.return_value.filter.return_value.first.return_value = job
    monkeypatch.setattr(api, "get_redis", lambda _: fake_redis)

    fake_redis.lpush(queue_name_for_priority(9), "not-json-at-all")
    queue_job(fake_redis, job_id="job-valid", priority=9)

    result = api.get_next_job(WorkerRequest(worker_id="worker-alpha"), db=db)

    assert result.id == "job-valid"
    assert fake_redis.lrange(queue_name_for_priority(9), 0, -1) == []
    assert fake_redis.zrange(ZSET_RUNNING, 0, -1) == ["job-valid"]


def test_worker_can_load_its_own_running_job():
    db = MagicMock()
    job = make_job(id="job-42", worker_id="worker-alpha", status="RUNNING")
    db.query.return_value.filter.return_value.first.return_value = job

    result = api.get_running_job_for_worker("job-42", "worker-alpha", db)

    assert result is job


def test_loading_a_missing_running_job_returns_not_found():
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = None

    with pytest.raises(HTTPException) as exc_info:
        api.get_running_job_for_worker("missing", "worker-alpha", db)

    assert exc_info.value.status_code == 404


def test_loading_a_job_that_is_not_running_is_rejected():
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = make_job(status="PENDING")

    with pytest.raises(HTTPException) as exc_info:
        api.get_running_job_for_worker("job-1", "worker-alpha", db)

    assert exc_info.value.status_code == 400


def test_worker_cannot_update_another_workers_running_job():
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = make_job(worker_id="worker-beta")

    with pytest.raises(HTTPException) as exc_info:
        api.get_running_job_for_worker("job-1", "worker-alpha", db)

    assert exc_info.value.status_code == 403


def test_completing_a_job_marks_it_completed_and_keeps_worker_history():
    db = MagicMock()
    job = make_job(id="job-complete", priority=9)
    db.query.return_value.filter.return_value.first.return_value = job

    result = api.complete_job("job-complete", WorkerRequest(worker_id="worker-alpha"), db=db)

    assert result == {"status": "success", "id": "job-complete"}
    assert job.status == "COMPLETED"
    assert job.worker_id == "worker-alpha"
    assert isinstance(job.updated_at, datetime)
    db.commit.assert_called_once()


def test_failing_a_job_requeues_it_when_retries_are_left():
    db = MagicMock()
    job = make_job(id="job-retry", retry_count=0, max_retries=2, status="RUNNING")
    db.query.return_value.filter.return_value.first.return_value = job

    result = api.fail_job(
        "job-retry",
        JobFailureRequest(worker_id="worker-alpha", error_message="temporary issue"),
        db=db,
    )

    assert result["job_state"] == "PENDING"
    assert result["retry_count"] == 1
    assert job.retry_count == 1
    assert job.status == "PENDING"
    assert job.worker_id is None
    assert "temporary issue" in job.error_message


def test_failing_a_job_marks_it_failed_after_the_last_retry():
    db = MagicMock()
    job = make_job(id="job-failed", retry_count=1, max_retries=1, status="RUNNING")
    db.query.return_value.filter.return_value.first.return_value = job

    result = api.fail_job(
        "job-failed",
        JobFailureRequest(worker_id="worker-alpha", error_message="permanent issue"),
        db=db,
    )

    assert result["job_state"] == "FAILED"
    assert result["retry_count"] == 1
    assert job.status == "FAILED"
    assert job.worker_id is None
    assert job.error_message == "permanent issue"


def test_sleep_jobs_use_the_requested_duration(monkeypatch, fake_redis):
    worker = build_worker(monkeypatch, fake_redis)
    sleep = MagicMock()
    monkeypatch.setattr("scheduler.worker.time.sleep", sleep)

    worker.execute("sleep", {"duration": 4})

    sleep.assert_called_once_with(4)


def test_sleep_jobs_fall_back_to_the_default_duration(monkeypatch, fake_redis):
    worker = build_worker(monkeypatch, fake_redis)
    sleep = MagicMock()
    monkeypatch.setattr("scheduler.worker.time.sleep", sleep)

    worker.execute("sleep", {})

    sleep.assert_called_once_with(5)


def test_http_jobs_make_the_expected_request(monkeypatch, fake_redis):
    worker = build_worker(monkeypatch, fake_redis)
    response = MagicMock()
    request = MagicMock(return_value=response)
    monkeypatch.setattr("scheduler.worker.requests.request", request)

    worker.execute("http", {"url": "https://example.com", "method": "post"})

    request.assert_called_once_with("POST", "https://example.com", timeout=10)
    response.raise_for_status.assert_called_once()


def test_http_jobs_require_a_target_url(monkeypatch, fake_redis):
    worker = build_worker(monkeypatch, fake_redis)

    with pytest.raises(ValueError):
        worker.execute("http", {"method": "GET"})


def test_unknown_job_types_are_rejected(monkeypatch, fake_redis):
    worker = build_worker(monkeypatch, fake_redis)

    with pytest.raises(NotImplementedError):
        worker.execute("unknown", {})


def test_worker_requests_the_next_job_from_the_assignment_endpoint(monkeypatch, fake_redis):
    worker = build_worker(monkeypatch, fake_redis)
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"id": "job-1", "job_type": "sleep", "payload": {"duration": 1}, "priority": 4, "max_retries": 1, "retry_count": 0}
    post = MagicMock(return_value=response)
    monkeypatch.setattr("scheduler.worker.requests.post", post)

    assert worker.request_next_job()["id"] == "job-1"
    post.assert_called_once_with(
        "http://scheduler.test/next-job",
        json={"worker_id": "worker-alpha"},
        timeout=5,
    )


def test_worker_treats_an_empty_queue_as_idle(monkeypatch, fake_redis):
    worker = build_worker(monkeypatch, fake_redis)
    response = MagicMock()
    response.status_code = 404
    post = MagicMock(return_value=response)
    monkeypatch.setattr("scheduler.worker.requests.post", post)

    assert worker.request_next_job() is None


def test_reporting_a_failure_sends_the_worker_id_and_message(monkeypatch, fake_redis):
    worker = build_worker(monkeypatch, fake_redis)
    response = MagicMock()
    response.json.return_value = {"job_state": "FAILED", "retry_count": 1, "max_retries": 1}
    post = MagicMock(return_value=response)
    monkeypatch.setattr("scheduler.worker.requests.post", post)

    result = worker.report_failure("job-1", "boom")

    assert result["job_state"] == "FAILED"
    post.assert_called_once_with(
        "http://scheduler.test/jobs/job-1/fail",
        json={"worker_id": "worker-alpha", "error_message": "boom"},
        timeout=5,
    )


def test_completion_reports_retry_until_the_api_accepts_them(monkeypatch, fake_redis):
    worker = build_worker(monkeypatch, fake_redis)
    success_response = MagicMock()
    post = MagicMock(side_effect=[requests.RequestException("timeout"), success_response])
    sleep = MagicMock()
    monkeypatch.setattr("scheduler.worker.requests.post", post)
    monkeypatch.setattr("scheduler.worker.time.sleep", sleep)

    worker.report_completion("job-1")

    assert post.call_count == 2
    sleep.assert_called_once_with(1)
    success_response.raise_for_status.assert_called_once()


def test_worker_processes_a_claimed_job_and_cleans_up_on_success(monkeypatch, fake_redis):
    worker = build_worker(monkeypatch, fake_redis)
    raw = serialize_job_message(
        job_id="job-1",
        job_type="sleep",
        payload={"duration": 1},
        priority=4,
        max_retries=3,
        retry_count=0,
    )
    fake_redis.lpush(processing_key("worker-alpha"), raw)
    execute = MagicMock()
    request_next_job = MagicMock(
        return_value={
            "id": "job-1",
            "job_type": "sleep",
            "payload": {"duration": 1},
            "priority": 4,
            "max_retries": 3,
            "retry_count": 0,
        }
    )
    report_completion = MagicMock()
    monkeypatch.setattr(worker, "execute", execute)
    monkeypatch.setattr(worker, "request_next_job", request_next_job)
    monkeypatch.setattr(worker, "report_completion", report_completion)
    monkeypatch.setattr(worker, "_start_heartbeat", MagicMock())

    worker._claim_and_execute()

    request_next_job.assert_called_once_with()
    execute.assert_called_once_with("sleep", {"duration": 1})
    report_completion.assert_called_once_with("job-1")
    assert fake_redis.lrange(queue_name_for_priority(4), 0, -1) == []
    assert fake_redis.lrange(processing_key("worker-alpha"), 0, -1) == []
    assert fake_redis.zrange(ZSET_RUNNING, 0, -1) == []


def test_worker_requeues_a_job_when_execution_fails_but_retry_is_allowed(monkeypatch, fake_redis):
    worker = build_worker(monkeypatch, fake_redis)
    raw = serialize_job_message(
        job_id="job-2",
        job_type="sleep",
        payload={"duration": 1},
        priority=5,
        max_retries=3,
        retry_count=0,
    )
    fake_redis.lpush(processing_key("worker-alpha"), raw)
    monkeypatch.setattr(
        worker,
        "request_next_job",
        MagicMock(
            return_value={
                "id": "job-2",
                "job_type": "sleep",
                "payload": {"duration": 1},
                "priority": 5,
                "max_retries": 3,
                "retry_count": 0,
            }
        ),
    )
    monkeypatch.setattr(worker, "execute", MagicMock(side_effect=RuntimeError("boom")))
    monkeypatch.setattr(
        worker,
        "report_failure",
        MagicMock(return_value={"job_state": "PENDING", "retry_count": 1, "max_retries": 3}),
    )
    monkeypatch.setattr(worker, "_start_heartbeat", MagicMock())
    sleep = MagicMock()
    monkeypatch.setattr("scheduler.worker.time.sleep", sleep)

    worker._claim_and_execute()

    queued = fake_redis.lrange(queue_name_for_priority(5), 0, -1)
    assert len(queued) == 1
    assert deserialize_job_message(queued[0])["retry_count"] == 1
    assert fake_redis.lrange(processing_key("worker-alpha"), 0, -1) == []
    assert fake_redis.zrange(ZSET_RUNNING, 0, -1) == []
    sleep.assert_called_once_with(2)


def test_worker_leaves_recovery_breadcrumbs_when_failure_callback_breaks(monkeypatch, fake_redis):
    worker = build_worker(monkeypatch, fake_redis)
    raw = serialize_job_message(
        job_id="job-3",
        job_type="sleep",
        payload={"duration": 1},
        priority=5,
        max_retries=3,
        retry_count=0,
    )
    fake_redis.lpush(processing_key("worker-alpha"), raw)
    monkeypatch.setattr(
        worker,
        "request_next_job",
        MagicMock(
            return_value={
                "id": "job-3",
                "job_type": "sleep",
                "payload": {"duration": 1},
                "priority": 5,
                "max_retries": 3,
                "retry_count": 0,
            }
        ),
    )
    monkeypatch.setattr(worker, "execute", MagicMock(side_effect=RuntimeError("boom")))
    monkeypatch.setattr(worker, "report_failure", MagicMock(side_effect=requests.RequestException("offline")))
    monkeypatch.setattr(worker, "_start_heartbeat", MagicMock())

    worker._claim_and_execute()

    assert fake_redis.lrange(processing_key("worker-alpha"), 0, -1)
    assert fake_redis.zrange(ZSET_RUNNING, 0, -1) == ["job-3"]


def test_worker_stays_idle_when_the_queue_is_empty(monkeypatch, fake_redis):
    worker = build_worker(monkeypatch, fake_redis)
    monkeypatch.setattr(worker, "request_next_job", MagicMock(return_value=None))

    worker._claim_and_execute()

    worker.request_next_job.assert_called_once_with()
