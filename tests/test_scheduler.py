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
from scheduler.api import JobCreate, JobFailureRequest, WorkerRequest
from scheduler.models import Job
from scheduler.worker import TaskWorker


def _make_job(**overrides):
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


def test_creating_a_job_stores_it_as_pending():
    db = MagicMock()

    created_jobs = []

    def capture_add(job):
        created_jobs.append(job)

    def hydrate_job(job):
        job.id = "job-created"
        job.retry_count = 0
        job.worker_id = None

    db.add.side_effect = capture_add
    db.refresh.side_effect = hydrate_job

    result = api.create_job(
        JobCreate(job_type="sleep", payload={"duration": 2}, priority=4, max_retries=3),
        db=db,
    )

    assert len(created_jobs) == 1
    created_job = created_jobs[0]
    assert created_job.status == "PENDING"
    assert created_job.priority == 4
    assert created_job.max_retries == 3
    assert result is created_job
    assert result.id == "job-created"
    db.commit.assert_called_once()


def test_worker_can_load_its_own_running_job():
    db = MagicMock()
    job = _make_job(id="job-42", worker_id="worker-alpha", status="RUNNING")
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
    db.query.return_value.filter.return_value.first.return_value = _make_job(status="PENDING")

    with pytest.raises(HTTPException) as exc_info:
        api.get_running_job_for_worker("job-1", "worker-alpha", db)

    assert exc_info.value.status_code == 400


def test_worker_cannot_update_another_workers_running_job():
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = _make_job(worker_id="worker-beta")

    with pytest.raises(HTTPException) as exc_info:
        api.get_running_job_for_worker("job-1", "worker-alpha", db)

    assert exc_info.value.status_code == 403


def test_completing_a_job_marks_it_as_completed_and_keeps_worker_history(monkeypatch):
    db = MagicMock()
    job = _make_job(id="job-complete", priority=9)
    db.query.return_value.filter.return_value.first.return_value = job

    result = api.complete_job("job-complete", WorkerRequest(worker_id="worker-alpha"), db=db)

    assert result == {"status": "success", "id": "job-complete"}
    assert job.status == "COMPLETED"
    assert job.worker_id == "worker-alpha"
    assert isinstance(job.updated_at, datetime)
    db.commit.assert_called_once()


def test_failing_a_job_requeues_it_when_retries_are_left(monkeypatch):
    db = MagicMock()
    job = _make_job(id="job-retry", retry_count=0, max_retries=2, status="RUNNING")
    monkeypatch.setattr(api, "get_running_job_for_worker", MagicMock(return_value=job))

    result = api.fail_job(
        "job-retry",
        JobFailureRequest(worker_id="worker-alpha", error_message="temporary issue"),
        db=db,
    )

    assert result == {"status": "processed", "job_state": "PENDING"}
    assert job.retry_count == 1
    assert job.status == "PENDING"
    assert job.worker_id is None
    assert "temporary issue" in job.error_message
    db.commit.assert_called_once()


def test_failing_a_job_marks_it_failed_after_the_last_retry(monkeypatch):
    db = MagicMock()
    job = _make_job(id="job-failed", retry_count=1, max_retries=1, status="RUNNING")
    monkeypatch.setattr(api, "get_running_job_for_worker", MagicMock(return_value=job))

    result = api.fail_job(
        "job-failed",
        JobFailureRequest(worker_id="worker-alpha", error_message="permanent issue"),
        db=db,
    )

    assert result == {"status": "processed", "job_state": "FAILED"}
    assert job.status == "FAILED"
    assert job.worker_id is None
    assert job.error_message == "permanent issue"
    db.commit.assert_called_once()


def test_sleep_jobs_use_the_requested_duration(monkeypatch):
    worker = TaskWorker("http://scheduler.test", worker_id="worker-alpha")
    sleep = MagicMock()
    monkeypatch.setattr("scheduler.worker.time.sleep", sleep)

    worker.execute("sleep", {"duration": 4})

    sleep.assert_called_once_with(4)


def test_sleep_jobs_fall_back_to_the_default_duration(monkeypatch):
    worker = TaskWorker("http://scheduler.test", worker_id="worker-alpha")
    sleep = MagicMock()
    monkeypatch.setattr("scheduler.worker.time.sleep", sleep)

    worker.execute("sleep", {})

    sleep.assert_called_once_with(5)


def test_http_jobs_make_the_expected_request(monkeypatch):
    worker = TaskWorker("http://scheduler.test", worker_id="worker-alpha")
    response = MagicMock()
    request = MagicMock(return_value=response)
    monkeypatch.setattr("scheduler.worker.requests.request", request)

    worker.execute("http", {"url": "https://example.com", "method": "post"})

    request.assert_called_once_with("POST", "https://example.com", timeout=10)
    response.raise_for_status.assert_called_once()


def test_http_jobs_require_a_target_url():
    worker = TaskWorker("http://scheduler.test", worker_id="worker-alpha")

    with pytest.raises(ValueError):
        worker.execute("http", {"method": "GET"})


def test_unknown_job_types_are_rejected():
    worker = TaskWorker("http://scheduler.test", worker_id="worker-alpha")

    with pytest.raises(NotImplementedError):
        worker.execute("unknown", {})


def test_reporting_a_failure_sends_the_worker_id_and_error_message(monkeypatch):
    worker = TaskWorker("http://scheduler.test", worker_id="worker-alpha")
    response = MagicMock()
    post = MagicMock(return_value=response)
    monkeypatch.setattr("scheduler.worker.requests.post", post)

    worker.report_failure("job-1", "boom")

    post.assert_called_once_with(
        "http://scheduler.test/jobs/job-1/fail",
        json={"worker_id": "worker-alpha", "error_message": "boom"},
        timeout=5,
    )
    response.raise_for_status.assert_called_once()


def test_completion_reports_retry_until_the_api_accepts_them(monkeypatch):
    worker = TaskWorker("http://scheduler.test", worker_id="worker-alpha")
    success_response = MagicMock()
    post = MagicMock(side_effect=[requests.RequestException("timeout"), success_response])
    sleep = MagicMock()
    monkeypatch.setattr("scheduler.worker.requests.post", post)
    monkeypatch.setattr("scheduler.worker.time.sleep", sleep)

    worker.report_completion("job-1")

    assert post.call_count == 2
    sleep.assert_called_once_with(1)
    success_response.raise_for_status.assert_called_once()


def test_same_worker_can_repeat_a_completion_callback_safely():
    db = MagicMock()
    completed_job = _make_job(id="job-complete", status="COMPLETED")
    db.query.return_value.filter.return_value.first.return_value = completed_job

    result = api.complete_job("job-complete", WorkerRequest(worker_id="worker-alpha"), db=db)

    assert result == {"status": "success", "id": "job-complete"}
    db.commit.assert_not_called()


def test_worker_reports_completion_after_a_successful_execution(monkeypatch):
    worker = TaskWorker("http://scheduler.test", worker_id="worker-alpha")
    claim_response = MagicMock(status_code=200)
    claim_response.json.return_value = {
        "id": "job-1",
        "job_type": "sleep",
        "payload": {"duration": 1},
    }
    post = MagicMock(return_value=claim_response)
    execute = MagicMock()
    report_completion = MagicMock()
    monkeypatch.setattr("scheduler.worker.requests.post", post)
    monkeypatch.setattr(worker, "execute", execute)
    monkeypatch.setattr(worker, "report_completion", report_completion)

    worker.poll_and_execute()

    post.assert_called_once_with(
        "http://scheduler.test/next-job",
        json={"worker_id": "worker-alpha"},
        timeout=5,
    )
    execute.assert_called_once_with("sleep", {"duration": 1})
    report_completion.assert_called_once_with("job-1")


def test_worker_reports_failure_when_execution_raises(monkeypatch):
    worker = TaskWorker("http://scheduler.test", worker_id="worker-alpha")
    claim_response = MagicMock(status_code=200)
    claim_response.json.return_value = {
        "id": "job-2",
        "job_type": "sleep",
        "payload": {"duration": 1},
    }
    post = MagicMock(return_value=claim_response)
    execute = MagicMock(side_effect=RuntimeError("boom"))
    report_failure = MagicMock()
    monkeypatch.setattr("scheduler.worker.requests.post", post)
    monkeypatch.setattr(worker, "execute", execute)
    monkeypatch.setattr(worker, "report_failure", report_failure)

    worker.poll_and_execute()

    execute.assert_called_once_with("sleep", {"duration": 1})
    report_failure.assert_called_once_with("job-2", "boom")


def test_worker_stays_idle_when_the_queue_is_empty(monkeypatch):
    worker = TaskWorker("http://scheduler.test", worker_id="worker-alpha")
    claim_response = MagicMock(status_code=404)
    post = MagicMock(return_value=claim_response)
    monkeypatch.setattr("scheduler.worker.requests.post", post)

    worker.poll_and_execute()

    post.assert_called_once_with(
        "http://scheduler.test/next-job",
        json={"worker_id": "worker-alpha"},
        timeout=5,
    )


def test_worker_handles_scheduler_connection_errors_gracefully(monkeypatch):
    worker = TaskWorker("http://scheduler.test", worker_id="worker-alpha")
    monkeypatch.setattr(
        "scheduler.worker.requests.post",
        MagicMock(side_effect=requests.RequestException("offline")),
    )

    worker.poll_and_execute()


def test_worker_keeps_an_explicit_worker_id_when_one_is_given():
    worker = TaskWorker("http://scheduler.test", worker_id="worker-gamma")

    assert worker.worker_id == "worker-gamma"
