from collections import Counter

from scheduler.database import SessionLocal
from scheduler.models import Job

try:
    from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
    from prometheus_client.core import GaugeMetricFamily
except ModuleNotFoundError:  # pragma: no cover - optional for local imports
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"
    CollectorRegistry = None
    GaugeMetricFamily = None
    generate_latest = None


UNASSIGNED = "unassigned"


def metrics_supported() -> bool:
    return (
        CollectorRegistry is not None
        and GaugeMetricFamily is not None
        and generate_latest is not None
    )


def priority_label(priority: int) -> str:
    return f"{priority:02d}"


class SchedulerCollector:
    def collect(self):
        db = SessionLocal()
        try:
            jobs = db.query(Job).all()
        finally:
            db.close()

        status_counts = Counter(job.status for job in jobs)
        priority_counts = Counter((priority_label(job.priority), job.status) for job in jobs)
        running_by_worker = Counter(
            (job.worker_id or UNASSIGNED)
            for job in jobs
            if job.status == "RUNNING"
        )

        jobs_total = GaugeMetricFamily(
            "scheduler_jobs_total",
            "Current number of jobs by status.",
            labels=["status"],
        )
        for status, count in sorted(status_counts.items()):
            jobs_total.add_metric([status], count)
        yield jobs_total

        jobs_by_priority = GaugeMetricFamily(
            "scheduler_jobs_by_priority",
            "Current number of jobs by priority and status.",
            labels=["priority", "status"],
        )
        for priority in range(10, 0, -1):
            label = priority_label(priority)
            for status in ("PENDING", "RUNNING", "COMPLETED", "FAILED"):
                jobs_by_priority.add_metric([label, status], priority_counts.get((label, status), 0))
        yield jobs_by_priority

        running_jobs_by_worker = GaugeMetricFamily(
            "scheduler_running_jobs_by_worker",
            "Current number of running jobs by worker.",
            labels=["worker_id"],
        )
        for worker_id, count in sorted(running_by_worker.items()):
            running_jobs_by_worker.add_metric([worker_id], count)
        yield running_jobs_by_worker

        job_state = GaugeMetricFamily(
            "scheduler_job_state",
            "Current job state with descriptive labels.",
            labels=[
                "job_id",
                "job_type",
                "status",
                "priority",
                "worker_id",
                "retry_count",
                "max_retries",
            ],
        )
        retry_jobs = GaugeMetricFamily(
            "scheduler_retry_jobs",
            "Jobs that have retried at least once.",
            labels=[
                "job_id",
                "job_type",
                "status",
                "priority",
                "worker_id",
                "retry_count",
                "max_retries",
            ],
        )

        for job in jobs:
            labels = [
                job.id,
                job.job_type,
                job.status,
                priority_label(job.priority),
                job.worker_id or UNASSIGNED,
                str(job.retry_count),
                str(job.max_retries),
            ]
            job_state.add_metric(labels, 1)
            if job.retry_count > 0:
                retry_jobs.add_metric(labels, job.retry_count)

        yield job_state
        yield retry_jobs


def render_metrics() -> bytes:
    if not metrics_supported():
        raise RuntimeError("prometheus_client is not installed")

    registry = CollectorRegistry()
    registry.register(SchedulerCollector())
    return generate_latest(registry)
