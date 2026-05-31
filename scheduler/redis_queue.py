import json
from typing import Any

try:
    import redis
except ModuleNotFoundError:
    redis = None


QUEUE_PREFIX = "jobs:priority"
PRIORITY_LEVELS = tuple(range(10, 0, -1))
ZSET_RUNNING = "jobs:running"


def processing_key(worker_id: str) -> str:
    return f"jobs:processing:{worker_id}"


def heartbeat_key(job_id: str) -> str:
    return f"heartbeat:{job_id}"


def queue_name_for_priority(priority: int) -> str:
    if priority not in PRIORITY_LEVELS:
        raise ValueError(f"Priority must be between 1 and 10, got {priority}")
    return f"{QUEUE_PREFIX}:{priority:02d}"


def priority_queue_names() -> tuple[str, ...]:
    return tuple(queue_name_for_priority(priority) for priority in PRIORITY_LEVELS)


def get_redis(redis_url: str):
    if redis is None:
        raise ModuleNotFoundError("redis is required to use the runtime queue")
    return redis.from_url(redis_url, decode_responses=True)


def serialize_job_message(
    *,
    job_id: str,
    job_type: str,
    payload: dict[str, Any],
    priority: int,
    max_retries: int,
    retry_count: int,
) -> str:
    return json.dumps(
        {
            "id": job_id,
            "job_type": job_type,
            "payload": payload,
            "priority": priority,
            "max_retries": max_retries,
            "retry_count": retry_count,
        }
    )


def deserialize_job_message(raw: str) -> dict[str, Any]:
    return json.loads(raw)


def remove_processing_entry(redis_client, worker_id: str, job_id: str) -> None:
    if not worker_id:
        return

    proc_key = processing_key(worker_id)
    for raw in redis_client.lrange(proc_key, 0, -1):
        try:
            if deserialize_job_message(raw).get("id") == job_id:
                redis_client.lrem(proc_key, 1, raw)
                return
        except json.JSONDecodeError:
            continue
