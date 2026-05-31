# OTEE Task Scheduler

Simple distributed task scheduler built with FastAPI, PostgreSQL, Redis, and Docker Compose.

## Current stack
- Python
- FastAPI
- PostgreSQL
- Redis
- Docker Compose for local development

## What is in the project

- An API for creating and tracking scheduled jobs
- Redis-backed worker queues with three worker processes
- A timeout sweeper that requeues or fails stalled jobs
- Unit tests for job creation, worker execution, and retry behavior

## Run the stack

```bash
docker compose up --build
```

This starts:
- PostgreSQL
- Redis
- the API
- three workers
- the timeout sweeper
- the unit-test container

## Run the unit tests

```bash
pytest -q tests/test_scheduler.py
```

## Swagger UI

[http://localhost:8001/docs](http://localhost:8001/docs)

## Watch the logs

```bash
docker compose logs -f
```

## Create Bulk Task

```bash
for i in $(seq 1 100); do
  curl -s -X POST http://localhost:8001/jobs \
    -H "Content-Type: application/json" \
    -d "{
      \"job_type\": \"sleep\",
      \"payload\": {\"duration\": 5},
      \"priority\": $(( (i % 10) + 1 )),
      \"max_retries\": 1
    }" > /dev/null
done
```
