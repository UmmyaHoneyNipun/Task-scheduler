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
