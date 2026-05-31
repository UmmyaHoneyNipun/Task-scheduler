# OTEE Task Scheduler

Simple distributed task scheduler built with FastAPI, PostgreSQL, and Docker Compose.

## Current stack
- Python
- FastAPI
- PostgreSQL
- Docker Compose for local development

## What is in the project

- An API for creating and managing scheduled tasks according to priority (1-10 where 10 means highest and 1 lowest)
- Worker processes that claim and execute tasks
- A timeout sweeper that requeues or fails stalled jobs
- Unit tests for job creation, execution, and worker behavior

## Swagger UI
docker compose up -d --build
http://localhost:8001/docs

## Run the unit tests
pytest -q tests/test_scheduler.py

## Run the stack and unit tests together
docker compose up --build

The `test` service runs the unit suite automatically as part of the default Compose stack.

## Watch the log
docker compose logs -f
