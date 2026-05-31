## Architecture and Design Choices

The design is intentionally small and practical. I wanted the project to be easy to reason about, easy to run locally, and still clear about the core responsibilities: queueing work, executing it with multiple workers, and recovering from worker failures.

### Architecture

The project is split into a few focused parts:

- `scheduler/api.py` owns job creation and state transitions. It writes durable job records to PostgreSQL and pushes new work into Redis queues.
- `scheduler/models.py` and `scheduler/database.py` define the job schema and database session management. PostgreSQL is the durable source of truth for job state and retry history.
- `scheduler/redis_queue.py` centralizes the Redis queue names, message format, heartbeat keys, and processing-list helpers so the worker and sweeper follow the same protocol.
- `scheduler/worker.py` asks the API for the next job, executes the work, and reports completion or failure.
- `scheduler/sweeper.py` watches for timed-out `RUNNING` jobs. If a worker stops heartbeating, the sweeper requeues the job when retries are still available or marks it as failed when the retry budget is exhausted.

### Queue Flow

The runtime flow looks like this:

1. A client creates a job through the API.
2. The API saves the job in PostgreSQL with status `PENDING`.
3. The API serializes the job metadata and pushes it into the correct Redis queue.
4. A worker calls `POST /next-job`, and the API atomically moves one job into that worker's processing list while marking it `RUNNING`.
5. While the job is running, the worker updates a Redis heartbeat key.
6. On success, the worker calls the API to mark the job `COMPLETED`.
7. On failure, the worker calls the API to update retry state. The worker or sweeper requeues the job when retries remain.

This split keeps durability in PostgreSQL and fast queue operations in Redis.

### Why I Did Not Use Kafka, NATS, Grafana, or Prometheus

I did not use Kafka or NATS because they would make the solution much heavier than it needed to be. They are strong choices for high-throughput event systems, but this project only needs a small queue with retries and worker recovery.

I also removed Grafana and Prometheus from the final version. They are useful for observability, but they are not needed to demonstrate the scheduler logic itself. Leaving them out keeps the submission focused on the actual scheduling behavior instead of the monitoring layer.

### Why Redis Was a Good Fit Here

Redis is a good middle ground for this version of the project:

- it gives fast queue operations
- it supports worker processing lists naturally
- it makes heartbeats and timeout tracking simple
- it keeps the worker flow easy to follow

PostgreSQL still keeps the durable job record, so Redis is used for coordination rather than long-term storage.

### Could Those Tools Be Used?

Yes.

- Kafka or NATS could carry jobs as messages instead of Redis lists.
- Grafana and Prometheus could be added later for metrics and dashboards.
- Redis Streams could also be a natural evolution if the project needed richer delivery semantics.

For this assignment, I preferred a smaller design that is easier to explain and easier to verify.

### Why I Used SQLite in Unit Tests

The application runs with PostgreSQL in the real stack, but I used SQLite in the unit tests to keep the test environment lightweight.

Unit tests should be quick and easy to run. I did not want someone to need Docker or a running PostgreSQL instance just to check the basic logic of the project.

In these tests, I am mainly checking things like job creation, state transitions, retry handling, queue cleanup, and worker execution behavior. Those cases do not need PostgreSQL-specific locking.

So SQLite is only there to keep local unit testing simple, while PostgreSQL remains the correct runtime database for the actual scheduler stack.
