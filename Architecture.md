## Architecture and Design Choices

The design is intentionally simple. I wanted to keep the solution focused on the actual assignment requirements instead of adding extra tools that would make the project harder to run.

### Architecture

The project is split into a few clear parts:

- `scheduler/api.py` is the main scheduler and API layer. It is responsible for creating jobs, returning job status, and giving workers the next available job.
- `scheduler/models.py` and `scheduler/database.py` define the job model and database connection. PostgreSQL is used as the single source of truth for job state.
- `scheduler/worker.py` contains the worker logic. Workers continuously poll the API, execute jobs, and report whether the job completed or failed.
- `scheduler/sweeper.py` handles jobs that get stuck in the `RUNNING` state for too long. If a worker crashes or never reports back, the sweeper can either requeue the job or mark it as failed.

One important part of the design is how workers claim jobs. Workers take jobs in priority order, and PostgreSQL row locking is used to make sure two workers do not pick the same job at the same time.

For example, if Worker A claims a job, that job row is locked while it is being selected. If Worker B asks for a job at the same moment, PostgreSQL skips the locked row and gives Worker B the next available job instead. This keeps the worker logic simple and avoids duplicate processing.

### Why I Did Not Use Kafka, NATS, Redis, Grafana, or Prometheus

I avoided Kafka or NATS because they would make the project bigger than needed. The assignment can be solved clearly with PostgreSQL, so I kept the design simple and focused on the scheduler logic.

I also did not use Redis. Redis would work well for a fast queue or distributed lock, but the assignment requires persistent job state, retries, and status tracking. PostgreSQL already handles those requirements well, so using Redis would add another moving part without much benefit for this scope.

I removed Grafana and Prometheus as well. Monitoring is important in a real production system, but for this assignment I wanted the project to stay focused on the scheduler itself: job creation, job claiming, retries, worker execution, and timeout handling. I implemented those just to checkpout how it works, added a image which demonstrate how the Task Scheduler works. it was the better way to test .

### Could Those Tools Be Used?

Yes. I chose a smaller and more direct design that solves the assignment requirements first. Extra production tools can be added later, but they are not necessary to prove that the scheduler works correctly.

### Why I Used SQLite in Unit Tests

The application is designed to run with PostgreSQL, but I used SQLite for the unit tests because it keeps the test environment lightweight.

Unit tests should be quick and easy to run. I did not want someone to need Docker or a running PostgreSQL instance just to check the basic logic of the project.

In these tests, I am mainly checking things like job creation, status changes, retry handling, and timeout logic. Those parts do not require PostgreSQL-specific features.

PostgreSQL is still the right choice for the actual scheduler because it gives us row-level locking and `SKIP LOCKED`, which are important when multiple workers try to claim jobs at the same time.

So SQLite is used only to make local testing simple. The Development behavior that depends on PostgreSQL locking would be covered separately with integration tests.