# OTEE Task Scheduler

## Planned stack

- Python
- FastAPI
- PostgreSQL
- Docker Compose for local development

## Planned pieces

- An API for creating and managing scheduled tasks
- Worker processes that claim and execute tasks
- Database-backed coordination so multiple workers can run safely
- Basic local development tooling


## Swagger UI
docker compose up -d --build
http://localhost:8001/docs
