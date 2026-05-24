.PHONY: format lint check docker-up down dev \
        db-migrate db-upgrade \
        prefect-server prefect-pool prefect-pool-inspect \
        prefect-worker prefect-deploy prefect-deploy-sync prefect-deploy-clean

# Best command to auto-format and fix everything
format:
	uv run ruff check --select I --fix .  # Sort imports first
	uv run ruff format .                  # Format code
	uv run ruff check --fix .             # Fix any other auto-fixable lint errors

# Lint code and automatically fix what can be fixed
lint:
	uv run ruff check --fix .

# Check code (useful for CI)
check:
	uv run ruff format --check .
	uv run ruff check .

# Start background services (MariaDB, MongoDB, MinIO)
docker-up:
	docker compose up -d

# Stop background services
down:
	docker compose down

# Run the FastAPI application in development mode
dev:
	uv run python apps/api-service/src/main.py

# ── Database Migrations ───────────────────────────────────────────────────────

# Auto-generate a new migration script based on changes in your models
db-migrate:
	uv run alembic revision --autogenerate -m "auto-migration"

# Apply all pending migrations to the database
db-upgrade:
	uv run alembic upgrade head

# ── Prefect ───────────────────────────────────────────────────────────────────

# 1) Start the Prefect server + UI  →  http://localhost:4200
#    Run in a dedicated terminal; keep it alive while developing.
prefect-server:
	uv run prefect server start

# 2) Create the process work pool (run once per environment).
#    Safe to re-run — exits cleanly if the pool already exists.
prefect-pool:
	uv run prefect work-pool create tanalysis-pool --type process --overwrite

# Inspect the work pool (queue status, worker count, etc.)
prefect-pool-inspect:
	uv run prefect work-pool inspect tanalysis-pool

# 3) Register / update all deployment definitions from prefect.yaml.
#    Re-run whenever you change prefect.yaml or want to push new code.
prefect-deploy:
	uv run prefect --no-prompt deploy --all

# Deploy individual flows (useful during development)
prefect-deploy-sync:
	uv run prefect --no-prompt deploy --name jira-sync

prefect-deploy-clean:
	uv run prefect --no-prompt deploy --name jira-clean

# 4) Start the worker — polls tanalysis-pool and executes scheduled runs.
#    Run in a dedicated terminal after deploying.
prefect-worker:
	uv run prefect worker start --pool tanalysis-pool
