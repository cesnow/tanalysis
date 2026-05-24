.PHONY: format lint check docker-up down dev \
        prefect-server prefect-pool prefect-pool-inspect \
        prefect-worker prefect-deploy prefect-deploy-sync prefect-deploy-clean

# Best command to auto-format and fix everything
format:
	ruff check --select I --fix .  # Sort imports first
	ruff format .                  # Format code
	ruff check --fix .             # Fix any other auto-fixable lint errors

# Lint code and automatically fix what can be fixed
lint:
	ruff check --fix .

# Check code (useful for CI)
check:
	ruff format --check .
	ruff check .

# Start background services (MariaDB, MongoDB, MinIO)
docker-up:
	docker compose up -d

# Stop background services
down:
	docker compose down

# Run the FastAPI application in development mode
dev:
	python main.py

# ── Prefect ───────────────────────────────────────────────────────────────────

# 1) Start the Prefect server + UI  →  http://localhost:4200
#    Run in a dedicated terminal; keep it alive while developing.
prefect-server:
	prefect server start

# 2) Create the process work pool (run once per environment).
#    Safe to re-run — exits cleanly if the pool already exists.
prefect-pool:
	prefect work-pool create tanalysis-pool --type process --overwrite

# Inspect the work pool (queue status, worker count, etc.)
prefect-pool-inspect:
	prefect work-pool inspect tanalysis-pool

# 3) Register / update all deployment definitions from prefect.yaml.
#    Re-run whenever you change prefect.yaml or want to push new code.
prefect-deploy:
	prefect --no-prompt deploy --all

# Deploy individual flows (useful during development)
prefect-deploy-sync:
	prefect --no-prompt deploy --name jira-sync

prefect-deploy-clean:
	prefect --no-prompt deploy --name jira-clean

# 4) Start the worker — polls tanalysis-pool and executes scheduled runs.
#    Run in a dedicated terminal after deploying.
prefect-worker:
	prefect worker start --pool tanalysis-pool
