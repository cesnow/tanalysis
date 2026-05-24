# Tanalysis Context

This file provides architectural context, commands, and conventions for AI assistants to optimize context parsing. Follow these guidelines strictly.

## Project Architecture
- **App-Centric Monorepo**: Managed by `uv` workspaces.
  - `apps/`: Deployable services (`api-service`, `workflow-service`).
  - `packages/`: Reusable internal libraries (`shared`).
- **Flat `src/` Layout Rule**: Applications place entrypoints directly inside `src/` (e.g., `apps/api-service/src/main.py`) rather than nesting in package folders. Hatchling is configured to map these correctly.
- **Orchestration**: Prefect `@task` logic is isolated in `tasks/` and imported into `flows/` to decouple execution from flow definitions.

## Tech Stack
- **Runtime**: Python >= 3.11
- **Package Manager**: `uv`
- **Build System**: Hatchling
- **API Framework**: FastAPI
- **Workflow Orchestration**: Prefect
- **Data Stores**: MariaDB (SQLAlchemy/Alembic), MongoDB (PyMongo), Redis (Caching), MinIO (Object Storage)
- **Linter/Formatter**: Ruff

## Common Commands
- **Install & Sync**: `uv sync --extra dev --all-packages`
- **Format & Lint**: `make format` (runs `ruff` sorting, formatting, and fixes - ALWAYS use before committing)
- **Run API**: `make dev` (starts FastAPI via uvicorn)
- **Databases (Docker)**: `make docker-up` / `make down`
- **DB Migrations**: `make db-migrate` (autogenerate) / `make db-upgrade` (apply)
- **Prefect**: `make prefect-server`, `make prefect-worker`, `make prefect-deploy`

## Coding Conventions
- **Configuration**: Always centralize environment variables in `packages/shared/src/shared/config/settings.py` using `pydantic-settings`. Synchronize changes to `.env` and `.env.example`.
- **Imports**: Use absolute imports natively resolved via the `uv` workspace (e.g., `from shared.db.mongodb import client`).
- **Dependency Management**: Add packages to the specific project scope: `uv add <pkg> --package <app-or-shared>`.
- **Optional Services**: MongoDB and Redis connections conditionally load based on `MONGODB_ENABLED` and `REDIS_ENABLED`. Application logic must account for `None` clients if disabled.
- **Typing**: Use strict type hints and modern syntax (`|` instead of `Union`, `dict[str, Any]`, etc.).
