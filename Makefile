.PHONY: format lint check up dev down

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
