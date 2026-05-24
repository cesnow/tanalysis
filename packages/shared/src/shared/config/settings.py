"""Application settings — a single source of truth for all environment variables."""

from pathlib import Path

from pydantic_settings import BaseSettings

# Project root: tanalysis/
# parents[0]=config/  parents[1]=shared/  parents[2]=src/
# parents[3]=packages/shared/  parents[4]=packages/  parents[5]=tanalysis/
_BASE_DIR = Path(__file__).resolve().parents[5]


class Settings(BaseSettings):
    # ── Jira ──────────────────────────────────────────────────────────────────
    jira_base_url: str = "https://your-domain.atlassian.net"
    jira_email: str = ""
    jira_api_token: str = ""
    jira_project_key: str = ""

    # ── MongoDB ───────────────────────────────────────────────────────────────
    mongodb_enabled: bool = True
    mongodb_url: str = "mongodb://localhost:27017"
    mongodb_database: str = "tanalysis"
    mongodb_additional_options: dict[str, str] = {}

    # ── Redis ─────────────────────────────────────────────────────────────────
    redis_enabled: bool = True
    redis_url: str = "redis://localhost:6379/0"

    # ── MariaDB ───────────────────────────────────────────────────────────────
    mariadb_url: str = "mysql+pymysql://root:root@localhost:3306/tanalysis"

    # ── MinIO ─────────────────────────────────────────────────────────────────
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "tanalysis"
    minio_secure: bool = False

    # ── Prefect ───────────────────────────────────────────────────────────────
    prefect_api_url: str = "http://localhost:4200/api"

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: str = "INFO"

    # ── Observability ─────────────────────────────────────────────────────────
    metrics_enabled: bool = True

    # ── Environment ───────────────────────────────────────────────────────────
    environment: str = "development"  # development | production

    model_config = {
        # Resolves to tanalysis/.env — correct project-root path.
        # K8s pods inject env vars directly; missing .env is silently ignored.
        "env_file": str(_BASE_DIR / ".env"),
        "env_file_encoding": "utf-8",
    }


settings = Settings()
