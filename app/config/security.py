"""Security configuration values — CORS origins, future JWT settings."""

# Allowed CORS origins. Override per environment (e.g. via env var in production).
ALLOWED_CORS_ORIGINS: list[str] = ["*"]
