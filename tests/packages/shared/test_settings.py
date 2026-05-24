from pathlib import Path

from shared.config import settings as settings_module
from shared.config.settings import Settings


def test_env_file_points_to_project_root() -> None:
    project_root = Path(__file__).resolve().parents[3]

    assert project_root == settings_module._BASE_DIR
    assert settings_module.Settings.model_config.get("env_file") == str(project_root / ".env")


def test_settings_parses_mongodb_additional_options_from_env(monkeypatch) -> None:
    monkeypatch.setenv("MONGODB_ADDITIONAL_OPTIONS", '{"replicaSet": "rs0"}')

    settings = Settings()

    assert settings.mongodb_additional_options == {"replicaSet": "rs0"}
