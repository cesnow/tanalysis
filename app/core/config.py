from pathlib import Path

from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    # Jira
    jira_base_url: str = "https://your-domain.atlassian.net"
    jira_email: str = ""
    jira_api_token: str = ""
    jira_project_key: str = ""
    # MongoDB
    mongodb_url: str = "mongodb://localhost:27017"
    mongodb_database: str = "tanalysis"

    # MariaDB
    mariadb_url: str = "mysql+pymysql://root:root@localhost:3306/tanalysis"

    # MinIO
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "tanalysis"
    minio_secure: bool = False

    # Prefect
    prefect_api_url: str = "http://localhost:4200/api"

    model_config = {
        "env_file": str(BASE_DIR / ".env"),  # 本地開發用；K8s 環境直接吃環境變數，.env 不存在時會自動略過
        "env_file_encoding": "utf-8",
        "env_file_required": False,  # .env 不存在時不報錯（K8s pods 無此檔案）
    }


settings = Settings()
