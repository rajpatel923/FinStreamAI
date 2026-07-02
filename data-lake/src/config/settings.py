import sys
from pathlib import Path

# Add repo root to sys.path so finstream_config is importable
_here = Path(__file__).resolve()
_REPO_ROOT = next(
    (p for p in _here.parents if (p / "finstream_config").is_dir()),
    _here.parents[2],
)
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from finstream_config.settings import Settings as _BaseSettings  # noqa: E402
from pydantic_settings import SettingsConfigDict  # noqa: E402


class DataLakeSettings(_BaseSettings):
    # MinIO / S3-compatible object storage
    MINIO_ENDPOINT: str = "http://localhost:9000"
    DATALAKE_BUCKET_NAME: str = "finstreami-datalake"

    # Port for Prometheus metrics / FastAPI
    DATA_LAKE_METRICS_PORT: int = 8004

    model_config = SettingsConfigDict(
        env_file=(_REPO_ROOT / ".env", ".env"),
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    @property
    def delta_storage_options(self) -> dict:
        """boto3-compatible storage options for delta-rs."""
        return {
            "endpoint_url": self.MINIO_ENDPOINT,
            "aws_access_key_id": self.MINIO_ROOT_USER if hasattr(self, "MINIO_ROOT_USER") else self.AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": self.MINIO_ROOT_PASSWORD if hasattr(self, "MINIO_ROOT_PASSWORD") else self.AWS_SECRET_ACCESS_KEY,
            "aws_region": self.AWS_REGION,
            "allow_http": "true",
        }

    @property
    def bronze_path(self) -> str:
        return f"s3://{self.DATALAKE_BUCKET_NAME}/bronze"

    @property
    def silver_path(self) -> str:
        return f"s3://{self.DATALAKE_BUCKET_NAME}/silver"

    @property
    def gold_path(self) -> str:
        return f"s3://{self.DATALAKE_BUCKET_NAME}/gold"


# Add MINIO_ROOT_USER / MINIO_ROOT_PASSWORD support
from pydantic import Field  # noqa: E402


class _ExtendedSettings(DataLakeSettings):
    MINIO_ROOT_USER: str = Field(default="minioadmin")
    MINIO_ROOT_PASSWORD: str = Field(default="minioadmin123")

    @property
    def delta_storage_options(self) -> dict:
        return {
            "endpoint_url": self.MINIO_ENDPOINT,
            "aws_access_key_id": self.MINIO_ROOT_USER,
            "aws_secret_access_key": self.MINIO_ROOT_PASSWORD,
            "aws_region": self.AWS_REGION,
            "allow_http": "true",
            # MinIO has no native atomic-rename lock; delta-rs requires this
            # opt-out or every write after the first fails with
            # "A Delta Lake table already exists at that location".
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }


settings = _ExtendedSettings()
