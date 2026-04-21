from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
import os

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent


def _get_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _get_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _get_csv(name: str, default: str) -> list[str]:
    raw_value = os.getenv(name, default)
    return [item.strip() for item in raw_value.split(",") if item.strip()]


@dataclass(frozen=True)
class DatabaseSettings:
    host: str
    port: int
    name: str
    user: str
    password: str


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: list[str]
    transactions_topic: str
    dead_letter_topic: str
    consumer_group: str


@dataclass(frozen=True)
class ModelSettings:
    path: str
    auto_train_if_missing: bool
    training_batch_size: int
    training_fraud_rate: float
    version: str


@dataclass(frozen=True)
class PipelineSettings:
    max_retries: int
    high_risk_threshold: float
    dashboard_refresh_seconds: int
    stale_after_minutes: int


@dataclass(frozen=True)
class FlaskSettings:
    host: str
    port: int
    debug: bool


@dataclass(frozen=True)
class AppSettings:
    base_dir: Path
    database: DatabaseSettings
    kafka: KafkaSettings
    model: ModelSettings
    pipeline: PipelineSettings
    flask: FlaskSettings

    @property
    def resolved_model_path(self) -> Path:
        model_path = Path(self.model.path)
        return model_path if model_path.is_absolute() else self.base_dir / model_path


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    return AppSettings(
        base_dir=BASE_DIR,
        database=DatabaseSettings(
            host=os.getenv("DATABASE_HOST", "localhost"),
            port=_get_int("DATABASE_PORT", 5432),
            name=os.getenv("DATABASE_NAME", "fraud_detection"),
            user=os.getenv("DATABASE_USER", "postgres"),
            password=os.getenv("DATABASE_PASSWORD", "postgres123"),
        ),
        kafka=KafkaSettings(
            bootstrap_servers=_get_csv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            transactions_topic=os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions"),
            dead_letter_topic=os.getenv("KAFKA_DEAD_LETTER_TOPIC", "transactions.dlq"),
            consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "fraud-detection-group"),
        ),
        model=ModelSettings(
            path=os.getenv("MODEL_PATH", "models/fraud_model.pkl"),
            auto_train_if_missing=_get_bool("MODEL_AUTO_TRAIN_IF_MISSING", True),
            training_batch_size=_get_int("MODEL_TRAINING_BATCH_SIZE", 1000),
            training_fraud_rate=_get_float("MODEL_TRAINING_FRAUD_RATE", 0.05),
            version=os.getenv("MODEL_VERSION", "random_forest_v2"),
        ),
        pipeline=PipelineSettings(
            max_retries=_get_int("PIPELINE_MAX_RETRIES", 2),
            high_risk_threshold=_get_float("PIPELINE_HIGH_RISK_THRESHOLD", 0.85),
            dashboard_refresh_seconds=_get_int("DASHBOARD_REFRESH_SECONDS", 5),
            stale_after_minutes=_get_int("PIPELINE_STALE_AFTER_MINUTES", 15),
        ),
        flask=FlaskSettings(
            host=os.getenv("FLASK_HOST", "0.0.0.0"),
            port=_get_int("FLASK_PORT", 5000),
            debug=_get_bool("FLASK_DEBUG", False),
        ),
    )

