from pathlib import Path

from config.settings import (
    AppSettings,
    DatabaseSettings,
    FlaskSettings,
    KafkaSettings,
    ModelSettings,
    PipelineSettings,
)


def build_test_settings(base_dir: Path) -> AppSettings:
    return AppSettings(
        base_dir=base_dir,
        database=DatabaseSettings(
            host="localhost",
            port=5432,
            name="fraud_detection_test",
            user="postgres",
            password="postgres",
        ),
        kafka=KafkaSettings(
            bootstrap_servers=["localhost:9092"],
            transactions_topic="transactions",
            dead_letter_topic="transactions.dlq",
            consumer_group="fraud-detection-tests",
        ),
        model=ModelSettings(
            path=str(base_dir / "fraud_model.pkl"),
            auto_train_if_missing=True,
            training_batch_size=200,
            training_fraud_rate=0.1,
            version="random_forest_test",
        ),
        pipeline=PipelineSettings(
            max_retries=1,
            high_risk_threshold=0.85,
            dashboard_refresh_seconds=5,
            stale_after_minutes=15,
        ),
        flask=FlaskSettings(host="127.0.0.1", port=5001, debug=False),
    )

