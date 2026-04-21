from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from src.streaming.kafka_consumer import TransactionConsumer
from tests.test_support import build_test_settings


class FakeFuture:
    def get(self, timeout=None):
        return {"timeout": timeout}


class FakeDlqProducer:
    def __init__(self):
        self.messages = []
        self.closed = False

    def send(self, topic, value):
        self.messages.append((topic, value))
        return FakeFuture()

    def flush(self):
        return None

    def close(self):
        self.closed = True


class FakeDetector:
    def load_model(self):
        return True

    def predict_single(self, transaction):
        return {
            "transaction_id": transaction["transaction_id"],
            "predicted_fraud": True,
            "fraud_probability": 0.91,
            "confidence": "High",
            "risk_level": "HIGH",
            "risk_factors": ["high_amount"],
            "model_version": "test-model",
        }


class FakeDB:
    def __init__(self, save_results=None):
        self.save_results = list(save_results or [{"success": True, "status": "inserted"}])
        self.events = []
        self.closed = False

    def connect(self):
        return True

    def ensure_schema(self):
        return True

    def record_pipeline_event(self, **kwargs):
        self.events.append(kwargs)
        return True

    def save_transaction(self, *args, **kwargs):
        result = self.save_results.pop(0) if self.save_results else {"success": True, "status": "inserted"}
        return result

    def get_fraud_stats(self):
        return {
            "total_transactions": 1,
            "fraud_detected": 1,
            "fraud_rate": 100,
            "avg_amount": 250.0,
            "avg_latency_ms": 12,
            "dead_lettered_today": 0,
            "duplicate_events_today": 0,
        }

    def close(self):
        self.closed = True


def build_payload():
    return {
        "transaction_id": "TXN_TEST_001",
        "customer_id": "CUST_1001",
        "amount": 1200.00,
        "merchant": "Luxury Electronics Store",
        "location": "International",
        "timestamp": "2026-04-20T03:15:00",
        "is_fraud": True,
    }


def test_consumer_routes_invalid_payload_to_dead_letter():
    with TemporaryDirectory() as temp_dir:
        settings = build_test_settings(Path(temp_dir))
        fake_db = FakeDB()
        fake_dlq = FakeDlqProducer()
        consumer = TransactionConsumer(
            consumer=cast(Any, []),
            detector=cast(Any, FakeDetector()),
            db=cast(Any, fake_db),
            dlq_producer=cast(Any, fake_dlq),
            settings=settings,
        )

        result = consumer.process_payload({"transaction_id": "BROKEN"})

        assert result["status"] == "validation_failed"
        assert consumer.dlq_count == 1
        assert any(event["event_type"] == "validation_failed" for event in fake_db.events)
        assert fake_dlq.messages


def test_consumer_counts_duplicate_updates():
    with TemporaryDirectory() as temp_dir:
        settings = build_test_settings(Path(temp_dir))
        fake_db = FakeDB(save_results=[{"success": True, "status": "updated"}])
        consumer = TransactionConsumer(
            consumer=cast(Any, []),
            detector=cast(Any, FakeDetector()),
            db=cast(Any, fake_db),
            dlq_producer=cast(Any, FakeDlqProducer()),
            settings=settings,
        )

        result = consumer.process_payload(build_payload())

        assert result["success"] is True
        assert consumer.duplicate_updates == 1
        assert consumer.processed_count == 1


def test_consumer_retries_once_before_success():
    with TemporaryDirectory() as temp_dir:
        settings = build_test_settings(Path(temp_dir))
        fake_db = FakeDB(
            save_results=[
                {"success": False, "status": "db_error", "error": "temporary outage"},
                {"success": True, "status": "inserted"},
            ]
        )
        consumer = TransactionConsumer(
            consumer=cast(Any, []),
            detector=cast(Any, FakeDetector()),
            db=cast(Any, fake_db),
            dlq_producer=cast(Any, FakeDlqProducer()),
            settings=settings,
        )

        result = consumer.process_payload(build_payload())

        assert result["success"] is True
        assert consumer.retry_events == 1
        assert any(event["event_type"] == "processing_retry" for event in fake_db.events)


