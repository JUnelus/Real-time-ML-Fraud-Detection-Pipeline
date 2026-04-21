from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from dashboard import create_app
from tests.test_support import build_test_settings


class DashboardStubDB:
    def connect(self):
        return True

    def close(self):
        return None

    def get_fraud_stats(self):
        return {
            "total_transactions": 24,
            "fraud_detected": 3,
            "fraud_rate": 12.5,
            "avg_amount": 189.42,
            "total_fraud_amount": 1845.20,
            "avg_latency_ms": 38.4,
            "high_risk_alerts": 2,
            "dead_lettered_today": 1,
            "duplicate_events_today": 2,
            "errors_today": 1,
        }

    def get_recent_transactions(self, limit=10):
        return [
            {
                "transaction_id": "TXN_1",
                "amount": 999.99,
                "merchant": "Luxury Electronics Store",
                "location": "International",
                "predicted_fraud": True,
                "fraud_probability": 0.93,
                "confidence": "High",
                "risk_level": "HIGH",
                "risk_factors": ["high_amount", "international_location"],
                "pipeline_status": "processed",
                "retry_count": 1,
                "processing_latency_ms": 44,
                "processing_timestamp": datetime(2026, 4, 20, 10, 0, 0),
            }
        ]

    def get_fraud_alerts(self, hours_back=24):
        return [
            {
                "transaction_id": "TXN_1",
                "amount": 999.99,
                "merchant": "Luxury Electronics Store",
                "location": "International",
                "fraud_probability": 0.93,
                "risk_level": "HIGH",
                "risk_factors": ["high_amount", "international_location"],
                "minutes_ago": 5,
            }
        ]

    def get_operational_metrics(self):
        return {
            "processed_last_hour": 11,
            "errors_last_hour": 1,
            "dead_letter_last_hour": 1,
            "duplicates_last_hour": 2,
            "latest_processing_timestamp": datetime.now(),
        }

    def get_recent_pipeline_events(self, limit=8, status=None):
        return [
            {
                "transaction_id": "TXN_1",
                "event_type": "processing_retry",
                "status": status or "success",
                "stage": "processing",
                "details": {"attempt": 1, "reason": "temporary outage"},
                "created_at": datetime(2026, 4, 20, 10, 1, 0),
            }
        ]


def test_dashboard_api_endpoints():
    with TemporaryDirectory() as temp_dir:
        app = create_app(
            db_handler=cast(Any, DashboardStubDB()),
            settings=build_test_settings(Path(temp_dir)),
        )
        client = app.test_client()

        health = client.get("/api/health")
        stats = client.get("/api/stats")
        operations = client.get("/api/operations")

        assert health.status_code == 200
        assert stats.status_code == 200
        assert operations.status_code == 200
        assert stats.get_json()["high_risk_alerts"] == 2
        assert operations.get_json()["metrics"]["processed_last_hour"] == 11


