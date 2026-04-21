from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from psycopg2.extras import Json

from config.settings import AppSettings, get_settings
from src.contracts import PredictionResult, TransactionRecord

TRANSACTIONS_TABLE_DDL = (
    "CREATE TABLE IF NOT EXISTS transactions ("
    " id SERIAL PRIMARY KEY,"
    " transaction_id VARCHAR(50) UNIQUE NOT NULL,"
    " customer_id VARCHAR(20) NOT NULL,"
    " amount DECIMAL(10, 2) NOT NULL,"
    " merchant VARCHAR(100) NOT NULL,"
    " location VARCHAR(100) NOT NULL,"
    " transaction_timestamp TIMESTAMP NOT NULL,"
    " actual_fraud BOOLEAN,"
    " predicted_fraud BOOLEAN,"
    " fraud_probability DECIMAL(5, 4),"
    " confidence VARCHAR(20),"
    " risk_level VARCHAR(20) DEFAULT 'LOW',"
    " risk_factors JSONB DEFAULT '[]',"
    " processing_latency_ms INTEGER DEFAULT 0,"
    " pipeline_status VARCHAR(30) DEFAULT 'processed',"
    " failure_reason TEXT,"
    " retry_count INTEGER DEFAULT 0,"
    " raw_payload JSONB,"
    " model_version VARCHAR(50),"
    " processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
    " created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    ");"
)

PIPELINE_EVENTS_TABLE_DDL = (
    "CREATE TABLE IF NOT EXISTS pipeline_events ("
    " id SERIAL PRIMARY KEY,"
    " transaction_id VARCHAR(50),"
    " event_type VARCHAR(50) NOT NULL,"
    " status VARCHAR(20) NOT NULL,"
    " stage VARCHAR(30),"
    " details JSONB DEFAULT '{}',"
    " raw_payload JSONB,"
    " created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    ");"
)

FRAUD_STATS_VIEW_DDL = (
    "CREATE OR REPLACE VIEW fraud_stats AS "
    "SELECT DATE(processing_timestamp) AS date, "
    "COUNT(*) AS total_transactions, "
    "SUM(CASE WHEN predicted_fraud = true THEN 1 ELSE 0 END) AS fraud_detected, "
    "ROUND(AVG(CASE WHEN predicted_fraud = true THEN 1.0 ELSE 0.0 END) * 100, 2) AS fraud_rate_percent, "
    "ROUND(AVG(amount), 2) AS avg_amount, "
    "SUM(CASE WHEN predicted_fraud = true THEN amount ELSE 0 END) AS total_fraud_amount, "
    "ROUND(AVG(processing_latency_ms), 2) AS avg_latency_ms, "
    "SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END) AS retried_transactions "
    "FROM transactions "
    "GROUP BY DATE(processing_timestamp) "
    "ORDER BY date DESC;"
)

ALTER_AND_INDEX_STATEMENTS = [
    "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS risk_level VARCHAR(20) DEFAULT 'LOW';",
    "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS risk_factors JSONB DEFAULT '[]';",
    "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS processing_latency_ms INTEGER DEFAULT 0;",
    "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS pipeline_status VARCHAR(30) DEFAULT 'processed';",
    "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS failure_reason TEXT;",
    "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;",
    "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS raw_payload JSONB;",
    "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS model_version VARCHAR(50);",
    "CREATE INDEX IF NOT EXISTS idx_transaction_timestamp ON transactions(transaction_timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_processing_timestamp ON transactions(processing_timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_customer_id ON transactions(customer_id);",
    "CREATE INDEX IF NOT EXISTS idx_predicted_fraud ON transactions(predicted_fraud);",
    "CREATE INDEX IF NOT EXISTS idx_amount ON transactions(amount);",
    "CREATE INDEX IF NOT EXISTS idx_transactions_pipeline_status ON transactions(pipeline_status);",
    "CREATE INDEX IF NOT EXISTS idx_transactions_risk_level ON transactions(risk_level);",
    "CREATE INDEX IF NOT EXISTS idx_pipeline_events_status ON pipeline_events(status);",
    "CREATE INDEX IF NOT EXISTS idx_pipeline_events_created_at ON pipeline_events(created_at DESC);",
]

INSERT_PIPELINE_EVENT_SQL = (
    "INSERT INTO pipeline_events (transaction_id, event_type, status, stage, details, raw_payload) "
    "VALUES (%s, %s, %s, %s, %s, %s)"
)

SELECT_TRANSACTION_EXISTS_SQL = "SELECT 1 FROM transactions WHERE transaction_id = %s LIMIT 1"

UPSERT_TRANSACTION_SQL = (
    "INSERT INTO transactions ("
    " transaction_id, customer_id, amount, merchant, location, transaction_timestamp, actual_fraud,"
    " predicted_fraud, fraud_probability, confidence, risk_level, risk_factors, processing_latency_ms,"
    " pipeline_status, failure_reason, retry_count, raw_payload, model_version"
    ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
    "ON CONFLICT (transaction_id) DO UPDATE SET "
    " customer_id = EXCLUDED.customer_id,"
    " amount = EXCLUDED.amount,"
    " merchant = EXCLUDED.merchant,"
    " location = EXCLUDED.location,"
    " transaction_timestamp = EXCLUDED.transaction_timestamp,"
    " actual_fraud = EXCLUDED.actual_fraud,"
    " predicted_fraud = EXCLUDED.predicted_fraud,"
    " fraud_probability = EXCLUDED.fraud_probability,"
    " confidence = EXCLUDED.confidence,"
    " risk_level = EXCLUDED.risk_level,"
    " risk_factors = EXCLUDED.risk_factors,"
    " processing_latency_ms = EXCLUDED.processing_latency_ms,"
    " pipeline_status = EXCLUDED.pipeline_status,"
    " failure_reason = EXCLUDED.failure_reason,"
    " retry_count = EXCLUDED.retry_count,"
    " raw_payload = EXCLUDED.raw_payload,"
    " model_version = EXCLUDED.model_version,"
    " processing_timestamp = CURRENT_TIMESTAMP"
)

FRAUD_STATS_QUERY = (
    "WITH tx AS ("
    " SELECT COUNT(*) AS total_transactions,"
    " COALESCE(SUM(CASE WHEN predicted_fraud = true THEN 1 ELSE 0 END), 0) AS fraud_detected,"
    " COALESCE(ROUND(AVG(CASE WHEN predicted_fraud = true THEN 1.0 ELSE 0.0 END) * 100, 2), 0) AS fraud_rate,"
    " COALESCE(ROUND(AVG(amount), 2), 0) AS avg_amount,"
    " COALESCE(SUM(CASE WHEN predicted_fraud = true THEN amount ELSE 0 END), 0) AS total_fraud_amount,"
    " COALESCE(ROUND(AVG(processing_latency_ms), 2), 0) AS avg_latency_ms,"
    " COALESCE(SUM(CASE WHEN risk_level = 'HIGH' AND predicted_fraud = true THEN 1 ELSE 0 END), 0) AS high_risk_alerts,"
    " COALESCE(SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END), 0) AS retried_transactions"
    " FROM transactions WHERE DATE(processing_timestamp) = CURRENT_DATE"
    "), events AS ("
    " SELECT COALESCE(SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END), 0) AS errors_today,"
    " COALESCE(SUM(CASE WHEN status = 'dead_lettered' THEN 1 ELSE 0 END), 0) AS dead_lettered_today,"
    " COALESCE(SUM(CASE WHEN event_type = 'duplicate_transaction' THEN 1 ELSE 0 END), 0) AS duplicate_events_today"
    " FROM pipeline_events WHERE DATE(created_at) = CURRENT_DATE"
    ") SELECT * FROM tx CROSS JOIN events"
)

RECENT_TRANSACTIONS_QUERY = (
    "SELECT transaction_id, customer_id, amount, merchant, location, transaction_timestamp,"
    " predicted_fraud, fraud_probability, confidence, risk_level, risk_factors,"
    " processing_latency_ms, pipeline_status, retry_count, processing_timestamp"
    " FROM transactions ORDER BY processing_timestamp DESC LIMIT %s"
)

FRAUD_ALERTS_QUERY = (
    "SELECT transaction_id, amount, merchant, location, fraud_probability, risk_level, risk_factors,"
    " EXTRACT(EPOCH FROM (NOW() - processing_timestamp))::INTEGER / 60 AS minutes_ago"
    " FROM transactions WHERE predicted_fraud = true"
    " AND processing_timestamp >= NOW() - (%s || ' hours')::INTERVAL"
    " ORDER BY processing_timestamp DESC"
)

RECENT_PIPELINE_EVENTS_QUERY = (
    "SELECT transaction_id, event_type, status, stage, details, created_at"
    " FROM pipeline_events ORDER BY created_at DESC LIMIT %s"
)

RECENT_PIPELINE_EVENTS_BY_STATUS_QUERY = (
    "SELECT transaction_id, event_type, status, stage, details, created_at"
    " FROM pipeline_events WHERE status = %s ORDER BY created_at DESC LIMIT %s"
)

OPERATIONAL_METRICS_QUERY = (
    "WITH recent_transactions AS ("
    " SELECT COUNT(*) AS processed_last_hour, MAX(processing_timestamp) AS latest_processing_timestamp"
    " FROM transactions WHERE processing_timestamp >= NOW() - INTERVAL '1 hour'"
    "), recent_events AS ("
    " SELECT COALESCE(SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END), 0) AS errors_last_hour,"
    " COALESCE(SUM(CASE WHEN status = 'dead_lettered' THEN 1 ELSE 0 END), 0) AS dead_letter_last_hour,"
    " COALESCE(SUM(CASE WHEN event_type = 'duplicate_transaction' THEN 1 ELSE 0 END), 0) AS duplicates_last_hour"
    " FROM pipeline_events WHERE created_at >= NOW() - INTERVAL '1 hour'"
    ") SELECT * FROM recent_transactions CROSS JOIN recent_events"
)


class DatabaseHandler:
    """Handles PostgreSQL database operations for fraud detection and pipeline observability."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        settings: Optional[AppSettings] = None,
    ):
        self.settings = settings or get_settings()
        self.connection_params = {
            "host": host or self.settings.database.host,
            "port": port or self.settings.database.port,
            "database": database or self.settings.database.name,
            "user": username or self.settings.database.user,
            "password": password or self.settings.database.password,
        }
        self.connection = None
        print("🗄️  Database handler initialized")
        print(
            f"   📋 Connecting to: {self.connection_params['user']}@{self.connection_params['host']}:"
            f"{self.connection_params['port']}/{self.connection_params['database']}"
        )

    def connect(self) -> bool:
        if self.connection and not self.connection.closed:
            return True

        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.connection.autocommit = True
            print("✅ Connected to PostgreSQL database")
            return True
        except Exception as exc:
            print(f"❌ Failed to connect to database: {exc}")
            return False

    def _ensure_connection(self) -> bool:
        return self.connect()

    def ensure_schema(self) -> bool:
        if not self._ensure_connection():
            return False

        try:
            cursor = self.connection.cursor()
            cursor.execute(TRANSACTIONS_TABLE_DDL)
            cursor.execute(PIPELINE_EVENTS_TABLE_DDL)
            for statement in ALTER_AND_INDEX_STATEMENTS:
                cursor.execute(statement)
            cursor.execute(FRAUD_STATS_VIEW_DDL)
            cursor.close()
            return True
        except Exception as exc:
            print(f"❌ Failed to ensure schema: {exc}")
            return False

    def record_pipeline_event(
        self,
        event_type: str,
        status: str,
        transaction_id: Optional[str] = None,
        stage: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        raw_payload: Optional[Any] = None,
    ) -> bool:
        if not self._ensure_connection():
            return False

        try:
            cursor = self.connection.cursor()
            cursor.execute(
                INSERT_PIPELINE_EVENT_SQL,
                (
                    transaction_id,
                    event_type,
                    status,
                    stage,
                    Json(details or {}),
                    Json(raw_payload) if raw_payload is not None else None,
                ),
            )
            cursor.close()
            return True
        except Exception as exc:
            print(f"❌ Pipeline event insert error: {exc}")
            return False

    def save_transaction(
        self,
        transaction: Dict | TransactionRecord,
        prediction: Dict | PredictionResult,
        processing_latency_ms: int = 0,
        retry_count: int = 0,
        pipeline_status: str = "processed",
        raw_payload: Optional[Any] = None,
        failure_reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not self._ensure_connection():
            return {"success": False, "status": "db_unavailable"}

        tx = transaction if isinstance(transaction, TransactionRecord) else TransactionRecord.from_dict(transaction)
        pred = prediction if isinstance(prediction, PredictionResult) else PredictionResult.from_dict(prediction)

        try:
            cursor = self.connection.cursor()
            cursor.execute(SELECT_TRANSACTION_EXISTS_SQL, (tx.transaction_id,))
            exists = cursor.fetchone() is not None
            cursor.execute(
                UPSERT_TRANSACTION_SQL,
                (
                    tx.transaction_id,
                    tx.customer_id,
                    tx.amount,
                    tx.merchant,
                    tx.location,
                    tx.event_time,
                    tx.is_fraud,
                    pred.predicted_fraud,
                    pred.fraud_probability,
                    pred.confidence,
                    pred.risk_level,
                    Json(pred.risk_factors),
                    int(processing_latency_ms),
                    pipeline_status,
                    failure_reason,
                    int(retry_count),
                    Json(raw_payload if raw_payload is not None else tx.to_dict()),
                    pred.model_version,
                ),
            )
            cursor.close()

            save_status = "updated" if exists else "inserted"
            self.record_pipeline_event(
                event_type="duplicate_transaction" if exists else "transaction_processed",
                status="success",
                transaction_id=tx.transaction_id,
                stage="database",
                details={
                    "save_status": save_status,
                    "predicted_fraud": pred.predicted_fraud,
                    "risk_level": pred.risk_level,
                    "processing_latency_ms": int(processing_latency_ms),
                    "retry_count": int(retry_count),
                },
                raw_payload=raw_payload if raw_payload is not None else tx.to_dict(),
            )

            if pred.predicted_fraud and pred.risk_level == "HIGH":
                self.record_pipeline_event(
                    event_type="high_risk_alert",
                    status="success",
                    transaction_id=tx.transaction_id,
                    stage="detection",
                    details={
                        "fraud_probability": pred.fraud_probability,
                        "risk_factors": pred.risk_factors,
                    },
                )

            print(f"💾 Saved to database: {tx.transaction_id} ({save_status})")
            return {"success": True, "status": save_status, "transaction_id": tx.transaction_id}
        except Exception as exc:
            print(f"❌ Database save error: {exc}")
            self.record_pipeline_event(
                event_type="database_write_failed",
                status="error",
                transaction_id=tx.transaction_id,
                stage="database",
                details={"reason": str(exc)},
                raw_payload=raw_payload if raw_payload is not None else tx.to_dict(),
            )
            return {"success": False, "status": "db_error", "error": str(exc)}

    def seed_sample_data(self) -> bool:
        if not self._ensure_connection():
            return False

        sample_transactions = [
            {
                "transaction_id": "SAMPLE_001",
                "customer_id": "CUST_1001",
                "amount": 1500.0,
                "merchant": "Luxury Store",
                "location": "International",
                "timestamp": datetime.now().isoformat(),
                "is_fraud": True,
            },
            {
                "transaction_id": "SAMPLE_002",
                "customer_id": "CUST_1002",
                "amount": 45.99,
                "merchant": "Amazon",
                "location": "New York, NY",
                "timestamp": datetime.now().isoformat(),
                "is_fraud": False,
            },
            {
                "transaction_id": "SAMPLE_003",
                "customer_id": "CUST_1003",
                "amount": 875.5,
                "merchant": "Electronics Store",
                "location": "Los Angeles, CA",
                "timestamp": datetime.now().isoformat(),
                "is_fraud": True,
            },
        ]
        sample_predictions = [
            {
                "transaction_id": "SAMPLE_001",
                "predicted_fraud": True,
                "fraud_probability": 0.95,
                "confidence": "High",
                "risk_level": "HIGH",
                "risk_factors": ["high_amount", "international_location", "luxury_merchant"],
                "model_version": self.settings.model.version,
            },
            {
                "transaction_id": "SAMPLE_002",
                "predicted_fraud": False,
                "fraud_probability": 0.05,
                "confidence": "High",
                "risk_level": "LOW",
                "risk_factors": ["routine_behavior"],
                "model_version": self.settings.model.version,
            },
            {
                "transaction_id": "SAMPLE_003",
                "predicted_fraud": True,
                "fraud_probability": 0.88,
                "confidence": "High",
                "risk_level": "HIGH",
                "risk_factors": ["elevated_amount"],
                "model_version": self.settings.model.version,
            },
        ]

        for transaction_payload, prediction_payload in zip(sample_transactions, sample_predictions):
            self.save_transaction(
                transaction_payload,
                prediction_payload,
                processing_latency_ms=35,
                retry_count=0,
                raw_payload=transaction_payload,
            )
        return True

    def get_fraud_stats(self) -> Optional[Dict[str, Any]]:
        if not self._ensure_connection():
            return None

        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(FRAUD_STATS_QUERY)
            result = cursor.fetchone()
            cursor.close()
            return dict(result) if result else None
        except Exception as exc:
            print(f"❌ Database query error: {exc}")
            return None

    def get_recent_transactions(self, limit: int = 10) -> List[Dict[str, Any]]:
        if not self._ensure_connection():
            return []

        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(RECENT_TRANSACTIONS_QUERY, (limit,))
            results = cursor.fetchall()
            cursor.close()
            return [dict(row) for row in results]
        except Exception as exc:
            print(f"❌ Database query error: {exc}")
            return []

    def get_fraud_alerts(self, hours_back: int = 1) -> List[Dict[str, Any]]:
        if not self._ensure_connection():
            return []

        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(FRAUD_ALERTS_QUERY, (hours_back,))
            results = cursor.fetchall()
            cursor.close()
            return [dict(row) for row in results]
        except Exception as exc:
            print(f"❌ Database query error: {exc}")
            return []

    def get_recent_pipeline_events(self, limit: int = 10, status: Optional[str] = None) -> List[Dict[str, Any]]:
        if not self._ensure_connection():
            return []

        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            if status:
                cursor.execute(RECENT_PIPELINE_EVENTS_BY_STATUS_QUERY, (status, limit))
            else:
                cursor.execute(RECENT_PIPELINE_EVENTS_QUERY, (limit,))
            results = cursor.fetchall()
            cursor.close()
            return [dict(row) for row in results]
        except Exception as exc:
            print(f"❌ Pipeline events query error: {exc}")
            return []

    def get_operational_metrics(self) -> Dict[str, Any]:
        if not self._ensure_connection():
            return {
                "processed_last_hour": 0,
                "errors_last_hour": 0,
                "dead_letter_last_hour": 0,
                "duplicates_last_hour": 0,
                "latest_processing_timestamp": None,
            }

        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(OPERATIONAL_METRICS_QUERY)
            result = cursor.fetchone()
            cursor.close()
            return dict(result) if result else {}
        except Exception as exc:
            print(f"❌ Operational metrics query error: {exc}")
            return {
                "processed_last_hour": 0,
                "errors_last_hour": 0,
                "dead_letter_last_hour": 0,
                "duplicates_last_hour": 0,
                "latest_processing_timestamp": None,
            }

    def close(self):
        if self.connection and not self.connection.closed:
            self.connection.close()
            print("👋 Database connection closed")
        self.connection = None


# Test the database handler
if __name__ == "__main__":
    print("🚀 Testing Enhanced Database Handler\n")

    db = DatabaseHandler()
    if db.connect() and db.ensure_schema():
        db.seed_sample_data()

        print("📊 Getting fraud statistics...")
        stats = db.get_fraud_stats()
        if stats:
            print(f"   Total transactions today: {stats['total_transactions']}")
            print(f"   Fraud detected: {stats['fraud_detected']}")
            print(f"   Fraud rate: {stats['fraud_rate']}%")
            print(f"   Average latency: {stats['avg_latency_ms']} ms")
            print(f"   Dead-lettered today: {stats['dead_lettered_today']}")

        print("\n📋 Recent transactions:")
        for transaction in db.get_recent_transactions(limit=5):
            fraud_status = "🚨 FRAUD" if transaction['predicted_fraud'] else "✅ Normal"
            print(f"   {fraud_status} ${transaction['amount']} at {transaction['merchant']} [{transaction['risk_level']}]")

        print("\n⚙️ Recent pipeline events:")
        for event in db.get_recent_pipeline_events(limit=5):
            print(f"   {event['event_type']} -> {event['status']}")

        db.close()
        print("\n✅ Enhanced database handler test complete!")
    else:
        print("❌ Could not connect to database")
        print("💡 Check your .env file and make sure PostgreSQL is running")
