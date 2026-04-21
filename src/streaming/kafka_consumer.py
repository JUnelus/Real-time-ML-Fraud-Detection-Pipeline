# src/streaming/kafka_consumer.py

from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

from kafka import KafkaConsumer, KafkaProducer

from config.settings import AppSettings, get_settings
from src.contracts import TransactionRecord, TransactionValidationError
from src.database.db_handler import DatabaseHandler
from src.ml_model.fraud_detector import FraudDetector


class TransactionConsumer:
    """Consumes transactions from Kafka, scores them, and records operational telemetry."""

    def __init__(
        self,
        consumer: Optional[KafkaConsumer] = None,
        detector: Optional[FraudDetector] = None,
        db: Optional[DatabaseHandler] = None,
        dlq_producer: Optional[KafkaProducer] = None,
        settings: Optional[AppSettings] = None,
    ):
        self.settings = settings or get_settings()
        self.consumer = consumer
        self.detector = detector or FraudDetector()
        self.db = db or DatabaseHandler(settings=self.settings)
        self.dlq_producer = dlq_producer

        self.processed_count = 0
        self.fraud_detected = 0
        self.database_saves = 0
        self.error_count = 0
        self.dlq_count = 0
        self.retry_events = 0
        self.duplicate_updates = 0
        self.total_latency_ms = 0

        print("🚀 Starting Transaction Consumer with Reliability + Observability upgrades...")

        if self.consumer is None:
            self.consumer = self._create_consumer()

        if self.dlq_producer is None:
            self.dlq_producer = self._create_dlq_producer()

        model_loaded = self.detector.load_model()
        if not model_loaded and self.settings.model.auto_train_if_missing:
            print("⚠️  No trained model found. Training a new one...")
            self._train_new_model()
        elif not model_loaded:
            raise RuntimeError("Model not found and auto-train is disabled.")

        if not self.db.connect():
            raise RuntimeError("Database connection failed")
        self.db.ensure_schema()
        print("✅ Connected to database and schema verified")

    def _create_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self.settings.kafka.transactions_topic,
            bootstrap_servers=self.settings.kafka.bootstrap_servers,
            value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
            auto_offset_reset="latest",
            group_id=self.settings.kafka.consumer_group,
        )

    def _create_dlq_producer(self) -> Optional[KafkaProducer]:
        try:
            return KafkaProducer(
                bootstrap_servers=self.settings.kafka.bootstrap_servers,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
        except Exception as exc:
            print(f"⚠️  DLQ producer unavailable: {exc}")
            return None

    def _train_new_model(self):
        from src.data_generator.transaction_simulator import TransactionSimulator

        simulator = TransactionSimulator()
        training_data = simulator.generate_batch(
            size=self.settings.model.training_batch_size,
            fraud_rate=self.settings.model.training_fraud_rate,
        )
        self.detector.train(training_data)
        self.detector.save_model()
        print("✅ New model trained and saved!")

    @staticmethod
    def _to_serializable_payload(payload: Any) -> Any:
        if isinstance(payload, (dict, list, str, int, float, bool)) or payload is None:
            return payload
        return {"raw_payload": str(payload)}

    def _send_to_dead_letter(self, payload: Any, reason: str, stage: str, transaction_id: Optional[str] = None):
        dlq_message = {
            "transaction_id": transaction_id,
            "reason": reason,
            "stage": stage,
            "payload": self._to_serializable_payload(payload),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }

        if self.dlq_producer is not None:
            try:
                self.dlq_producer.send(self.settings.kafka.dead_letter_topic, value=dlq_message).get(timeout=10)
            except Exception as exc:
                print(f"⚠️  Failed to publish DLQ message: {exc}")

        self.db.record_pipeline_event(
            event_type="dead_lettered",
            status="dead_lettered",
            transaction_id=transaction_id,
            stage=stage,
            details={"reason": reason},
            raw_payload=dlq_message,
        )
        self.dlq_count += 1

    def process_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Validate, score, persist, and audit a single transaction payload."""

        raw_payload = self._to_serializable_payload(payload)

        try:
            transaction = TransactionRecord.from_dict(payload)
        except TransactionValidationError as exc:
            self.error_count += 1
            self.db.record_pipeline_event(
                event_type="validation_failed",
                status="error",
                transaction_id=payload.get("transaction_id") if isinstance(payload, dict) else None,
                stage="validation",
                details={"reason": str(exc)},
                raw_payload=raw_payload,
            )
            self._send_to_dead_letter(payload, str(exc), stage="validation")
            return {"success": False, "status": "validation_failed", "error": str(exc)}

        last_error = None
        for attempt in range(self.settings.pipeline.max_retries + 1):
            start_time = time.perf_counter()
            try:
                prediction = self.detector.predict_single(transaction.to_dict())
                processing_latency_ms = int((time.perf_counter() - start_time) * 1000)
                prediction["processing_latency_ms"] = processing_latency_ms

                db_result = self.db.save_transaction(
                    transaction,
                    prediction,
                    processing_latency_ms=processing_latency_ms,
                    retry_count=attempt,
                    pipeline_status="processed",
                    raw_payload=raw_payload,
                )

                if not db_result.get("success"):
                    raise RuntimeError(db_result.get("error", db_result.get("status", "Database write failed")))

                self.processed_count += 1
                self.database_saves += 1
                self.total_latency_ms += processing_latency_ms
                if prediction["predicted_fraud"]:
                    self.fraud_detected += 1
                if db_result.get("status") == "updated":
                    self.duplicate_updates += 1

                fraud_indicator = "🚨 FRAUD ALERT" if prediction["predicted_fraud"] else "✅ Normal"
                print(f"\n{fraud_indicator} | Risk={prediction['risk_level']} | Saved={db_result['status']}")
                print(f"   Transaction: {transaction.transaction_id}")
                print(f"   Amount: ${transaction.amount}")
                print(f"   Merchant: {transaction.merchant}")
                print(f"   Location: {transaction.location}")
                print(f"   Probability: {prediction['fraud_probability']}")
                print(f"   Risk factors: {', '.join(prediction['risk_factors'])}")
                print(f"   Latency: {processing_latency_ms} ms")

                if prediction["predicted_fraud"] and prediction["fraud_probability"] >= self.settings.pipeline.high_risk_threshold:
                    print("   🔥 HIGH RISK - Immediate review required!")

                return {
                    "success": True,
                    "status": db_result["status"],
                    "prediction": prediction,
                    "transaction_id": transaction.transaction_id,
                }
            except Exception as exc:
                last_error = str(exc)
                self.error_count += 1
                if attempt < self.settings.pipeline.max_retries:
                    self.retry_events += 1
                    self.db.record_pipeline_event(
                        event_type="processing_retry",
                        status="error",
                        transaction_id=transaction.transaction_id,
                        stage="processing",
                        details={"attempt": attempt + 1, "reason": str(exc)},
                        raw_payload=raw_payload,
                    )
                    continue

        self.db.record_pipeline_event(
            event_type="processing_failed",
            status="error",
            transaction_id=transaction.transaction_id,
            stage="processing",
            details={"reason": last_error or "Unknown processing failure"},
            raw_payload=raw_payload,
        )
        self._send_to_dead_letter(payload, last_error or "Unknown processing failure", stage="processing", transaction_id=transaction.transaction_id)
        return {
            "success": False,
            "status": "processing_failed",
            "transaction_id": transaction.transaction_id,
            "error": last_error,
        }

    def start_consuming(self, max_messages: Optional[int] = None):
        print("\n👂 Listening for transactions...")
        print(f"📊 Will process {'unlimited' if max_messages is None else max_messages} messages")
        print(f"💾 Saving results to PostgreSQL database")
        print(f"🧯 Dead-letter topic: {self.settings.kafka.dead_letter_topic}")
        print("⏹️  Press Ctrl+C to stop\n")

        try:
            for message in self.consumer:
                self.process_payload(message.value)

                avg_latency = self.total_latency_ms / self.processed_count if self.processed_count else 0
                fraud_rate = (self.fraud_detected / self.processed_count) * 100 if self.processed_count else 0
                save_rate = (self.database_saves / max(self.processed_count, 1)) * 100 if self.processed_count else 0

                print(
                    f"   📈 Stats: {self.processed_count} processed | {self.fraud_detected} fraud ({fraud_rate:.1f}%)"
                    f" | {self.database_saves} saved ({save_rate:.1f}%) | {self.dlq_count} DLQ | avg latency {avg_latency:.1f} ms"
                )
                print(f"   {'=' * 70}")

                if max_messages and (self.processed_count + self.dlq_count) >= max_messages:
                    print(f"\n🛑 Reached limit of {max_messages} messages")
                    break
        except KeyboardInterrupt:
            print("\n⏹️  Consumer stopped by user")
        except Exception as exc:
            print(f"\n❌ Error during consumption: {exc}")
        finally:
            self.close()

    def show_database_stats(self):
        try:
            stats = self.db.get_fraud_stats()
            if stats:
                print("\n📊 Database Statistics (Today):")
                print(f"   Total transactions: {stats['total_transactions']}")
                print(f"   Fraud detected: {stats['fraud_detected']}")
                print(f"   Fraud rate: {stats['fraud_rate']}%")
                print(f"   Average amount: ${stats['avg_amount']}")
                print(f"   Average latency: {stats['avg_latency_ms']} ms")
                print(f"   Dead-lettered: {stats['dead_lettered_today']}")
                print(f"   Duplicate updates: {stats['duplicate_events_today']}")
        except Exception as exc:
            print(f"   ⚠️  Could not get database stats: {exc}")

    def close(self):
        if self.consumer is not None:
            self.consumer.close()
        if self.dlq_producer is not None:
            self.dlq_producer.flush()
            self.dlq_producer.close()
        self.db.close()

        print("\n📊 Final Statistics:")
        print(f"   Total processed: {self.processed_count}")
        print(f"   Fraud detected: {self.fraud_detected}")
        print(f"   Database saves: {self.database_saves}")
        print(f"   Processing errors: {self.error_count}")
        print(f"   Dead-lettered: {self.dlq_count}")
        print(f"   Retry events: {self.retry_events}")
        print(f"   Duplicate updates: {self.duplicate_updates}")
        if self.processed_count:
            print(f"   Average latency: {self.total_latency_ms / self.processed_count:.1f} ms")

        self.show_database_stats()
        print("👋 Consumer closed")


if __name__ == "__main__":
    print("🚀 Testing Enhanced Kafka Consumer with Reliability + Observability\n")

    try:
        consumer = TransactionConsumer()
        consumer.show_database_stats()
        consumer.start_consuming(max_messages=10)
        print("\n✅ Real-time fraud detection pipeline is running with retries, DLQ, and observability.")
    except Exception as exc:
        print(f"❌ Error: {exc}")
        print("\n💡 Troubleshooting:")
        print("1. Make sure Kafka containers are running")
        print("2. Make sure PostgreSQL is running and tables exist")
        print("3. Try running the producer first to send some messages")