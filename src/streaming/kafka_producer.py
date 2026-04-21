# src/streaming/kafka_producer.py

from __future__ import annotations

import json
import time

from kafka import KafkaProducer

from config.settings import AppSettings, get_settings
from src.contracts import TransactionRecord
from src.data_generator.transaction_simulator import TransactionSimulator


class SimpleTransactionProducer:
    """Sends validated transactions to Kafka."""

    def __init__(self, settings: AppSettings | None = None):
        self.settings = settings or get_settings()
        print("🚀 Connecting to Kafka...")

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.settings.kafka.bootstrap_servers,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
            print("✅ Connected to Kafka!")
        except Exception as exc:
            print(f"❌ Failed to connect to Kafka: {exc}")
            print("💡 Make sure Docker containers are running!")
            raise

        self.simulator = TransactionSimulator()
        self.topic = self.settings.kafka.transactions_topic

    def send_transaction(self, transaction):
        """Send a single validated transaction to Kafka."""
        try:
            transaction_payload = TransactionRecord.from_dict(transaction).to_dict()
            future = self.producer.send(self.topic, value=transaction_payload)
            future.get(timeout=10)
            print(
                f"📤 Sent: {transaction_payload['transaction_id']} (${transaction_payload['amount']}) - "
                f"{'FRAUD' if transaction_payload.get('is_fraud') else 'NORMAL'}"
            )
            return True
        except Exception as exc:
            print(f"❌ Failed to send transaction: {exc}")
            return False

    def stream_transactions(self, count: int = 10, delay_seconds: int = 2):
        """Send multiple transactions with delays."""
        print(f"\n🔄 Streaming {count} transactions (one every {delay_seconds} seconds)...")
        sent_count = 0

        for index in range(count):
            transaction = (
                self.simulator.generate_fraud_transaction()
                if index % 10 == 0
                else self.simulator.generate_normal_transaction()
            )

            if self.send_transaction(transaction):
                sent_count += 1

            print(f"   Progress: {index + 1}/{count}")
            if index < count - 1:
                time.sleep(delay_seconds)

        print(f"\n✅ Streaming complete! Sent {sent_count}/{count} transactions")
        self.producer.flush()

    def close(self):
        self.producer.close()
        print("👋 Producer closed")


if __name__ == "__main__":
    print("🚀 Testing Kafka Producer\n")

    try:
        producer = SimpleTransactionProducer()
        print("📤 Sending test transactions...")
        producer.stream_transactions(count=5, delay_seconds=1)
        producer.close()
        print("\n✅ Kafka producer is working with shared config and schema validation.")
    except Exception as exc:
        print(f"❌ Error: {exc}")
        print("\n💡 Troubleshooting:")
        print("1. Make sure Docker is running")
        print("2. Run: docker-compose -f docker-compose-with-db.yml ps")
        print("3. Check if both kafka and zookeeper are Up")