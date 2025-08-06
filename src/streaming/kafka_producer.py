# src/streaming/kafka_producer.py

import json
import time
from kafka import KafkaProducer
from src.data_generator.transaction_simulator import TransactionSimulator


class SimpleTransactionProducer:
    """Sends transactions to Kafka stream"""

    def __init__(self):
        print("🚀 Connecting to Kafka...")

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("✅ Connected to Kafka!")

        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            print("💡 Make sure Docker containers are running!")
            raise

        self.simulator = TransactionSimulator()
        self.topic = 'transactions'

    def send_transaction(self, transaction):
        """Send a single transaction to Kafka"""
        try:
            # Send to Kafka topic
            future = self.producer.send(self.topic, value=transaction)

            # Wait for confirmation (optional, but good for testing)
            record_metadata = future.get(timeout=10)

            print(
                f"📤 Sent: {transaction['transaction_id']} (${transaction['amount']}) - {'FRAUD' if transaction['is_fraud'] else 'NORMAL'}")
            return True

        except Exception as e:
            print(f"❌ Failed to send transaction: {e}")
            return False

    def stream_transactions(self, count=10, delay_seconds=2):
        """Send multiple transactions with delays"""
        print(f"\n🔄 Streaming {count} transactions (one every {delay_seconds} seconds)...")

        sent_count = 0

        for i in range(count):
            # Generate transaction (mix of normal and fraud)
            if i % 10 == 0:  # Every 10th transaction is fraud
                transaction = self.simulator.generate_fraud_transaction()
            else:
                transaction = self.simulator.generate_normal_transaction()

            # Send to Kafka
            if self.send_transaction(transaction):
                sent_count += 1

            print(f"   Progress: {i + 1}/{count}")

            # Wait before sending next transaction
            if i < count - 1:  # Don't wait after the last one
                time.sleep(delay_seconds)

        print(f"\n✅ Streaming complete! Sent {sent_count}/{count} transactions")

        # Make sure all messages are sent
        self.producer.flush()

    def close(self):
        """Close the producer"""
        self.producer.close()
        print("👋 Producer closed")


# Test the producer
if __name__ == "__main__":
    print("🚀 Testing Kafka Producer\n")

    try:
        # Create producer
        producer = SimpleTransactionProducer()

        # Send a few test transactions
        print("📤 Sending test transactions...")
        producer.stream_transactions(count=5, delay_seconds=1)

        # Close producer
        producer.close()

        print("\n✅ Step 4B Complete! Kafka producer is working.")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n💡 Troubleshooting:")
        print("1. Make sure Docker is running")
        print("2. Run: docker-compose -f docker-compose-with-db.yml ps")
        print("3. Check if both kafka and zookeeper are Up")