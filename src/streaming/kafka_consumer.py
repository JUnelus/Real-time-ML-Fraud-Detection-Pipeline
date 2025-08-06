# src/streaming/kafka_consumer.py
# Updated consumer that saves results to database

import json
from kafka import KafkaConsumer
from src.ml_model.fraud_detector import FraudDetector
from src.database.db_handler import DatabaseHandler


class TransactionConsumer:
    """Consumes transactions from Kafka and processes them with ML + Database"""

    def __init__(self):
        print("🚀 Starting Transaction Consumer with Database Storage...")

        # Connect to Kafka
        try:
            self.consumer = KafkaConsumer(
                'transactions',
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='fraud-detection-group'
            )
            print("✅ Connected to Kafka consumer!")

        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            raise

        # Load ML model
        self.detector = FraudDetector()
        model_loaded = self.detector.load_model()

        if not model_loaded:
            print("⚠️  No trained model found. Training a new one...")
            self._train_new_model()

        # Connect to database
        self.db = DatabaseHandler(
            username='postgres',
            password='postgres'
        )

        if not self.db.connect():
            print("❌ Failed to connect to database!")
            raise Exception("Database connection failed")

        print("✅ Connected to database!")

        # Statistics
        self.processed_count = 0
        self.fraud_detected = 0
        self.database_saves = 0

    def _train_new_model(self):
        """Train a new model if none exists"""
        from src.data_generator.transaction_simulator import TransactionSimulator

        simulator = TransactionSimulator()
        training_data = simulator.generate_batch(size=1000, fraud_rate=0.05)

        self.detector.train(training_data)
        self.detector.save_model()
        print("✅ New model trained and saved!")

    def process_transaction(self, transaction):
        """Process a single transaction through fraud detection + database"""

        # Get ML prediction
        prediction = self.detector.predict_single(transaction)

        # Save to database
        try:
            db_success = self.db.save_transaction(transaction, prediction)
            if db_success:
                self.database_saves += 1
                db_status = "💾 Saved"
            else:
                db_status = "❌ DB Error"
        except Exception as e:
            print(f"   ⚠️  Database save failed: {e}")
            db_status = "❌ DB Error"

        # Track statistics
        self.processed_count += 1
        if prediction['predicted_fraud']:
            self.fraud_detected += 1

        # Show results
        fraud_indicator = "🚨 FRAUD ALERT" if prediction['predicted_fraud'] else "✅ Normal"
        confidence_indicator = "🎯" if prediction['confidence'] == 'High' else "⚠️"

        print(f"\n{fraud_indicator} {confidence_indicator} {db_status}")
        print(f"   Transaction: {transaction['transaction_id']}")
        print(f"   Amount: ${transaction['amount']}")
        print(f"   Merchant: {transaction['merchant']}")
        print(f"   Location: {transaction['location']}")
        print(f"   Fraud Probability: {prediction['fraud_probability']}")
        print(f"   Confidence: {prediction['confidence']}")

        # Special handling for high-risk fraud
        if prediction['predicted_fraud'] and prediction['fraud_probability'] > 0.8:
            print(f"   🔥 HIGH RISK - Immediate review required!")

        return prediction

    def start_consuming(self, max_messages=None):
        """Start consuming messages from Kafka"""

        print(f"\n👂 Listening for transactions...")
        print(f"📊 Will process {'unlimited' if max_messages is None else max_messages} messages")
        print(f"💾 Saving results to PostgreSQL database")
        print(f"⏹️  Press Ctrl+C to stop\n")

        try:
            for message in self.consumer:
                transaction = message.value

                # Process the transaction
                prediction = self.process_transaction(transaction)

                # Show running statistics
                fraud_rate = (self.fraud_detected / self.processed_count) * 100 if self.processed_count > 0 else 0
                db_success_rate = (self.database_saves / self.processed_count) * 100 if self.processed_count > 0 else 0

                print(
                    f"   📈 Stats: {self.processed_count} processed, {self.fraud_detected} fraud ({fraud_rate:.1f}%), {self.database_saves} saved ({db_success_rate:.1f}%)")

                # Stop if we've reached the limit
                if max_messages and self.processed_count >= max_messages:
                    print(f"\n🛑 Reached limit of {max_messages} messages")
                    break

                print(f"   {'=' * 60}")

        except KeyboardInterrupt:
            print(f"\n⏹️  Consumer stopped by user")

        except Exception as e:
            print(f"\n❌ Error during consumption: {e}")

        finally:
            self.close()

    def show_database_stats(self):
        """Show current database statistics"""
        try:
            stats = self.db.get_fraud_stats()
            if stats:
                print(f"\n📊 Database Statistics (Today):")
                print(f"   Total transactions: {stats['total_transactions']}")
                print(f"   Fraud detected: {stats['fraud_detected']}")
                print(f"   Fraud rate: {stats['fraud_rate']}%")
                print(f"   Average amount: ${stats['avg_amount']}")
                if stats['total_fraud_amount']:
                    print(f"   Total fraud amount: ${stats['total_fraud_amount']}")
        except Exception as e:
            print(f"   ⚠️  Could not get database stats: {e}")

    def close(self):
        """Close connections"""
        self.consumer.close()
        self.db.close()

        print(f"\n📊 Final Statistics:")
        print(f"   Total processed: {self.processed_count}")
        print(f"   Fraud detected: {self.fraud_detected}")
        print(f"   Database saves: {self.database_saves}")

        if self.processed_count > 0:
            fraud_rate = (self.fraud_detected / self.processed_count) * 100
            save_rate = (self.database_saves / self.processed_count) * 100
            print(f"   Fraud rate: {fraud_rate:.1f}%")
            print(f"   Database save rate: {save_rate:.1f}%")

        # Show final database stats
        self.show_database_stats()

        print("👋 Consumer closed")


# Test the enhanced consumer
if __name__ == "__main__":
    print("🚀 Testing Enhanced Kafka Consumer with Database\n")

    try:
        # Create consumer
        consumer = TransactionConsumer()

        # Show current database state
        consumer.show_database_stats()

        # Start consuming (limit to 10 messages for testing)
        consumer.start_consuming(max_messages=10)

        print("\n✅ Step 5B Complete! Real-time fraud detection with database storage is working.")

    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n💡 Troubleshooting:")
        print("1. Make sure Kafka containers are running")
        print("2. Make sure PostgreSQL is running and tables exist")
        print("3. Try running the producer first to send some messages")