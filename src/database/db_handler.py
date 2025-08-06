import os
import psycopg2
import psycopg2.extras
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class DatabaseHandler:
    """Handles PostgreSQL database operations for fraud detection"""

    def __init__(self,
                 host: Optional[str] = None,
                 port: Optional[int] = None,
                 database: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None):

        # Use environment variables as defaults, with fallback to parameters
        self.connection_params = {
            'host': host or os.getenv('DATABASE_HOST', 'localhost'),
            'port': port or int(os.getenv('DATABASE_PORT', 5432)),
            'database': database or os.getenv('DATABASE_NAME', 'fraud_detection'),
            'user': username or os.getenv('DATABASE_USER', 'postgres'),
            'password': password or os.getenv('DATABASE_PASSWORD', '')
        }

        self.connection = None
        print("🗄️  Database handler initialized")
        print(
            f"   📋 Connecting to: {self.connection_params['user']}@{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}")

    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.connection.autocommit = True
            print("✅ Connected to PostgreSQL database")

            # Test the connection with a simple query
            cursor = self.connection.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            print(f"   📋 PostgreSQL: {version[:50]}...")
            cursor.close()

            return True

        except Exception as e:
            print(f"❌ Failed to connect to database: {e}")
            print(
                f"   Connection params: {self.connection_params['user']}@{self.connection_params['host']}:{self.connection_params['port']}")
            return False

    def save_transaction(self, transaction: Dict, prediction: Dict) -> bool:
        """Save transaction and fraud prediction to database"""

        if not self.connection:
            if not self.connect():
                return False

        try:
            cursor = self.connection.cursor()

            # Parse timestamp
            transaction_time = datetime.fromisoformat(transaction['timestamp'])

            # Insert transaction with prediction
            insert_query = """
                           INSERT INTO transactions (transaction_id, customer_id, amount, merchant, location, \
                                                     transaction_timestamp, actual_fraud, predicted_fraud, \
                                                     fraud_probability, confidence) \
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (transaction_id) DO UPDATE SET predicted_fraud      = EXCLUDED.predicted_fraud,
                                                                      fraud_probability    = EXCLUDED.fraud_probability,
                                                                      confidence           = EXCLUDED.confidence,
                                                                      processing_timestamp = CURRENT_TIMESTAMP \
                           """

            cursor.execute(insert_query, (
                transaction['transaction_id'],
                transaction['customer_id'],
                float(transaction['amount']),
                transaction['merchant'],
                transaction['location'],
                transaction_time,
                transaction.get('is_fraud', None),  # Actual fraud (if known)
                prediction['predicted_fraud'],
                float(prediction['fraud_probability']),
                prediction['confidence']
            ))

            cursor.close()
            print(f"💾 Saved to database: {transaction['transaction_id']}")
            return True

        except Exception as e:
            print(f"❌ Database save error: {e}")
            return False

    def get_fraud_stats(self) -> Optional[Dict]:
        """Get fraud detection statistics"""

        if not self.connection:
            if not self.connect():
                return None

        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Get today's stats
            cursor.execute("""
                           SELECT COUNT(*)                                                                    as total_transactions,
                                  SUM(CASE WHEN predicted_fraud = true THEN 1 ELSE 0 END)                     as fraud_detected,
                                  ROUND(AVG(CASE WHEN predicted_fraud = true THEN 1.0 ELSE 0.0 END) * 100,
                                        2)                                                                    as fraud_rate,
                                  ROUND(AVG(amount), 2)                                                       as avg_amount,
                                  SUM(CASE WHEN predicted_fraud = true THEN amount ELSE 0 END)                as total_fraud_amount
                           FROM transactions
                           WHERE DATE(processing_timestamp) = CURRENT_DATE
                           """)

            result = cursor.fetchone()
            cursor.close()

            return dict(result) if result else None

        except Exception as e:
            print(f"❌ Database query error: {e}")
            return None

    def get_recent_transactions(self, limit: int = 10) -> List[Dict]:
        """Get recent transactions"""

        if not self.connection:
            if not self.connect():
                return []

        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

            cursor.execute("""
                           SELECT transaction_id,
                                  customer_id,
                                  amount,
                                  merchant,
                                  location,
                                  transaction_timestamp,
                                  predicted_fraud,
                                  fraud_probability,
                                  confidence,
                                  processing_timestamp
                           FROM transactions
                           ORDER BY processing_timestamp DESC
                           LIMIT %s
                           """, (limit,))

            results = cursor.fetchall()
            cursor.close()

            return [dict(row) for row in results]

        except Exception as e:
            print(f"❌ Database query error: {e}")
            return []

    def get_fraud_alerts(self, hours_back: int = 1) -> List[Dict]:
        """Get recent fraud alerts"""

        if not self.connection:
            if not self.connect():
                return []

        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

            cursor.execute("""
                           SELECT transaction_id,
                                  amount,
                                  merchant,
                                  location,
                                  fraud_probability,
                                  EXTRACT(EPOCH FROM (NOW() - processing_timestamp)) ::INTEGER / 60 as minutes_ago
                           FROM transactions
                           WHERE predicted_fraud = true
                             AND processing_timestamp >= NOW() - (%s || ' hours')::INTERVAL
                           ORDER BY processing_timestamp DESC
                           """, (hours_back,))

            results = cursor.fetchall()
            cursor.close()

            return [dict(row) for row in results]

        except Exception as e:
            print(f"❌ Database query error: {e}")
            return []

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("👋 Database connection closed")


# Test the database handler
if __name__ == "__main__":
    print("🚀 Testing Database Handler with Environment Variables\n")

    # Show loaded environment variables (without exposing password)
    print("🔧 Environment Configuration:")
    print(f"   DATABASE_HOST: {os.getenv('DATABASE_HOST', 'Not set')}")
    print(f"   DATABASE_PORT: {os.getenv('DATABASE_PORT', 'Not set')}")
    print(f"   DATABASE_NAME: {os.getenv('DATABASE_NAME', 'Not set')}")
    print(f"   DATABASE_USER: {os.getenv('DATABASE_USER', 'Not set')}")
    print(f"   DATABASE_PASSWORD: {'***' if os.getenv('DATABASE_PASSWORD') else 'Not set'}")
    print()

    # Create handler and connect (using environment variables)
    db = DatabaseHandler()

    if db.connect():
        # Check if we can see the sample data
        print("📋 Checking existing data...")
        recent = db.get_recent_transactions(limit=5)
        print(f"   Found {len(recent)} existing transactions")

        # Test saving a transaction
        test_transaction = {
            'transaction_id': f'TEST_{int(datetime.now().timestamp())}',
            'customer_id': 'CUST_TEST',
            'amount': 299.99,
            'merchant': 'Test Store',
            'location': 'Test City, TS',
            'timestamp': datetime.now().isoformat(),
            'is_fraud': False
        }

        test_prediction = {
            'predicted_fraud': False,
            'fraud_probability': 0.15,
            'confidence': 'High'
        }

        print("\n💾 Testing transaction save...")
        success = db.save_transaction(test_transaction, test_prediction)

        if success:
            print("✅ Transaction saved successfully!")

            # Get stats
            print("\n📊 Getting fraud statistics...")
            stats = db.get_fraud_stats()
            if stats:
                print(f"   Total transactions today: {stats['total_transactions']}")
                print(f"   Fraud detected: {stats['fraud_detected']}")
                print(f"   Fraud rate: {stats['fraud_rate']}%")
                print(f"   Average amount: ${stats['avg_amount']}")

            # Get recent transactions
            print("\n📋 Recent transactions:")
            recent = db.get_recent_transactions(limit=5)
            for tx in recent:
                fraud_status = "🚨 FRAUD" if tx['predicted_fraud'] else "✅ Normal"
                print(f"   {fraud_status} ${tx['amount']} at {tx['merchant']}")

            # Get fraud alerts
            print("\n🚨 Recent fraud alerts:")
            alerts = db.get_fraud_alerts(hours_back=24)
            for alert in alerts:
                print(f"   ${alert['amount']} at {alert['merchant']} ({alert['minutes_ago']} min ago)")

        db.close()

        print("\n✅ Database handler test complete!")

    else:
        print("❌ Could not connect to database")
        print("💡 Check your .env file and make sure PostgreSQL is running")