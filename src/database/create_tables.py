# Script to create database tables manually

import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def create_tables():
    """Create all required database tables and indexes using environment variables"""

    # Get database connection parameters from environment
    connection_params = {
        'host': os.getenv('DATABASE_HOST', 'localhost'),
        'port': int(os.getenv('DATABASE_PORT', 5432)),
        'database': os.getenv('DATABASE_NAME', 'fraud_detection'),
        'user': os.getenv('DATABASE_USER', 'postgres'),
        'password': os.getenv('DATABASE_PASSWORD', '')
    }

    print("🚀 Creating Database Tables with Environment Variables\n")
    print("🔧 Configuration:")
    print(f"   Host: {connection_params['host']}")
    print(f"   Port: {connection_params['port']}")
    print(f"   Database: {connection_params['database']}")
    print(f"   User: {connection_params['user']}")
    print(f"   Password: {'***' if connection_params['password'] else 'None'}")
    print()

    # Connect to database
    try:
        conn = psycopg2.connect(**connection_params)
        conn.autocommit = True
        cursor = conn.cursor()

        print("✅ Connected to database")

        # Show PostgreSQL version
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"   📋 {version[:50]}...")
        print()

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("💡 Check your .env file and ensure PostgreSQL is running")
        return False

    try:
        # Create transactions table
        print("📋 Creating transactions table...")

        create_table_sql = """
                           CREATE TABLE IF NOT EXISTS transactions \
                           ( \
                               id                    SERIAL PRIMARY KEY, \
                               transaction_id        VARCHAR(50) UNIQUE NOT NULL, \
                               customer_id           VARCHAR(20)        NOT NULL, \
                               amount                DECIMAL(10, 2)     NOT NULL, \
                               merchant              VARCHAR(100)       NOT NULL, \
                               location              VARCHAR(100)       NOT NULL, \
                               transaction_timestamp TIMESTAMP          NOT NULL, \
                               actual_fraud          BOOLEAN, \
                               predicted_fraud       BOOLEAN, \
                               fraud_probability     DECIMAL(5, 4), \
                               confidence            VARCHAR(20), \
                               processing_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
                               created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                           ); \
                           """

        cursor.execute(create_table_sql)
        print("✅ Transactions table created")

        # Create indexes
        print("📋 Creating indexes...")

        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_transaction_timestamp ON transactions(transaction_timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_processing_timestamp ON transactions(processing_timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_customer_id ON transactions(customer_id);",
            "CREATE INDEX IF NOT EXISTS idx_predicted_fraud ON transactions(predicted_fraud);",
            "CREATE INDEX IF NOT EXISTS idx_amount ON transactions(amount);"
        ]

        for index_sql in indexes:
            cursor.execute(index_sql)

        print("✅ Indexes created")

        # Create fraud statistics view
        print("📋 Creating fraud statistics view...")

        create_view_sql = """
                          CREATE \
                          OR REPLACE \
                          VIEW fraud_stats AS
                          SELECT DATE(processing_timestamp)                                                  as date, \
                                 COUNT(*)                                                                    as total_transactions, \
                                 SUM(CASE WHEN predicted_fraud = true THEN 1 ELSE 0 END)                     as fraud_detected, \
                                 ROUND(AVG(CASE WHEN predicted_fraud = true THEN 1.0 ELSE 0.0 END) * 100, \
                                       2)                                                                    as fraud_rate_percent, \
                                 ROUND(AVG(amount), 2)                                                       as avg_amount, \
                                 SUM(CASE WHEN predicted_fraud = true THEN amount ELSE 0 END)                as total_fraud_amount
                          FROM transactions
                          GROUP BY DATE(processing_timestamp)
                          ORDER BY date DESC; \
                          """

        cursor.execute(create_view_sql)
        print("✅ Fraud statistics view created")

        # Insert sample data
        print("📋 Inserting sample data...")

        sample_data_sql = """
                          INSERT INTO transactions (transaction_id, customer_id, amount, merchant, location, \
                                                    transaction_timestamp, actual_fraud, predicted_fraud, \
                                                    fraud_probability, confidence) \
                          VALUES ('SAMPLE_001', 'CUST_1001', 1500.00, 'Luxury Store', 'International', \
                                  NOW() - INTERVAL '1 hour', true, true, 0.95, 'High'), \
                                 ('SAMPLE_002', 'CUST_1002', 45.99, 'Amazon', 'New York, NY', \
                                  NOW() - INTERVAL '30 minutes', false, false, 0.05, 'High'), \
                                 ('SAMPLE_003', 'CUST_1003', 875.50, 'Electronics Store', 'Los Angeles, CA', \
                                  NOW() - INTERVAL '15 minutes', true, true, 0.88, 'High')
                          ON CONFLICT (transaction_id) DO NOTHING; \
                          """

        cursor.execute(sample_data_sql)

        # Check how many rows were inserted
        cursor.execute("SELECT COUNT(*) FROM transactions WHERE transaction_id LIKE 'SAMPLE_%';")
        sample_count = cursor.fetchone()[0]
        print(f"✅ Sample data: {sample_count} sample transactions")

        # Verify table creation
        cursor.execute("SELECT COUNT(*) FROM transactions;")
        total_count = cursor.fetchone()[0]
        print(f"📊 Total transactions in table: {total_count}")

        # Test the fraud stats view
        cursor.execute("SELECT * FROM fraud_stats LIMIT 1;")
        stats = cursor.fetchone()
        if stats:
            print(f"📈 Fraud stats view working - Date: {stats[0]}, Transactions: {stats[1]}")

        cursor.close()
        conn.close()

        print("\n🎉 Database setup complete!")
        print("✅ Tables created successfully")
        print("✅ Indexes optimized for queries")
        print("✅ Views created for analytics")
        print("✅ Sample data inserted")

        return True

    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        cursor.close()
        conn.close()
        return False


def verify_environment():
    """Verify that required environment variables are set"""

    required_vars = [
        'DATABASE_HOST',
        'DATABASE_PORT',
        'DATABASE_NAME',
        'DATABASE_USER',
        'DATABASE_PASSWORD'
    ]

    missing_vars = []

    print("🔍 Checking environment variables...")
    for var in required_vars:
        value = os.getenv(var)
        if value is None:
            missing_vars.append(var)
            print(f"❌ {var}: Not set")
        else:
            # Hide password for security
            display_value = '***' if 'PASSWORD' in var and value else value
            print(f"✅ {var}: {display_value}")

    if missing_vars:
        print(f"\n❌ Missing environment variables: {', '.join(missing_vars)}")
        print("💡 Create a .env file with the required variables")
        return False

    print("✅ All environment variables are set")
    return True


if __name__ == "__main__":
    print("🚀 Database Table Creation Script\n")

    # First verify environment variables
    if not verify_environment():
        print("\n❌ Environment setup incomplete")
        exit(1)

    print()

    # Create tables
    success = create_tables()

    if success:
        print("\n✅ Ready to use! Next steps:")
        print("1. Test database handler: python src/database/db_handler.py")
        print("2. Start the fraud detection pipeline")
        print("3. View results in the dashboard")
    else:
        print("\n❌ Setup failed. Check the error messages above.")
        print("💡 Common issues:")
        print("   - PostgreSQL not running")
        print("   - Incorrect credentials in .env file")
        print("   - Database doesn't exist")