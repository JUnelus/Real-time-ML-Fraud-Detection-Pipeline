# Script to create database tables manually

from __future__ import annotations

from config.settings import get_settings
from src.database.db_handler import DatabaseHandler


def create_tables() -> bool:
    """Create all required database tables, indexes, and views."""

    settings = get_settings()

    print("🚀 Creating Database Tables with Centralized Settings\n")
    print("🔧 Configuration:")
    print(f"   Host: {settings.database.host}")
    print(f"   Port: {settings.database.port}")
    print(f"   Database: {settings.database.name}")
    print(f"   User: {settings.database.user}")
    print(f"   Password: {'***' if settings.database.password else 'None'}")
    print()

    db = DatabaseHandler(settings=settings)
    if not db.connect():
        print("❌ Connection failed")
        print("💡 Check your environment variables and ensure PostgreSQL is running")
        return False

    if not db.ensure_schema():
        db.close()
        return False

    db.seed_sample_data()

    stats = db.get_fraud_stats() or {}
    print("✅ Database schema verified")
    print(f"📊 Seeded transactions today: {stats.get('total_transactions', 0)}")
    print(f"🧯 Dead-lettered today: {stats.get('dead_lettered_today', 0)}")
    db.close()
    return True


if __name__ == "__main__":
    print("🚀 Database Table Creation Script\n")
    success = create_tables()

    if success:
        print("\n✅ Ready to use! Next steps:")
        print("1. Start the consumer: python src/streaming/kafka_consumer.py")
        print("2. Start the producer: python src/streaming/kafka_producer.py")
        print("3. Launch the dashboard: python dashboard.py")
    else:
        print("\n❌ Setup failed. Check the error messages above.")
