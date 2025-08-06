import os
from flask import Flask, render_template, jsonify
from src.database.db_handler import DatabaseHandler
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Initialize database connection using environment variables
db = DatabaseHandler()


@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')


@app.route('/api/stats')
def get_stats():
    """API endpoint for fraud statistics"""
    try:
        if not db.connect():
            return jsonify({'error': 'Database connection failed'}), 500

        stats = db.get_fraud_stats()
        db.close()

        if not stats:
            return jsonify({
                'total_transactions': 0,
                'fraud_detected': 0,
                'fraud_rate': 0,
                'avg_amount': 0,
                'total_fraud_amount': 0
            })

        return jsonify({
            'total_transactions': int(stats['total_transactions']),
            'fraud_detected': int(stats['fraud_detected']),
            'fraud_rate': float(stats['fraud_rate']),
            'avg_amount': float(stats['avg_amount']),
            'total_fraud_amount': float(stats['total_fraud_amount']) if stats['total_fraud_amount'] else 0,
            'last_updated': datetime.now().strftime('%H:%M:%S')
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/recent')
def get_recent_transactions():
    """API endpoint for recent transactions"""
    try:
        if not db.connect():
            return jsonify({'error': 'Database connection failed'}), 500

        transactions = db.get_recent_transactions(limit=10)
        db.close()

        # Format transactions for JSON
        formatted_transactions = []
        for tx in transactions:
            formatted_transactions.append({
                'transaction_id': tx['transaction_id'],
                'amount': float(tx['amount']),
                'merchant': tx['merchant'],
                'location': tx['location'],
                'predicted_fraud': tx['predicted_fraud'],
                'fraud_probability': float(tx['fraud_probability']) if tx['fraud_probability'] else 0,
                'confidence': tx['confidence'],
                'timestamp': tx['processing_timestamp'].strftime('%H:%M:%S') if tx[
                    'processing_timestamp'] else 'Unknown'
            })

        return jsonify(formatted_transactions)

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/config')
def get_config():
    """API endpoint for configuration info (for debugging)"""
    return jsonify({
        'database_host': os.getenv('DATABASE_HOST', 'localhost'),
        'database_name': os.getenv('DATABASE_NAME', 'fraud_detection'),
        'database_user': os.getenv('DATABASE_USER', 'postgres'),
        'kafka_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'flask_host': os.getenv('FLASK_HOST', '0.0.0.0'),
        'flask_port': os.getenv('FLASK_PORT', '5000'),
        'model_path': os.getenv('MODEL_PATH', 'models/fraud_model.pkl')
    })


if __name__ == '__main__':
    print("🌐 Starting Fraud Detection Dashboard with Environment Variables...")

    # Show configuration
    print("\n🔧 Configuration:")
    print(
        f"   Database: {os.getenv('DATABASE_USER', 'postgres')}@{os.getenv('DATABASE_HOST', 'localhost')}:{os.getenv('DATABASE_PORT', '5432')}/{os.getenv('DATABASE_NAME', 'fraud_detection')}")
    print(f"   Kafka: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
    print(f"   Flask: {os.getenv('FLASK_HOST', '0.0.0.0')}:{os.getenv('FLASK_PORT', '5000')}")

    # Get Flask configuration from environment
    flask_host = os.getenv('FLASK_HOST', '0.0.0.0')
    flask_port = int(os.getenv('FLASK_PORT', 5000))
    flask_debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'

    print(f"\n📊 Dashboard will be available at: http://{flask_host}:{flask_port}")
    print("🔄 Auto-refreshes every 5 seconds")
    print("⏹️  Press Ctrl+C to stop")

    # Test database connection on startup
    print("\n🔗 Testing database connection...")
    test_db = DatabaseHandler()
    if test_db.connect():
        print("✅ Database connection successful")
        test_db.close()
    else:
        print("❌ Database connection failed - check your .env file")
        print("💡 Dashboard will still start but may show connection errors")

    print()

    app.run(
        host=flask_host,
        port=flask_port,
        debug=flask_debug
    )