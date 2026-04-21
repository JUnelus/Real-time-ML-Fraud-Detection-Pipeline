from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, render_template

from config.settings import AppSettings, get_settings
from src.database.db_handler import DatabaseHandler


def _serialize_transaction(tx: dict) -> dict:
    return {
        "transaction_id": tx["transaction_id"],
        "amount": float(tx["amount"]),
        "merchant": tx["merchant"],
        "location": tx["location"],
        "predicted_fraud": bool(tx["predicted_fraud"]),
        "fraud_probability": float(tx["fraud_probability"] or 0),
        "confidence": tx["confidence"],
        "risk_level": tx.get("risk_level", "LOW"),
        "risk_factors": list(tx.get("risk_factors") or []),
        "pipeline_status": tx.get("pipeline_status", "processed"),
        "retry_count": int(tx.get("retry_count") or 0),
        "processing_latency_ms": int(tx.get("processing_latency_ms") or 0),
        "timestamp": tx["processing_timestamp"].strftime("%H:%M:%S") if tx.get("processing_timestamp") else "Unknown",
    }


def _serialize_event(event: dict) -> dict:
    return {
        "transaction_id": event.get("transaction_id") or "Unknown",
        "event_type": event.get("event_type", "unknown"),
        "status": event.get("status", "unknown"),
        "stage": event.get("stage") or "pipeline",
        "details": dict(event.get("details") or {}),
        "created_at": event["created_at"].strftime("%H:%M:%S") if event.get("created_at") else "Unknown",
    }


def create_app(db_handler: Optional[DatabaseHandler] = None, settings: Optional[AppSettings] = None) -> Flask:
    app = Flask(__name__)
    app.config["SETTINGS"] = settings or get_settings()
    app.config["DB_HANDLER"] = db_handler

    def get_db() -> DatabaseHandler:
        return app.config["DB_HANDLER"] or DatabaseHandler(settings=app.config["SETTINGS"])

    @app.route("/")
    def dashboard():
        return render_template("dashboard.html", refresh_seconds=app.config["SETTINGS"].pipeline.dashboard_refresh_seconds)

    @app.route("/api/stats")
    def get_stats():
        db = get_db()
        try:
            if not db.connect():
                return jsonify({"error": "Database connection failed"}), 500

            stats = db.get_fraud_stats() or {}
            return jsonify(
                {
                    "total_transactions": int(stats.get("total_transactions", 0)),
                    "fraud_detected": int(stats.get("fraud_detected", 0)),
                    "fraud_rate": float(stats.get("fraud_rate", 0)),
                    "avg_amount": float(stats.get("avg_amount", 0)),
                    "total_fraud_amount": float(stats.get("total_fraud_amount", 0) or 0),
                    "avg_latency_ms": float(stats.get("avg_latency_ms", 0) or 0),
                    "high_risk_alerts": int(stats.get("high_risk_alerts", 0) or 0),
                    "dead_lettered_today": int(stats.get("dead_lettered_today", 0) or 0),
                    "duplicate_events_today": int(stats.get("duplicate_events_today", 0) or 0),
                    "errors_today": int(stats.get("errors_today", 0) or 0),
                    "last_updated": datetime.now().strftime("%H:%M:%S"),
                }
            )
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
        finally:
            if app.config["DB_HANDLER"] is None:
                db.close()

    @app.route("/api/recent")
    def get_recent_transactions():
        db = get_db()
        try:
            if not db.connect():
                return jsonify({"error": "Database connection failed"}), 500
            transactions = db.get_recent_transactions(limit=10)
            return jsonify([_serialize_transaction(tx) for tx in transactions])
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
        finally:
            if app.config["DB_HANDLER"] is None:
                db.close()

    @app.route("/api/alerts")
    def get_alerts():
        db = get_db()
        try:
            if not db.connect():
                return jsonify({"error": "Database connection failed"}), 500
            alerts = db.get_fraud_alerts(hours_back=24)
            return jsonify(
                [
                    {
                        "transaction_id": alert["transaction_id"],
                        "amount": float(alert["amount"]),
                        "merchant": alert["merchant"],
                        "location": alert["location"],
                        "fraud_probability": float(alert["fraud_probability"]),
                        "risk_level": alert.get("risk_level", "HIGH"),
                        "risk_factors": list(alert.get("risk_factors") or []),
                        "minutes_ago": int(alert.get("minutes_ago") or 0),
                    }
                    for alert in alerts
                ]
            )
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
        finally:
            if app.config["DB_HANDLER"] is None:
                db.close()

    @app.route("/api/operations")
    def get_operations():
        db = get_db()
        try:
            if not db.connect():
                return jsonify({"error": "Database connection failed"}), 500

            metrics = db.get_operational_metrics()
            failures = db.get_recent_pipeline_events(limit=8, status="error")
            latest_events = db.get_recent_pipeline_events(limit=8)

            return jsonify(
                {
                    "metrics": {
                        "processed_last_hour": int(metrics.get("processed_last_hour", 0) or 0),
                        "errors_last_hour": int(metrics.get("errors_last_hour", 0) or 0),
                        "dead_letter_last_hour": int(metrics.get("dead_letter_last_hour", 0) or 0),
                        "duplicates_last_hour": int(metrics.get("duplicates_last_hour", 0) or 0),
                        "latest_processing_timestamp": (
                            metrics.get("latest_processing_timestamp").isoformat()
                            if metrics.get("latest_processing_timestamp")
                            else None
                        ),
                    },
                    "failures": [_serialize_event(event) for event in failures],
                    "latest_events": [_serialize_event(event) for event in latest_events],
                }
            )
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
        finally:
            if app.config["DB_HANDLER"] is None:
                db.close()

    @app.route("/api/health")
    def get_health():
        db = get_db()
        settings_obj = app.config["SETTINGS"]
        model_path = Path(settings_obj.resolved_model_path)

        try:
            db_ok = db.connect()
            metrics = db.get_operational_metrics() if db_ok else {}
            model_available = model_path.exists()

            latest_processing_timestamp = metrics.get("latest_processing_timestamp")
            is_stale = True
            if latest_processing_timestamp:
                minutes_since_last_event = (datetime.now() - latest_processing_timestamp).total_seconds() / 60
                is_stale = minutes_since_last_event > settings_obj.pipeline.stale_after_minutes

            status = "healthy"
            if not db_ok:
                status = "down"
            elif not model_available or is_stale or int(metrics.get("errors_last_hour", 0) or 0) > 0:
                status = "degraded"

            return jsonify(
                {
                    "status": status,
                    "components": {
                        "database": "up" if db_ok else "down",
                        "model": "loaded" if model_available else "missing",
                        "stream_activity": "active" if not is_stale else "stale",
                    },
                    "dead_letter_topic": settings_obj.kafka.dead_letter_topic,
                    "transactions_topic": settings_obj.kafka.transactions_topic,
                    "model_path": str(model_path),
                    "latest_processing_timestamp": latest_processing_timestamp.isoformat() if latest_processing_timestamp else None,
                }
            )
        except Exception as exc:
            return jsonify({"status": "down", "error": str(exc)}), 500
        finally:
            if app.config["DB_HANDLER"] is None:
                db.close()

    @app.route("/api/config")
    def get_config():
        settings_obj = app.config["SETTINGS"]
        return jsonify(
            {
                "database_host": settings_obj.database.host,
                "database_name": settings_obj.database.name,
                "database_user": settings_obj.database.user,
                "kafka_servers": settings_obj.kafka.bootstrap_servers,
                "transactions_topic": settings_obj.kafka.transactions_topic,
                "dead_letter_topic": settings_obj.kafka.dead_letter_topic,
                "flask_host": settings_obj.flask.host,
                "flask_port": settings_obj.flask.port,
                "model_path": str(settings_obj.resolved_model_path),
            }
        )

    return app


app = create_app()


if __name__ == "__main__":
    settings = get_settings()
    print("🌐 Starting Fraud Detection Dashboard with Reliability + Observability APIs...")
    print("\n🔧 Configuration:")
    print(f"   Database: {settings.database.user}@{settings.database.host}:{settings.database.port}/{settings.database.name}")
    print(f"   Kafka: {', '.join(settings.kafka.bootstrap_servers)}")
    print(f"   Topics: {settings.kafka.transactions_topic} | DLQ: {settings.kafka.dead_letter_topic}")
    print(f"   Flask: {settings.flask.host}:{settings.flask.port}")
    print(f"\n📊 Dashboard will be available at: http://{settings.flask.host}:{settings.flask.port}")
    print(f"🔄 Auto-refreshes every {settings.pipeline.dashboard_refresh_seconds} seconds")
    print("⏹️  Press Ctrl+C to stop\n")
    app.run(host=settings.flask.host, port=settings.flask.port, debug=settings.flask.debug)

