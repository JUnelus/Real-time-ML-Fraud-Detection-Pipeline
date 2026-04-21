# src/ml_model/fraud_detector.py

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split

from config.settings import get_settings
from src.contracts import PredictionResult, TransactionRecord


class FraudDetector:
    """Random Forest fraud detector with lightweight explainability metadata."""

    def __init__(self, model_path: str | None = None):
        self.settings = get_settings()
        self.model = RandomForestClassifier(n_estimators=75, random_state=42)
        self.is_trained = False
        resolved_path = Path(model_path) if model_path else self.settings.resolved_model_path
        self.model_path = resolved_path
        self.model_version = self.settings.model.version
        print("🤖 Fraud Detector initialized!")
        print(f"   📦 Model path: {self.model_path}")
        print(f"   🏷️  Model version: {self.model_version}")

    def _build_feature_row(self, transaction: TransactionRecord) -> Dict:
        timestamp = transaction.event_time
        return {
            "amount": transaction.amount,
            "hour": timestamp.hour,
            "is_weekend": timestamp.weekday() >= 5,
            "is_high_amount": transaction.amount > 500,
            "is_international": "International" in transaction.location,
            "is_luxury_merchant": "Luxury" in transaction.merchant,
        }

    def prepare_features(self, transactions: List[Dict]) -> pd.DataFrame:
        """Convert transaction data into features for ML model."""

        print(f"🔧 Preparing features from {len(transactions)} transactions...")

        features = []
        labels = []

        for tx in transactions:
            transaction = tx if isinstance(tx, TransactionRecord) else TransactionRecord.from_dict(tx)
            features.append(self._build_feature_row(transaction))
            labels.append(bool(transaction.is_fraud))

        df = pd.DataFrame(features)
        df["label"] = labels

        print("✅ Features prepared!")
        print(f"   Features: {list(df.columns[:-1])}")
        return df

    def train(self, transactions: List[Dict]):
        """Train the fraud detection model."""

        print("\n🎯 Training fraud detection model...")
        df = self.prepare_features(transactions)

        X = df.drop("label", axis=1)
        y = df["label"]

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.2,
            random_state=42,
            stratify=y if y.nunique() > 1 else None,
        )

        print("📊 Training data:")
        print(f"   - Training samples: {len(X_train)}")
        print(f"   - Testing samples: {len(X_test)}")
        print(f"   - Fraud rate: {y.mean():.1%}")

        self.model.fit(X_train, y_train)

        y_pred = self.model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)

        print("\n✅ Training complete!")
        print(f"   - Accuracy: {accuracy:.1%}")
        print("\n📈 Detailed Results:")
        print(classification_report(y_test, y_pred, target_names=["Normal", "Fraud"]))

        self.is_trained = True
        return accuracy

    def _extract_risk_factors(self, transaction: TransactionRecord) -> List[str]:
        risk_factors: List[str] = []
        event_time = transaction.event_time

        if transaction.amount >= 1000:
            risk_factors.append("high_amount")
        elif transaction.amount >= 500:
            risk_factors.append("elevated_amount")

        if "International" in transaction.location:
            risk_factors.append("international_location")

        if "Luxury" in transaction.merchant:
            risk_factors.append("luxury_merchant")

        if event_time.hour <= 4 or event_time.hour >= 23:
            risk_factors.append("odd_hour_activity")

        if not risk_factors:
            risk_factors.append("routine_behavior")

        return risk_factors

    @staticmethod
    def _derive_risk_level(probability: float) -> str:
        if probability >= 0.85:
            return "HIGH"
        if probability >= 0.5:
            return "MEDIUM"
        return "LOW"

    @staticmethod
    def _derive_confidence(probability: float) -> str:
        if probability >= 0.85 or probability <= 0.15:
            return "High"
        if probability >= 0.65 or probability <= 0.35:
            return "Medium"
        return "Review"

    def predict_single(self, transaction: Dict) -> Dict:
        """Predict if a single transaction is fraud and return enriched metadata."""

        if not self.is_trained:
            raise ValueError("❌ Model not trained yet! Call train() first.")

        transaction_record = (
            transaction if isinstance(transaction, TransactionRecord) else TransactionRecord.from_dict(transaction)
        )
        feature_df = pd.DataFrame([self._build_feature_row(transaction_record)])

        is_fraud = bool(self.model.predict(feature_df)[0])
        fraud_probability = round(float(self.model.predict_proba(feature_df)[0][1]), 4)

        prediction = PredictionResult(
            transaction_id=transaction_record.transaction_id,
            predicted_fraud=is_fraud,
            fraud_probability=fraud_probability,
            confidence=self._derive_confidence(fraud_probability),
            risk_level=self._derive_risk_level(fraud_probability),
            risk_factors=self._extract_risk_factors(transaction_record),
            model_version=self.model_version,
        )
        return prediction.to_dict()

    def save_model(self):
        """Save the trained model to the configured path."""
        if not self.is_trained:
            print("❌ Cannot save untrained model!")
            return

        self.model_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self.model, self.model_path)
        print(f"💾 Model saved to {self.model_path}")

    def load_model(self):
        """Load a previously trained model from the configured path."""
        if not self.model_path.exists():
            print(f"❌ Model file not found: {self.model_path}")
            return False

        self.model = joblib.load(self.model_path)
        self.is_trained = True
        print(f"📂 Model loaded from {self.model_path}")
        return True


if __name__ == "__main__":
    print("🚀 Testing Fraud Detector\n")

    from src.data_generator.transaction_simulator import TransactionSimulator

    print("📦 Generating training data...")
    simulator = TransactionSimulator()
    training_data = simulator.generate_batch(size=1000, fraud_rate=0.05)

    detector = FraudDetector()
    accuracy = detector.train(training_data)

    print("\n🧪 Testing on new transactions...")
    test_data = simulator.generate_batch(size=10, fraud_rate=0.2)

    print("\n🔍 Predictions:")
    correct_predictions = 0

    for index, transaction in enumerate(test_data, start=1):
        prediction = detector.predict_single(transaction)
        actual_fraud = bool(transaction["is_fraud"])
        predicted_fraud = prediction["predicted_fraud"]
        is_correct = actual_fraud == predicted_fraud
        if is_correct:
            correct_predictions += 1

        status_icon = "✅" if is_correct else "❌"
        print(f"{status_icon} Transaction {index}:")
        print(f"   Amount: ${transaction['amount']}")
        print(f"   Merchant: {transaction['merchant']}")
        print(f"   Location: {transaction['location']}")
        print(f"   Actual: {'FRAUD' if actual_fraud else 'NORMAL'}")
        print(f"   Predicted: {'FRAUD' if predicted_fraud else 'NORMAL'}")
        print(f"   Probability: {prediction['fraud_probability']}")
        print(f"   Risk level: {prediction['risk_level']}")
        print(f"   Risk factors: {', '.join(prediction['risk_factors'])}")
        print()

    test_accuracy = correct_predictions / len(test_data)
    print(f"🎯 Test Results: {correct_predictions}/{len(test_data)} correct ({test_accuracy:.1%})")

    detector.save_model()
    print(f"📦 Final model accuracy: {accuracy:.1%}")
    print("\n✅ Enhanced fraud detector is working.")

