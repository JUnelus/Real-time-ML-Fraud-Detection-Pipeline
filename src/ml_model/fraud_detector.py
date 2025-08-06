# src/ml_model/fraud_detector.py

import json
import pandas as pd
from datetime import datetime
from typing import Dict, List
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import joblib
import os


class FraudDetector:
    """Simple ML model to detect fraudulent transactions"""

    def __init__(self):
        self.model = RandomForestClassifier(n_estimators=50, random_state=42)
        self.is_trained = False
        print("🤖 Fraud Detector initialized!")

    def prepare_features(self, transactions: List[Dict]) -> pd.DataFrame:
        """Convert transaction data into features for ML model"""

        print(f"🔧 Preparing features from {len(transactions)} transactions...")

        features = []
        labels = []

        for tx in transactions:
            # Extract basic features
            feature_row = {
                'amount': float(tx['amount']),
                'hour': datetime.fromisoformat(tx['timestamp']).hour,
                'is_weekend': datetime.fromisoformat(tx['timestamp']).weekday() >= 5,
                'is_high_amount': tx['amount'] > 500,
                'is_international': 'International' in tx['location'],
                'is_luxury_merchant': 'Luxury' in tx['merchant']
            }

            features.append(feature_row)
            labels.append(tx['is_fraud'])

        # Convert to DataFrame
        df = pd.DataFrame(features)
        df['label'] = labels

        print("✅ Features prepared!")
        print(f"   Features: {list(df.columns[:-1])}")
        return df

    def train(self, transactions: List[Dict]):
        """Train the fraud detection model"""

        print("\n🎯 Training fraud detection model...")

        # Prepare features
        df = self.prepare_features(transactions)

        # Separate features and labels
        X = df.drop('label', axis=1)  # Features
        y = df['label']  # Labels (True/False for fraud)

        # Split into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        print(f"📊 Training data:")
        print(f"   - Training samples: {len(X_train)}")
        print(f"   - Testing samples: {len(X_test)}")
        print(f"   - Fraud rate: {y.mean():.1%}")

        # Train the model
        self.model.fit(X_train, y_train)

        # Test the model
        y_pred = self.model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)

        print(f"\n✅ Training complete!")
        print(f"   - Accuracy: {accuracy:.1%}")

        # Show detailed results
        print(f"\n📈 Detailed Results:")
        print(classification_report(y_test, y_pred, target_names=['Normal', 'Fraud']))

        self.is_trained = True
        return accuracy

    def predict_single(self, transaction: Dict) -> Dict:
        """Predict if a single transaction is fraud"""

        if not self.is_trained:
            raise ValueError("❌ Model not trained yet! Call train() first.")

        # Prepare features for this transaction
        features = {
            'amount': float(transaction['amount']),
            'hour': datetime.fromisoformat(transaction['timestamp']).hour,
            'is_weekend': datetime.fromisoformat(transaction['timestamp']).weekday() >= 5,
            'is_high_amount': transaction['amount'] > 500,
            'is_international': 'International' in transaction['location'],
            'is_luxury_merchant': 'Luxury' in transaction['merchant']
        }

        # Convert to DataFrame (model expects this format)
        feature_df = pd.DataFrame([features])

        # Make prediction
        is_fraud = self.model.predict(feature_df)[0]
        fraud_probability = self.model.predict_proba(feature_df)[0][1]  # Probability of fraud

        result = {
            'transaction_id': transaction['transaction_id'],
            'predicted_fraud': bool(is_fraud),
            'fraud_probability': round(float(fraud_probability), 3),
            'confidence': 'High' if fraud_probability > 0.8 or fraud_probability < 0.2 else 'Medium'
        }

        return result

    def save_model(self, filename: str = "fraud_model.pkl"):
        """Save the trained model"""
        if not self.is_trained:
            print("❌ Cannot save untrained model!")
            return

        # Create models directory if it doesn't exist
        os.makedirs("models", exist_ok=True)
        filepath = f"models/{filename}"

        joblib.dump(self.model, filepath)
        print(f"💾 Model saved to {filepath}")

    def load_model(self, filename: str = "fraud_model.pkl"):
        """Load a previously trained model"""
        filepath = f"models/{filename}"

        if not os.path.exists(filepath):
            print(f"❌ Model file not found: {filepath}")
            return False

        self.model = joblib.load(filepath)
        self.is_trained = True
        print(f"📂 Model loaded from {filepath}")
        return True


# Test the fraud detector
if __name__ == "__main__":
    print("🚀 Testing Fraud Detector\n")

    # First, we need some training data
    # Let's generate a larger batch for training
    from src.data_generator.transaction_simulator import TransactionSimulator

    print("📦 Generating training data...")
    simulator = TransactionSimulator()
    training_data = simulator.generate_batch(size=1000, fraud_rate=0.05)  # 5% fraud

    # Create and train the detector
    detector = FraudDetector()
    accuracy = detector.train(training_data)

    # Test on some new transactions
    print("\n🧪 Testing on new transactions...")
    test_data = simulator.generate_batch(size=10, fraud_rate=0.2)  # 20% fraud for testing

    print(f"\n🔍 Predictions:")
    correct_predictions = 0

    for i, tx in enumerate(test_data):
        prediction = detector.predict_single(tx)
        actual_fraud = tx['is_fraud']
        predicted_fraud = prediction['predicted_fraud']

        # Check if prediction was correct
        is_correct = actual_fraud == predicted_fraud
        if is_correct:
            correct_predictions += 1

        status_icon = "✅" if is_correct else "❌"

        print(f"{status_icon} Transaction {i + 1}:")
        print(f"   Amount: ${tx['amount']}")
        print(f"   Merchant: {tx['merchant']}")
        print(f"   Location: {tx['location']}")
        print(f"   Actual: {'FRAUD' if actual_fraud else 'NORMAL'}")
        print(f"   Predicted: {'FRAUD' if predicted_fraud else 'NORMAL'}")
        print(f"   Probability: {prediction['fraud_probability']}")
        print(f"   Confidence: {prediction['confidence']}")
        print()

    test_accuracy = correct_predictions / len(test_data)
    print(f"🎯 Test Results: {correct_predictions}/{len(test_data)} correct ({test_accuracy:.1%})")

    # Save the model
    detector.save_model()

    print("\n✅ Step 3 Complete! Basic fraud detector is working.")