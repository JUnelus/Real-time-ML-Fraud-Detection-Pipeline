# src/data_generator/transaction_simulator.py

from __future__ import annotations

import json
import random
from datetime import datetime
from typing import Dict, List
from uuid import uuid4

from src.contracts import TransactionRecord


class TransactionSimulator:
    """Generates realistic financial transaction data."""

    def __init__(self):
        self.merchants = [
            "Amazon", "Walmart", "Target", "Starbucks", "McDonald's",
            "Shell", "Costco", "Home Depot", "CVS", "Uber"
        ]
        self.locations = [
            "New York, NY", "Los Angeles, CA", "Chicago, IL",
            "Houston, TX", "Miami, FL", "Seattle, WA"
        ]

        print("✅ Transaction Simulator initialized!")
        print(f"   - {len(self.merchants)} merchants available")
        print(f"   - {len(self.locations)} locations available")

    @staticmethod
    def _build_transaction_id() -> str:
        return f"TXN_{uuid4().hex[:12].upper()}"

    def generate_normal_transaction(self) -> Dict:
        """Generate a normal (non-fraudulent) transaction."""

        transaction = TransactionRecord(
            transaction_id=self._build_transaction_id(),
            customer_id=f"CUST_{random.randint(1000, 9999)}",
            amount=round(random.uniform(5.0, 200.0), 2),
            merchant=random.choice(self.merchants),
            location=random.choice(self.locations),
            timestamp=datetime.now().isoformat(),
            is_fraud=False,
        )
        return transaction.to_dict()

    def generate_fraud_transaction(self) -> Dict:
        """Generate a fraudulent transaction with suspicious patterns."""

        transaction = self.generate_normal_transaction()
        fraud_type = random.choice(["high_amount", "unusual_location", "late_night"])

        if fraud_type == "high_amount":
            transaction["amount"] = round(random.uniform(1000.0, 5000.0), 2)
            transaction["merchant"] = "Luxury Electronics Store"
        elif fraud_type == "unusual_location":
            transaction["location"] = "International Location"
            transaction["amount"] = round(random.uniform(100.0, 800.0), 2)
        else:
            transaction["timestamp"] = datetime.now().replace(hour=3, minute=random.randint(0, 59)).isoformat()
            transaction["amount"] = round(random.uniform(200.0, 1000.0), 2)

        transaction["is_fraud"] = True
        return transaction

    def generate_batch(self, size: int = 100, fraud_rate: float = 0.05) -> List[Dict]:
        """Generate a batch of transactions."""

        transactions: List[Dict] = []
        fraud_count = int(size * fraud_rate)
        normal_count = size - fraud_count

        print(f"📊 Generating {size} transactions:")
        print(f"   - Normal transactions: {normal_count}")
        print(f"   - Fraud transactions: {fraud_count}")

        for _ in range(normal_count):
            transactions.append(self.generate_normal_transaction())

        for _ in range(fraud_count):
            transactions.append(self.generate_fraud_transaction())

        random.shuffle(transactions)
        print("✅ Batch generation complete!")
        return transactions

    def save_to_file(self, transactions: List[Dict], filename: str = "sample_transactions.json"):
        """Save transactions to a JSON file."""

        from pathlib import Path

        data_dir = Path("data")
        data_dir.mkdir(parents=True, exist_ok=True)
        filepath = data_dir / filename

        with filepath.open("w", encoding="utf-8") as file_handle:
            json.dump(transactions, file_handle, indent=2)

        print(f"💾 Saved {len(transactions)} transactions to {filepath}")


if __name__ == "__main__":
    print("🚀 Testing Transaction Simulator\n")

    simulator = TransactionSimulator()

    print("\n📝 Sample Normal Transaction:")
    normal_tx = simulator.generate_normal_transaction()
    print(json.dumps(normal_tx, indent=2))

    print("\n🚨 Sample Fraud Transaction:")
    fraud_tx = simulator.generate_fraud_transaction()
    print(json.dumps(fraud_tx, indent=2))

    print("\n📦 Generating batch of transactions...")
    batch = simulator.generate_batch(size=20, fraud_rate=0.1)

    fraud_count = sum(1 for tx in batch if tx["is_fraud"])
    print("\n📈 Batch Statistics:")
    print(f"   - Total transactions: {len(batch)}")
    print(f"   - Fraud transactions: {fraud_count}")
    print(f"   - Normal transactions: {len(batch) - fraud_count}")

    simulator.save_to_file(batch, "test_batch.json")
    print("\n✅ Enhanced transaction simulator is working.")

