# src/data_generator/transaction_simulator.py

import json
import random
import time
from datetime import datetime
from typing import Dict, List


class TransactionSimulator:
    """Generates realistic financial transaction data"""

    def __init__(self):
        # Common merchants people shop at
        self.merchants = [
            "Amazon", "Walmart", "Target", "Starbucks", "McDonald's",
            "Shell", "Costco", "Home Depot", "CVS", "Uber"
        ]

        # US cities
        self.locations = [
            "New York, NY", "Los Angeles, CA", "Chicago, IL",
            "Houston, TX", "Miami, FL", "Seattle, WA"
        ]

        print("✅ Transaction Simulator initialized!")
        print(f"   - {len(self.merchants)} merchants available")
        print(f"   - {len(self.locations)} locations available")

    def generate_normal_transaction(self) -> Dict:
        """Generate a normal (non-fraudulent) transaction"""

        transaction = {
            'transaction_id': f"TXN_{int(time.time() * 1000)}{random.randint(100, 999)}",
            'customer_id': f"CUST_{random.randint(1000, 9999)}",
            'amount': round(random.uniform(5.0, 200.0), 2),  # Normal amounts
            'merchant': random.choice(self.merchants),
            'location': random.choice(self.locations),
            'timestamp': datetime.now().isoformat(),
            'is_fraud': False
        }

        return transaction

    def generate_fraud_transaction(self) -> Dict:
        """Generate a fraudulent transaction with suspicious patterns"""

        # Start with a normal transaction
        transaction = self.generate_normal_transaction()

        # Add fraud indicators
        fraud_type = random.choice(['high_amount', 'unusual_location', 'late_night'])

        if fraud_type == 'high_amount':
            transaction['amount'] = round(random.uniform(1000.0, 5000.0), 2)
            transaction['merchant'] = "Luxury Electronics Store"

        elif fraud_type == 'unusual_location':
            transaction['location'] = "International Location"
            transaction['amount'] = round(random.uniform(100.0, 800.0), 2)

        elif fraud_type == 'late_night':
            # Simulate 3 AM transaction (suspicious time)
            transaction['timestamp'] = datetime.now().replace(hour=3, minute=random.randint(0, 59)).isoformat()
            transaction['amount'] = round(random.uniform(200.0, 1000.0), 2)

        transaction['is_fraud'] = True
        return transaction

    def generate_batch(self, size: int = 100, fraud_rate: float = 0.05) -> List[Dict]:
        """Generate a batch of transactions

        Args:
            size: Total number of transactions
            fraud_rate: Percentage that should be fraud (0.05 = 5%)
        """

        transactions = []
        fraud_count = int(size * fraud_rate)
        normal_count = size - fraud_count

        print(f"📊 Generating {size} transactions:")
        print(f"   - Normal transactions: {normal_count}")
        print(f"   - Fraud transactions: {fraud_count}")

        # Generate normal transactions
        for _ in range(normal_count):
            transactions.append(self.generate_normal_transaction())

        # Generate fraud transactions
        for _ in range(fraud_count):
            transactions.append(self.generate_fraud_transaction())

        # Shuffle them so fraud isn't all at the end
        random.shuffle(transactions)

        print("✅ Batch generation complete!")
        return transactions

    def save_to_file(self, transactions: List[Dict], filename: str = "sample_transactions.json"):
        """Save transactions to a JSON file"""

        # Create data directory if it doesn't exist
        import os
        os.makedirs("data", exist_ok=True)

        filepath = f"data/{filename}"

        with open(filepath, 'w') as f:
            json.dump(transactions, f, indent=2)

        print(f"💾 Saved {len(transactions)} transactions to {filepath}")


# Test the simulator
if __name__ == "__main__":
    print("🚀 Testing Transaction Simulator\n")

    # Create simulator
    simulator = TransactionSimulator()

    # Generate a few sample transactions
    print("\n📝 Sample Normal Transaction:")
    normal_tx = simulator.generate_normal_transaction()
    print(json.dumps(normal_tx, indent=2))

    print("\n🚨 Sample Fraud Transaction:")
    fraud_tx = simulator.generate_fraud_transaction()
    print(json.dumps(fraud_tx, indent=2))

    # Generate a batch
    print("\n📦 Generating batch of transactions...")
    batch = simulator.generate_batch(size=20, fraud_rate=0.1)  # 10% fraud rate

    # Show some stats
    fraud_count = sum(1 for tx in batch if tx['is_fraud'])
    print(f"\n📈 Batch Statistics:")
    print(f"   - Total transactions: {len(batch)}")
    print(f"   - Fraud transactions: {fraud_count}")
    print(f"   - Normal transactions: {len(batch) - fraud_count}")

    # Save to file
    simulator.save_to_file(batch, "test_batch.json")

    print("\n✅ Step 2 Complete! Transaction simulator is working.")