from src.contracts import TransactionRecord, TransactionValidationError


def test_transaction_record_validates_and_normalizes_payload():
    record = TransactionRecord.from_dict(
        {
            "transaction_id": "TXN_123",
            "customer_id": "CUST_001",
            "amount": "125.50",
            "merchant": "Amazon",
            "location": "New York, NY",
            "timestamp": "2026-04-20T12:30:00",
            "is_fraud": False,
        }
    )

    assert record.transaction_id == "TXN_123"
    assert record.amount == 125.5
    assert record.schema_version == "2.0"


def test_transaction_record_rejects_missing_fields():
    try:
        TransactionRecord.from_dict(
            {
                "transaction_id": "TXN_123",
                "amount": 25.0,
                "merchant": "Amazon",
                "location": "New York, NY",
                "timestamp": "2026-04-20T12:30:00",
            }
        )
    except TransactionValidationError:
        return

    raise AssertionError("Expected TransactionValidationError for missing fields")


