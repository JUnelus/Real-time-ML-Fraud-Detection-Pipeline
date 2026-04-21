from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


class TransactionValidationError(ValueError):
    """Raised when a transaction payload does not meet the required contract."""


@dataclass(frozen=True)
class TransactionRecord:
    """Canonical transaction payload used across the streaming pipeline."""

    transaction_id: str
    customer_id: str
    amount: float
    merchant: str
    location: str
    timestamp: str
    is_fraud: Optional[bool] = None
    schema_version: str = "2.0"
    source: str = "simulator"

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "TransactionRecord":
        if not isinstance(payload, dict):
            raise TransactionValidationError("Transaction payload must be a dictionary.")

        required_fields = [
            "transaction_id",
            "customer_id",
            "amount",
            "merchant",
            "location",
            "timestamp",
        ]
        missing_fields = [field_name for field_name in required_fields if field_name not in payload]
        if missing_fields:
            raise TransactionValidationError(
                f"Transaction payload missing required fields: {', '.join(sorted(missing_fields))}"
            )

        transaction_id = str(payload["transaction_id"]).strip()
        customer_id = str(payload["customer_id"]).strip()
        merchant = str(payload["merchant"]).strip()
        location = str(payload["location"]).strip()
        timestamp = str(payload["timestamp"]).strip()

        if not transaction_id or not customer_id:
            raise TransactionValidationError("Transaction ID and customer ID must be non-empty strings.")
        if not merchant or not location:
            raise TransactionValidationError("Merchant and location must be non-empty strings.")

        try:
            amount = round(float(payload["amount"]), 2)
        except (TypeError, ValueError) as exc:
            raise TransactionValidationError("Transaction amount must be numeric.") from exc

        if amount <= 0:
            raise TransactionValidationError("Transaction amount must be greater than zero.")

        try:
            datetime.fromisoformat(timestamp)
        except ValueError as exc:
            raise TransactionValidationError("Transaction timestamp must be ISO-8601 compatible.") from exc

        is_fraud = payload.get("is_fraud")
        if is_fraud is not None and not isinstance(is_fraud, bool):
            raise TransactionValidationError("Transaction is_fraud flag must be a boolean when provided.")

        return cls(
            transaction_id=transaction_id,
            customer_id=customer_id,
            amount=amount,
            merchant=merchant,
            location=location,
            timestamp=timestamp,
            is_fraud=is_fraud,
            schema_version=str(payload.get("schema_version", "2.0")),
            source=str(payload.get("source", "simulator")),
        )

    @property
    def event_time(self) -> datetime:
        return datetime.fromisoformat(self.timestamp)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PredictionResult:
    """Structured fraud prediction result enriched with explainability metadata."""

    transaction_id: str
    predicted_fraud: bool
    fraud_probability: float
    confidence: str
    risk_level: str
    risk_factors: List[str] = field(default_factory=list)
    model_version: str = "random_forest_v2"
    processing_latency_ms: int = 0

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "PredictionResult":
        if not isinstance(payload, dict):
            raise ValueError("Prediction payload must be a dictionary.")

        return cls(
            transaction_id=str(payload["transaction_id"]),
            predicted_fraud=bool(payload["predicted_fraud"]),
            fraud_probability=round(float(payload["fraud_probability"]), 4),
            confidence=str(payload["confidence"]),
            risk_level=str(payload.get("risk_level", "LOW")),
            risk_factors=list(payload.get("risk_factors", [])),
            model_version=str(payload.get("model_version", "random_forest_v2")),
            processing_latency_ms=int(payload.get("processing_latency_ms", 0)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

