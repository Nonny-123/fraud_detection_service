"""Environment-driven service configuration."""

from dataclasses import dataclass
import math
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL_PATH = PROJECT_ROOT / "models" / "fraud_xgb_model.joblib"


def _threshold_from_environment() -> float:
    raw_threshold = os.getenv("FRAUD_THRESHOLD", "0.5")
    try:
        threshold = float(raw_threshold)
    except ValueError as exc:
        raise ValueError("FRAUD_THRESHOLD must be a number between 0 and 1") from exc
    if not math.isfinite(threshold) or not 0.0 <= threshold <= 1.0:
        raise ValueError("FRAUD_THRESHOLD must be a number between 0 and 1")
    return threshold


@dataclass(frozen=True)
class Settings:
    """Runtime settings shared by the API and Kafka worker."""

    model_path: Path
    fraud_threshold: float
    kafka_bootstrap_servers: str
    transactions_topic: str
    alerts_topic: str

    @classmethod
    def from_environment(cls) -> "Settings":
        model_path = Path(os.getenv("MODEL_PATH", str(DEFAULT_MODEL_PATH))).expanduser()
        return cls(
            model_path=model_path,
            fraud_threshold=_threshold_from_environment(),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            transactions_topic=os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions.completed"),
            alerts_topic=os.getenv("KAFKA_ALERTS_TOPIC", "fraud.alerts"),
        )
