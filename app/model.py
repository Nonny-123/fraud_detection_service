"""Load and score the saved fraud detection pipeline."""

from dataclasses import dataclass
from functools import lru_cache
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import joblib

from app.config import Settings
from app.features import build_features


@dataclass(frozen=True)
class Prediction:
    """Prediction values returned by the service boundary."""

    is_fraud: bool
    fraud_probability: float
    threshold_used: float

    def as_dict(self) -> dict[str, bool | float]:
        return {
            "is_fraud": self.is_fraud,
            "fraud_probability": self.fraud_probability,
            "threshold_used": self.threshold_used,
        }


class FraudModel:
    """A loaded pipeline and its decision threshold."""

    def __init__(self, pipeline: Any, threshold: float):
        self.pipeline = pipeline
        self.threshold = threshold

    @classmethod
    def load(cls, settings: Settings) -> "FraudModel":
        if not settings.model_path.is_file():
            raise FileNotFoundError(f"Model file does not exist: {settings.model_path}")
        pipeline = joblib.load(settings.model_path)
        if not hasattr(pipeline, "predict_proba"):
            raise TypeError("Saved model must provide predict_proba()")
        return cls(pipeline=pipeline, threshold=settings.fraud_threshold)

    def predict(self, transaction: Mapping[str, Any]) -> Prediction:
        features = build_features(transaction)
        probabilities = self.pipeline.predict_proba(features)
        fraud_class_index = list(self.pipeline.classes_).index(1)
        fraud_probability = float(probabilities[0, fraud_class_index])
        return Prediction(
            is_fraud=fraud_probability >= self.threshold,
            fraud_probability=fraud_probability,
            threshold_used=self.threshold,
        )


@lru_cache(maxsize=8)
def get_model(model_path: str, fraud_threshold: float) -> FraudModel:
    """Return one loaded model for each path/threshold pair.

    The explicit arguments make cache behavior predictable when an API process
    and a Kafka worker use different environment settings.
    """
    settings = Settings(
        model_path=Path(model_path),
        fraud_threshold=fraud_threshold,
        kafka_bootstrap_servers="",
        transactions_topic="",
        alerts_topic="",
    )
    return FraudModel.load(settings)


def load_configured_model(settings: Settings | None = None) -> FraudModel:
    settings = settings or Settings.from_environment()
    return get_model(str(settings.model_path), settings.fraud_threshold)
