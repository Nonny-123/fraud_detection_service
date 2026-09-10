"""FastAPI application for completed-transaction fraud monitoring."""

from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, Request

from app.config import Settings
from app.model import FraudModel, load_configured_model
from app.schemas import PredictionResponse, TransactionRequest


logger = logging.getLogger("fraud_detection.api")


@asynccontextmanager
async def lifespan(application: FastAPI):
    """Load the pipeline once before serving requests."""
    settings = Settings.from_environment()
    model = load_configured_model(settings)
    application.state.settings = settings
    application.state.model = model
    logger.info("Fraud model loaded", extra={"model_path": str(settings.model_path)})
    yield


app = FastAPI(
    title="Fraud Detection Service",
    description="Post-transaction fraud probability scoring",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health(request: Request) -> dict[str, str]:
    """Report whether the API process has loaded its model."""
    model: FraudModel | None = getattr(request.app.state, "model", None)
    if model is None:
        return {"status": "not_ready"}
    return {"status": "ok"}


@app.post("/predict", response_model=PredictionResponse)
async def predict(transaction: TransactionRequest, request: Request) -> PredictionResponse:
    """Score one completed transaction using the configured threshold."""
    model: FraudModel = request.app.state.model
    prediction = model.predict(transaction.to_transaction())
    logger.info(
        "Transaction scored",
        extra={
            "fraud_probability": prediction.fraud_probability,
            "is_fraud": prediction.is_fraud,
            "threshold_used": prediction.threshold_used,
        },
    )
    return PredictionResponse(**prediction.as_dict())
