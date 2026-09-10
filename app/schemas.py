"""Validated request and response contracts for the prediction API."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _require_nonblank(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("must not be blank")
    return value


class TransactionRequest(BaseModel):
    """Raw fields required to score a completed transaction."""

    model_config = ConfigDict(extra="forbid")

    step: int = Field(ge=0, description="Simulation time step")
    type: str = Field(min_length=1, description="Transaction type, such as TRANSFER")
    amount: float = Field(ge=0, allow_inf_nan=False)
    nameOrig: str = Field(min_length=1, description="Sender account identifier")
    nameDest: str = Field(min_length=1, description="Receiver account identifier")
    oldbalanceOrg: float = Field(ge=0, allow_inf_nan=False)
    newbalanceOrig: float = Field(ge=0, allow_inf_nan=False)
    oldbalanceDest: float = Field(ge=0, allow_inf_nan=False)
    newbalanceDest: float = Field(ge=0, allow_inf_nan=False)

    _clean_type = field_validator("type")( _require_nonblank)
    _clean_sender = field_validator("nameOrig")( _require_nonblank)
    _clean_receiver = field_validator("nameDest")( _require_nonblank)

    def to_transaction(self) -> dict[str, Any]:
        """Return the validated raw fields for feature engineering."""
        return self.model_dump()


class PredictionResponse(BaseModel):
    """Stable response returned after scoring a transaction."""

    model_config = ConfigDict(extra="forbid")

    is_fraud: bool
    fraud_probability: float = Field(ge=0, le=1, allow_inf_nan=False)
    threshold_used: float = Field(ge=0, le=1, allow_inf_nan=False)
