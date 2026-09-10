"""Reproduce the training notebook's features for completed transactions."""

from collections.abc import Mapping
from typing import Any

import pandas as pd


FEATURE_COLUMNS = (
    "step",
    "type",
    "amount",
    "transaction_type",
    "net_sender",
    "net_receiver",
)


def build_features(transaction: Mapping[str, Any]) -> pd.DataFrame:
    """Return one model input row without changing the supplied transaction.

    Input is a raw transaction mapping with both pre- and post-transaction
    balances. API and consumer schemas handle field/type validation before
    calling this function. Missing fields raise KeyError; empty or non-string
    account identifiers raise ValueError. Extra fields are not model inputs.

    Scaling and encoding belong to the saved pipeline, not this function.
    """
    for field in ("nameOrig", "nameDest"):
        if not isinstance(transaction[field], str) or not transaction[field]:
            raise ValueError(f"{field} must be a nonempty string")

    features = {
        "step": transaction["step"],
        "type": transaction["type"],
        "amount": transaction["amount"],
        "transaction_type": transaction["nameOrig"][0] + transaction["nameDest"][0],
        "net_sender": transaction["oldbalanceOrg"] - transaction["newbalanceOrig"],
        "net_receiver": transaction["newbalanceDest"] - transaction["oldbalanceDest"],
    }
    return pd.DataFrame([features], columns=FEATURE_COLUMNS)
