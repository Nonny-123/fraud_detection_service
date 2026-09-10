"""Inspect the saved pipeline and verify a synthetic prediction."""

from pathlib import Path

import joblib
import numpy as np
import pandas as pd


MODEL_PATH = Path(__file__).resolve().parents[1] / "models" / "fraud_xgb_model.joblib"
EXPECTED_INPUTS = [
    "step", "type", "amount", "transaction_type", "net_sender", "net_receiver"
]


def main():
    # Load only the project's trusted model: joblib files can execute Python code.
    model = joblib.load(MODEL_PATH)
    inputs = list(model.feature_names_in_)
    classes = list(model.classes_)
    print(f"Model: {MODEL_PATH}")
    print("Pipeline:", " → ".join(
        f"{name} ({type(step).__name__})" for name, step in model.steps
    ))
    print("Inputs:", inputs)
    print("Classes:", classes)

    if inputs != EXPECTED_INPUTS or classes != [0, 1]:
        raise ValueError("The saved model's inputs or classes differ from expectations.")

    sample = pd.DataFrame([{
        "step": 1,
        "type": "TRANSFER",
        "amount": 100.0,
        "transaction_type": "CC",
        "net_sender": 100.0,
        "net_receiver": 100.0,
    }], columns=EXPECTED_INPUTS)
    probabilities = model.predict_proba(sample)
    if (
        probabilities.shape != (1, 2)
        or not np.isfinite(probabilities).all()
        or not ((probabilities >= 0) & (probabilities <= 1)).all()
        or not np.allclose(probabilities.sum(axis=1), 1.0)
    ):
        raise ValueError(f"Invalid prediction probabilities: {probabilities}")

    print(f"Synthetic transaction fraud probability: {probabilities[0, 1]:.8f}")
    print("Inspection passed. This verifies execution, not model accuracy.")


if __name__ == "__main__":
    main()
