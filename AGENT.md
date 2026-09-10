# Fraud Detection Service — Agent Build Guide

This file is context for an AI coding agent picking up this project. The model
was built manually in a Jupyter notebook (no agent involved). This doc tells
you what already exists, what's risky about it, and exactly what to build next.

---

## 1. Project status — what already exists

- **Source data**: PaySim-style transaction dataset, 6,362,620 rows, extreme
  class imbalance (0.129% fraud, ~8,214 fraud rows total).
- **Model**: XGBoost classifier inside a scikit-learn `Pipeline`
  (`ColumnTransformer` → `XGBClassifier`), trained via `GridSearchCV` +
  `StratifiedKFold(5)`, saved to disk with `joblib.dump()`.
- **Final feature set (what the saved pipeline expects as input)**:
  `step`, `type`, `amount`, `transaction_type`, `net_sender`, `net_receiver`
  → predicts `isFraud`.
- **Exact feature derivations** (must be reproduced identically outside the
  notebook — this is the single most important thing to get right):
  - `transaction_type` = first character of `nameOrig` + first character of
    `nameDest` (e.g. `"CC"`, `"CM"`). In this dataset `nameOrig` is always
    customer-prefixed (`C`), so in practice this column mostly encodes
    whether the **receiver** is a Customer or a Merchant account.
  - `net_sender` = `oldbalanceOrg` − `newbalanceOrig`
  - `net_receiver` = `newbalanceDest` − `oldbalanceDest`
  - Raw columns dropped after deriving the above: `nameOrig`, `nameDest`,
    `isFlaggedFraud`, `oldbalanceOrg`, `newbalanceOrig`, `oldbalanceDest`,
    `newbalanceDest`.
- **Preprocessing baked into the saved pipeline**: `StandardScaler` on
  numeric columns (`step`, `amount`, `net_sender`, `net_receiver`),
  `OneHotEncoder(handle_unknown="ignore")` on categorical columns (`type`,
  `transaction_type`). Because this is inside the pipeline object, the
  service does **not** need to re-implement scaling/encoding — only the
  feature *derivation* above, before calling `.predict_proba()`.
- **Measured performance** (test set, 164 fraud cases, default 0.5
  threshold): precision 0.12, recall 0.87, F1 0.21 for the fraud class.
  Translation: the model catches most fraud, but ~88% of what it flags is a
  false positive. Whatever consumes fraud alerts downstream needs to
  tolerate that volume.
- **Not yet computed**: PR-AUC, ROC-AUC, or any threshold sweep. Do not
  assume 0.5 is the right operating threshold — treat it as configurable.

---

## 2. CRITICAL — read before writing any Kafka or API code

**Feature timing problem.** `net_sender` and `net_receiver` require
`newbalanceOrig` and `newbalanceDest` — the account balances *after* a
transaction has been executed. Those values do not exist before a
transaction is approved. This means:

- **Post-transaction event monitoring** (score transactions that have
  already completed, to flag/alert/investigate) → current features are
  valid. Kafka fits naturally: a `transactions.completed` topic carries
  events that already contain both balances.
- **Real-time pre-transaction blocking** (score before allowing a transfer
  to go through) → the current model **cannot** work as-is. There is no
  `newbalanceOrig`/`newbalanceDest` yet at decision time. This would need
  the features reworked to use only pre-transaction data (e.g. `oldbalance`
  + the *requested* amount) and the model retrained.

**Default assumption for this build phase: post-transaction event
monitoring.** This is a modeling-scope decision, not an infra detail —
confirm it with Nonny before finalizing the Kafka topic schema. If the real
goal is pre-transaction blocking, stop and flag it; that's a retraining
task, not a FastAPI/Kafka/Docker task.

**Secondary issue, lower urgency**: hyperparameter tuning in the notebook
used `GridSearchCV`'s default scoring (accuracy), not F1/recall/PR-AUC.
With 0.13% fraud, accuracy is close to meaningless (predicting all-zero
scores 99.87%), so the "best" hyperparameters weren't actually selected
against the metric that matters. Don't fix this as part of the current
build — just don't treat the current model as fully tuned.

---

## 3. Explicitly out of scope for this phase

- Do not retrain, re-tune, or change the model. That's a separate,
  later task.
- Do not build CI/CD, tests, or AWS deployment yet — see Roadmap (§6).
  Get FastAPI + Kafka + Docker working and manually verified first.

---

## 4. Build scope — this phase

### 4.1 FastAPI service
- `POST /predict`
  - Request body (Pydantic model): the raw-ish transaction fields a real
    event would carry — `step`, `type`, `amount`, `nameOrig`, `nameDest`,
    `oldbalanceOrg`, `newbalanceOrig`, `oldbalanceDest`, `newbalanceDest`.
    The API derives `transaction_type`, `net_sender`, `net_receiver`
    internally, using the **same feature module** the Kafka consumer uses
    (see 4.2) — one implementation, not two copies that can drift apart.
  - Response: `{"is_fraud": bool, "fraud_probability": float, "threshold_used": float}`
  - Load the joblib pipeline once at app startup (module-level or via
    FastAPI lifespan), not per-request.
  - `GET /health` for liveness/readiness.
  - Threshold configurable via env var, default 0.5, clearly documented as
    unvalidated.

### 4.2 Shared feature engineering module
- Single Python module (e.g. `features.py`) implementing the exact
  derivations in §1, imported by both the FastAPI handler and the Kafka
  consumer. This is the piece most likely to silently drift from the
  notebook if duplicated — don't duplicate it.

### 4.3 Kafka integration
- Producer: publishes transaction-completed events to a topic (e.g.
  `transactions.completed`).
- Consumer: subscribes to that topic, runs the shared feature module +
  `pipeline.predict_proba()`, and publishes to `fraud.alerts` (or writes to
  a DB) when probability crosses the configured threshold.
- Pick one Kafka client library and stay consistent with the async FastAPI
  style (e.g. `aiokafka`); don't mix sync and async clients.
- Local Kafka via Docker Compose for dev (single-broker KRaft mode is
  simplest — avoids needing a separate Zookeeper container).

### 4.4 Docker
- `Dockerfile` for the FastAPI service: multi-stage build, install deps,
  copy the joblib model + code, expose port, non-root user.
- `docker-compose.yml`: FastAPI service + Kafka broker + Kafka consumer
  worker as separate services.
- `.dockerignore`; pin dependency versions (don't float on `latest`).

### 4.5 Repository layout

```
fraud-detection-service/
├── notebooks/
│   └── YouVerify_Assessment_by_Chukwunonyelim_Okonji.ipynb   # original notebook, kept for reference
├── models/
│   └── xgb_fraud_pipeline.joblib                              # saved model, path is env-configurable
├── app/
│   ├── __init__.py
│   ├── main.py          # FastAPI app — /predict, /health
│   ├── schemas.py        # Pydantic request/response models
│   ├── features.py       # SINGLE source of truth for feature derivation — imported by app AND kafka_service, never duplicated
│   ├── model.py          # loads the joblib pipeline once at startup, wraps predict_proba
│   └── config.py         # env-driven settings: model path, threshold, kafka broker
├── kafka_service/
│   ├── __init__.py
│   ├── consumer.py       # subscribes to transactions.completed, scores via app/features.py + app/model.py, publishes fraud.alerts
│   └── producer.py       # dev-only: simulates transaction events for local testing
├── tests/                # empty for now — see Roadmap (§6)
│   └── __init__.py
├── Dockerfile             # single image for both the API and the consumer; docker-compose sets the entrypoint per service
├── docker-compose.yml     # api service + kafka broker + consumer worker
├── .dockerignore
├── .env.example
├── requirements.txt
├── agent.md
└── README.md
```

Notes for whoever builds this:
- Do not name the Kafka folder `kafka/` — it can shadow the `kafka-python`
  package on import. `kafka_service/` avoids that.
- `app/features.py` must be imported by both `app/main.py` and
  `kafka_service/consumer.py`. If the same derivation logic gets copy-pasted
  into both instead, the API and the stream consumer can silently diverge in
  their scoring — this is the most likely way this service breaks quietly.
- One `Dockerfile` is enough since the API and consumer share the same
  dependencies and code; `docker-compose.yml` differentiates them by startup
  command (`uvicorn app.main:app` vs `python -m kafka_service.consumer`).

---

## 5. Conventions to follow

- FastAPI + Pydantic for the API layer, Docker for packaging — consistent
  with how this project's owner already builds services.
- Model path and Kafka broker address configurable via environment
  variables, never hardcoded.
- Structured logging (not print statements) from both the API and the
  consumer, including the probability score for every prediction — needed
  later for threshold calibration and drift monitoring.

---

## 6. Roadmap (not this phase — future work)

- Unit tests (pytest) for the feature engineering module and the API
  contract — do this *before or alongside* CI/CD, not strictly after.
- CI/CD via GitHub Actions (lint, run tests, build image, push).
- AWS deployment (e.g. ECS/Fargate) once the containerized setup is
  verified locally.
- Model re-tuning with a proper imbalance-aware scoring metric
  (`average_precision` or `f1`) and an actual threshold sweep — separate
  workstream from the service build.

---

## 7. Open questions to confirm with Nonny before/while building

1. Confirm: is this service for **post-transaction monitoring** (default
   assumption above) or does he actually want **pre-transaction blocking**?
   This changes the Kafka schema and possibly requires model rework.
2. Exact filename/path of the saved joblib pipeline.
3. What does a real transaction event payload look like in his system —
   does it already carry both balance pairs, or only some of them?
4. Preferred Kafka client library and whether Kafka is self-hosted (Docker
   Compose) or a managed service for later deployment.