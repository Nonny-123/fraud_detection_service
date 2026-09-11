# Fraud detection service

Stage 1 makes the existing model runnable outside its training notebook.
Stage 2 adds shared feature engineering in `app/features.py`, following the
application layout in `AGENT.md`.
The project monitors completed transactions using their post-transaction balances.

## Python setup

Use Python 3.14.7. From this project directory:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/inspect_model.py
```

Dependencies are pinned to the versions used for successful model inspection.
The virtual environment keeps them separate from your global Python packages.
Run `deactivate` when finished.

## Model inspection

The script loads `models/fraud_xgb_model.joblib`, displays its pipeline, verifies
its six input columns and class labels, and scores one synthetic transaction.
The model path is resolved relative to the script, so it also works when launched
from another directory using the script's full path and the virtual environment's
Python executable.

Expected inputs: `step`, `type`, `amount`, `transaction_type`, `net_sender`,
and `net_receiver`. Classes are `0` (not fraud) and `1` (fraud).

The pipeline contains preprocessing, SMOTE, and XGBoost. Scaling and encoding
are already fitted inside the pipeline; SMOTE is a training step. Feature
derivation from raw transactions is provided by `app.features.build_features`.

The synthetic example should return a fraud probability of approximately
`0.04674535`, followed by `Inspection passed`. This is an execution check,
not a measurement of predictive accuracy. The original notebook and model
are preserved without retraining.

## Feature engineering

From the project directory with the virtual environment activated, run `python`
and try:

```python
from app.features import build_features

transaction = {
    "step": 1,
    "type": "TRANSFER",
    "amount": 100.0,
    "nameOrig": "C123",
    "nameDest": "C456",
    "oldbalanceOrg": 500.0,
    "newbalanceOrig": 400.0,
    "oldbalanceDest": 200.0,
    "newbalanceDest": 300.0,
}
features = build_features(transaction)
print(features.to_dict(orient="records"))
```

This returns one row with `step=1`, `type="TRANSFER"`, `amount=100.0`,
`transaction_type="CC"`, `net_sender=100.0`, and `net_receiver=100.0`.
The output columns follow the saved model's expected order.

`transaction_type` combines the first character of each account identifier.
`net_sender` is the sender's old balance minus its new balance; `net_receiver`
is the receiver's new balance minus its old balance. Negative differences are
preserved exactly as in training. The function leaves the input unchanged and
excludes extra fields, including labels and event identifiers, from its output.

The API and Kafka consumer will both import this function. Their schemas will
validate transaction fields before feature engineering; this module does not
load the model or perform scaling, encoding, or prediction.

## API schemas

`app.schemas.TransactionRequest` accepts the nine raw fields needed for a
completed transaction and rejects unknown fields, blank account/type strings,
negative values, non-finite numbers, and negative steps. Its `to_transaction()`
method returns the validated mapping consumed by `build_features`.

`app.schemas.PredictionResponse` defines the response contract: `is_fraud`, a
probability from `0` to `1`, and the threshold used for that decision. Both
schemas reject extra fields so malformed API payloads fail clearly.

## FastAPI endpoints

Start the development server from the project root (the directory containing
`app/`, `models/`, and `docker-compose.yml`) after activating the virtual
environment:

```sh
cd "/Users/user/Downloads/coding/fraud_detection_service"
source .venv/bin/activate
python -m uvicorn app.main:app --reload
```

Do not run `python3 main.py` from inside `app/`; that removes the project root
from Python's import path and causes `ModuleNotFoundError: No module named
app`. The `app.main:app` notation tells Uvicorn to import `main.py` as part of
the `app` package.

Open `http://127.0.0.1:8000/docs` for the interactive OpenAPI documentation.
`GET /health` returns `{"status":"ok"}` after the model has loaded. `POST
/predict` accepts the `TransactionRequest` JSON body and returns the validated
`PredictionResponse`. The model is loaded during application startup, so a
missing or invalid model prevents the service from accepting requests.

## Prediction logic

`app.config.Settings.from_environment()` reads `MODEL_PATH`,
`FRAUD_THRESHOLD`, and the Kafka topic settings. The default model path is
`models/fraud_xgb_model.joblib`, and the default threshold is `0.5`. The
threshold must be a finite number from `0` through `1`; it is an operational
decision setting and has not been calibrated yet.

`app.model.FraudModel.load(settings)` loads the joblib pipeline once. Its
`predict(transaction)` method calls `build_features`, reads the probability
for class `1`, and returns `is_fraud`, `fraud_probability`, and
`threshold_used`. `load_configured_model()` caches one loaded model per model
path and threshold, so later API requests and Kafka messages do not reload the
model.

To try a complete raw-transaction prediction:

```sh
python - <<'PY'
from app.model import load_configured_model

transaction = {
    "step": 1, "type": "TRANSFER", "amount": 100.0,
    "nameOrig": "C123", "nameDest": "C456",
    "oldbalanceOrg": 500.0, "newbalanceOrig": 400.0,
    "oldbalanceDest": 200.0, "newbalanceDest": 300.0,
}
print(load_configured_model().predict(transaction).as_dict())
PY
```

## Kafka broker

Kafka provides the event stream used by the monitoring service:

- A **topic** is a named append-only stream. This project uses
  `transactions.completed` for completed transaction events and `fraud.alerts`
  for predictions that cross the threshold.
- A **producer** writes events to a topic. The future development producer will
  publish completed transactions.
- A **consumer** reads events from a topic. The future worker will consume
  transactions, score them, and publish fraud alerts.

The Compose file runs one Kafka broker in KRaft mode, so a separate Zookeeper
container is not needed. It exposes `localhost:9092` for tools on the host and
advertises `kafka:29092` for services inside Compose. The `kafka-init` service
creates both required topics after the broker health check passes.

Docker must be installed and running. Start the broker and initialize topics:

```sh
docker compose up -d kafka kafka-init
docker compose ps
docker compose logs kafka-init
```

The init logs should list `transactions.completed` and `fraud.alerts`. Check
the topics directly from the broker container:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --list
```

Stop the broker with `docker compose down`. Add `-v` only when you intentionally
want to delete the local Kafka data volume and start with empty topics.

## Kafka transaction producer

With the broker running and the virtual environment activated, publish one
validated sample completed transaction from the project root:

```sh
python -m kafka_service.producer
```

The producer uses `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`) and
`KAFKA_TRANSACTIONS_TOPIC` (default `transactions.completed`). It validates the
sample using `TransactionRequest`, adds `transaction_id`, waits for Kafka's
acknowledgement, and then closes its connection. A future consumer will read
this event, score it through `app.model`, and publish qualifying results to
`fraud.alerts`.
