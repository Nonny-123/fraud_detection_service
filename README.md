# Fraud Detection Service

Post-transaction fraud monitoring for completed financial transactions. The service derives the features expected by a trained XGBoost pipeline, returns a fraud probability through an API, and can score transaction events from Kafka to publish fraud alerts.

> This project supports monitoring after a transaction completes. It is not designed to block transactions before approval because its model requires post-transaction account balances.

## Key features

- Scores completed transactions with a saved XGBoost fraud-detection pipeline.
- Provides a FastAPI `POST /predict` endpoint and interactive OpenAPI documentation.
- Uses one shared feature-engineering module for API and Kafka scoring.
- Consumes `transactions.completed` events and publishes threshold-crossing predictions to `fraud.alerts`.
- Runs the API, Kafka broker, topic initialization, and consumer locally with Docker Compose.
- Configures the model location, fraud threshold, Kafka broker, and topics through environment variables.

## Tech stack

- **Language:** Python 3.14
- **API:** FastAPI, Pydantic, Uvicorn
- **Machine learning:** XGBoost, scikit-learn, imbalanced-learn, pandas, joblib
- **Event streaming:** Apache Kafka in KRaft mode, aiokafka
- **Containers:** Docker and Docker Compose

## Prerequisites

- Git
- Docker Desktop with Docker Compose (recommended for the full stack)
- Python 3.14.7 (for local development and the sample Kafka producer)

The repository includes the trained model at `models/fraud_xgb_model.joblib`; no external API keys, database, or cloud account are required for local use.

## Installation and setup

Clone the repository and enter it:

```sh
git clone https://github.com/Nonny-123/fraud_detection_service.git
cd fraud_detection_service
```

### Run the complete stack with Docker

Build the image and start Kafka, the topic initializer, API, and consumer:

```sh
docker compose up --build -d
docker compose ps
```

Open the interactive API documentation at [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs). Stop the local stack when finished:

```sh
docker compose down
```

Do not use `docker compose down -v` unless you intend to delete the local Kafka data volume.

### Local Python environment

Use this when running the model inspection script, API without Docker, or the sample Kafka producer from your host machine:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/inspect_model.py
```

Start the API outside Docker:

```sh
python -m uvicorn app.main:app --reload
```

## Usage

Check that the containerized API is ready:

```sh
curl http://127.0.0.1:8000/health
```

Expected response:

```json
{"status":"ok"}
```

Score one completed transaction:

```sh
curl -X POST http://127.0.0.1:8000/predict \
  -H 'content-type: application/json' \
  -d '{
    "step": 1,
    "type": "TRANSFER",
    "amount": 100.0,
    "nameOrig": "C123",
    "nameDest": "C456",
    "oldbalanceOrg": 500.0,
    "newbalanceOrig": 400.0,
    "oldbalanceDest": 200.0,
    "newbalanceDest": 300.0
  }'
```

Example response:

```json
{
  "is_fraud": false,
  "fraud_probability": 0.04674535244703293,
  "threshold_used": 0.5
}
```

To publish the included sample event to Kafka, ensure the Docker stack is running, activate the virtual environment, then run:

```sh
python -m kafka_service.producer
docker compose logs consumer
```

## Architecture

```text
                 ┌───────────────────────┐
                 │ Completed transaction │
                 └──────────┬────────────┘
                            │
              ┌─────────────┴──────────────┐
              ▼                            ▼
  ┌──────────────────────┐     ┌────────────────────────────────┐
  │ FastAPI POST /predict │     │ Kafka: transactions.completed  │
  └──────────┬───────────┘     └───────────────┬────────────────┘
             │                                 │
             ▼                                 ▼
  ┌──────────────────────┐     ┌────────────────────────────────┐
  │ Shared feature       │◄────│ Kafka consumer                 │
  │ engineering          │     └───────────────┬────────────────┘
  └──────────┬───────────┘                     │
             │                                 ▼
             ▼                    ┌──────────────────────────────┐
  ┌──────────────────────┐        │ XGBoost model pipeline       │
  │ JSON prediction      │        └───────────────┬──────────────┘
  └──────────────────────┘                        │ threshold crossed
                                                   ▼
                                    ┌──────────────────────────────┐
                                    │ Kafka: fraud.alerts          │
                                    └──────────────────────────────┘
```

The shared feature module derives `transaction_type`, `net_sender`, and `net_receiver` before prediction. Scaling and encoding are already stored inside the trained pipeline.

## Environment variables

| Variable | Purpose | Default | Required |
| --- | --- | --- | --- |
| `MODEL_PATH` | Path to the saved joblib model. | `models/fraud_xgb_model.joblib` locally; `/srv/app/models/fraud_xgb_model.joblib` in Compose | No |
| `FRAUD_THRESHOLD` | Probability from `0` to `1` used to classify an event as fraud. | `0.5` | No |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address. | `localhost:9092` locally; `kafka:29092` in Compose | No |
| `KAFKA_TRANSACTIONS_TOPIC` | Topic consumed for completed transactions. | `transactions.completed` | No |
| `KAFKA_ALERTS_TOPIC` | Topic that receives fraud alerts. | `fraud.alerts` | No |

`FRAUD_THRESHOLD=0.5` is an operational default, not a calibrated business threshold. The current model is intended to surface transactions for investigation and may generate false positives.

## API reference

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Returns `{"status":"ok"}` after the model loads. |
| `POST` | `/predict` | Scores one validated completed transaction. |

`POST /predict` requires `step`, `type`, `amount`, `nameOrig`, `nameDest`, `oldbalanceOrg`, `newbalanceOrig`, `oldbalanceDest`, and `newbalanceDest`. Numeric amounts and balances must be non-negative; account identifiers and transaction type must not be blank. Unknown fields are rejected.

For a complete interactive schema and response documentation, run the API and open [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs).

## Contributing

1. Fork the repository and create a focused branch.
2. Make and manually verify your change locally with Docker Compose.
3. Open a pull request that explains the change, its validation, and any configuration impact.

Please do not commit secrets, local environment files, generated Kafka data, or a replacement model without documenting its training and evaluation.

## License

This project is licensed under the [MIT License](LICENSE).
