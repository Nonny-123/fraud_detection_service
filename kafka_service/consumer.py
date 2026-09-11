"""Kafka worker that scores completed transactions and emits fraud alerts."""

import asyncio
import json
import logging
from collections.abc import Mapping
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import ValidationError

from app.config import Settings
from app.model import FraudModel, load_configured_model
from app.schemas import TransactionRequest

logger = logging.getLogger("fraud_detection.kafka.consumer")
CONSUMER_GROUP = "fraud-detection-worker"


def _transaction_from_event(event: Mapping[str, Any]) -> tuple[str, TransactionRequest]:
    transaction_id = event.get("transaction_id")
    if not isinstance(transaction_id, str) or not transaction_id.strip():
        raise ValueError("event transaction_id must be a nonblank string")
    payload = {key: value for key, value in event.items() if key != "transaction_id"}
    return transaction_id.strip(), TransactionRequest.model_validate(payload)


async def run_consumer(settings: Settings | None = None) -> None:
    """Consume, score, alert, and commit completed transaction events."""
    settings = settings or Settings.from_environment()
    model: FraudModel = load_configured_model(settings)
    consumer = AIOKafkaConsumer(
        settings.transactions_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    await consumer.start()
    await producer.start()
    logger.info("Fraud consumer started", extra={"topic": settings.transactions_topic})
    try:
        async for message in consumer:
            try:
                event = json.loads(message.value.decode("utf-8"))
                transaction_id, transaction = _transaction_from_event(event)
                prediction = model.predict(transaction.to_transaction())
                if prediction.is_fraud:
                    alert = {"transaction_id": transaction_id, **prediction.as_dict()}
                    await producer.send_and_wait(
                        settings.alerts_topic,
                        key=transaction_id,
                        value=alert,
                    )
                    logger.info(
                        "Fraud alert published",
                        extra={"transaction_id": transaction_id, **prediction.as_dict()},
                    )
                await consumer.commit()
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError, ValidationError) as exc:
                logger.warning("Skipping malformed transaction event", extra={"error": str(exc)})
                await consumer.commit()
            except Exception:
                logger.exception("Transaction event processing failed")
                raise
    finally:
        await consumer.stop()
        await producer.stop()
        logger.info("Fraud consumer stopped")


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_consumer())


if __name__ == "__main__":
    main()
