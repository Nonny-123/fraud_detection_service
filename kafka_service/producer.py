"""Development producer for completed transaction events."""

import asyncio
import json
import logging
from collections.abc import Mapping
from typing import Any

from aiokafka import AIOKafkaProducer

from app.config import Settings
from app.schemas import TransactionRequest


logger = logging.getLogger("fraud_detection.kafka.producer")


def build_event(transaction_id: str, transaction: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and build the JSON event sent to Kafka."""
    transaction_id = transaction_id.strip()
    if not transaction_id:
        raise ValueError("transaction_id must not be blank")
    validated = TransactionRequest.model_validate(transaction)
    return {"transaction_id": transaction_id, **validated.to_transaction()}


async def publish_event(
    transaction_id: str,
    transaction: Mapping[str, Any],
    settings: Settings | None = None,
) -> None:
    """Publish one validated completed transaction and wait for its ack."""
    settings = settings or Settings.from_environment()
    event = build_event(transaction_id, transaction)
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    await producer.start()
    try:
        metadata = await producer.send_and_wait(
            settings.transactions_topic,
            key=transaction_id,
            value=event,
        )
        logger.info(
            "Transaction event published",
            extra={
                "transaction_id": transaction_id,
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset,
            },
        )
    finally:
        await producer.stop()


SAMPLE_TRANSACTION = {
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


async def main() -> None:
    """Publish one sample event for local development."""
    await publish_event("sample-transaction-001", SAMPLE_TRANSACTION)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
