import logging
from contextlib import asynccontextmanager

import pytest
import pytest_asyncio
from aiokafka import TopicPartition
from rugby_common.application_services.kafka import (
    KafkaConsumerClient,
    KafkaProducerClient,
)

pytestmark = pytest.mark.asyncio

logging.basicConfig(level=logging.INFO)


@pytest_asyncio.fixture
async def producer():
    prod = KafkaProducerClient(bootstrap_servers=["localhost:9092"])
    await prod.start()
    yield prod
    await prod.stop()


@pytest_asyncio.fixture
async def consumer():
    con = KafkaConsumerClient(
        "test_topic",
        group_id="group1",
        bootstrap_servers=["localhost:9092"],
        enable_auto_commit=False,
    )
    await con.start()
    yield con
    await con.stop()


@asynccontextmanager
async def consumer_commit(client: KafkaConsumerClient):
    yield
    await client.commit()


async def test_commit(producer):
    # add some messages
    for i in range(1, 11):
        await producer.send_and_wait(
            topic="test_topic", value=f"Test message {i}".encode()
        )

    consumer = KafkaConsumerClient(
        "test_topic",
        group_id="group1",
        bootstrap_servers=["localhost:9092"],
        enable_auto_commit=False,
    )
    await consumer.start()

    # read 5 messages but don't commit offset
    async for msg in consumer:
        logging.info(msg)
        if "5" in msg.value.decode():
            break
    await consumer.stop()

    # start another consumer
    consumer = KafkaConsumerClient(
        "test_topic",
        group_id="group1",
        bootstrap_servers=["localhost:9092"],
        enable_auto_commit=False,
    )
    await consumer.start()
    # read 5 messages and note offset wasn't committed
    async for msg in consumer:
        async with consumer_commit(consumer):
            logging.info(msg)
            if "5" in msg.value.decode():
                break
    await consumer.stop()

    # start another consumer
    consumer = KafkaConsumerClient(
        "test_topic",
        group_id="group1",
        bootstrap_servers=["localhost:9092"],
        enable_auto_commit=False,
    )
    await consumer.start()
    # read 5 messages and note offset WAS committed
    async for msg in consumer:
        async with consumer_commit(consumer):
            logging.info(msg)
            if "10" in msg.value.decode():
                break
    await consumer.stop()
