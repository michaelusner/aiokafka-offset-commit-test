import logging
from contextlib import asynccontextmanager

import pytest
import pytest_asyncio
from rugby_common.application_services.kafka import (
    KafkaConsumerClient,
    KafkaProducerClient,
)

pytestmark = pytest.mark.asyncio

logging.basicConfig(level=logging.INFO)

KAFKA_HOST = "localhost:9092"


@pytest_asyncio.fixture
async def producer(messages):
    prod = KafkaProducerClient(bootstrap_servers=[KAFKA_HOST])
    await prod.start()
    # add messages
    for _ in range(messages):
        await prod.send_and_wait(topic="test_topic", value="Test message".encode())
    yield prod
    await prod.stop()


@asynccontextmanager
async def get_consumer():
    cons = KafkaConsumerClient(
        "test_topic",
        group_id="group1",
        bootstrap_servers=[KAFKA_HOST],
        enable_auto_commit=False,
    )
    await cons.start()
    yield cons
    await cons.stop()


@asynccontextmanager
async def consumer_commit(client: KafkaConsumerClient):
    yield
    await client.commit()


@pytest.mark.parametrize("messages", [1])
async def test_commit_offset_without_context_should_not_be_updated(producer):
    async with get_consumer() as consumer:
        msg = await consumer.getone()

    async with get_consumer() as consumer:
        msg1 = await consumer.getone()
        assert msg1.offset == msg.offset, "Offsets without commit should be equal"


@pytest.mark.parametrize("messages", [1])
async def test_commit_offset_context_with_exception_should_be_updated(producer):
    async with get_consumer() as consumer:
        try:
            async with consumer_commit(consumer):
                msg = await consumer.getone()
                logging.info(msg.value)
                raise AssertionError("Intentional assert")
        except AssertionError:
            pass

    async with get_consumer() as consumer:
        msg1 = await consumer.getone()
        assert msg1.offset == msg.offset, "Offsets with exception should be equal"


@pytest.mark.parametrize("messages", [2])
async def test_commit_offset_context_should_be_updated(producer):
    async with get_consumer() as consumer:
        msg = await consumer.getone()
        async with consumer_commit(consumer):
            pass

    async with get_consumer() as consumer:
        msg1 = await consumer.getone()
        async with consumer_commit(consumer):
            pass
        assert msg1.offset == msg.offset + 1, "Offsets should not be equal"


@pytest.mark.parametrize("messages", [5])
async def test_commit_offset_context_should_be_updated_in_iterator(producer):
    offset = -1
    count = 0
    async with get_consumer() as consumer:
        async for msg in consumer:
            async with consumer_commit(consumer):
                if offset == -1:
                    offset = msg.offset
                else:
                    assert msg.offset == offset + 1, "Offset not incremented by one"
                    offset = msg.offset
                count += 1
                if count == 5:
                    break
