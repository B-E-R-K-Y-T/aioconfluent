import asyncio
import random
import time

from pydantic import BaseModel

from kafka import Kafka
from read.config import KafkaConfig, ReaderConfig
from read.reader import Message

kafka = Kafka(
    order_processing=False,
    kafka_config=KafkaConfig(
        reader=ReaderConfig(
            max_poll_records=500,
        )
    ),
)


class Model(BaseModel):
    body: str


@kafka.handler(["test-topic3", "test-topic4"])
async def test2(value: Model, message: Message):
    print("TEST 66666")
    print(value.body)
    print(message.value)
    print("ОБРАБАТЫВАЕМ ТОПИКЖ: ", message.topic)
    # await asyncio.sleep(.001)
    await asyncio.sleep(random.random())

    # if random.random() > 0.5:
    #     await kafka.publish(Message(topic="test-topic", value=value.body.encode()))
    #     await kafka.publish(Message(topic="test-topic2", value=value.body.encode()))
    #     await kafka.publish(Message(topic="test-topic3", value=value.body.encode()))
    #     await kafka.publish(Message(topic="test-topic4", value=value.body.encode()))

    print(1)
    print(2)


@kafka.handler(["test-topic3"])
async def test2(value: Model, message: Message):
    print("TEST 3333")
    print(message)
    await asyncio.sleep(0.001)
    # await asyncio.sleep(random.random())
    # print(1)
    # print(2)


@kafka.handler(["test-topic4"])
async def test2(value: Model, message: Message):
    print("TEST 4444")
    print(message)
    # await asyncio.sleep(.001)
    await asyncio.sleep(random.random())

    # if random.random() > 0.5:
    #     await kafka.publish(Message(topic="test-topic", value=value.body.encode()))
    # print(1)
    # print(2)


# @kafka.handler(["test-topic3", "test-topic4", "test-topic2", "test-topic"])
# def sync_test2(msg):
#     print(msg)
#     # await asyncio.sleep(.001)
#     time.sleep(random.random())
#     # print(1)
#     # print(2)
#
#
@kafka.handler(["test-topic2"])
async def test2(value: Model, message: Message):
    print("TEST 2222")
    print(message)
    # await asyncio.sleep(.001)
    await asyncio.sleep(random.random())
    # if random.random() > 0.5:
    #     await kafka.publish(Message(topic="test-topic", value=value.body.encode()))
    # print(1)
    # print(2)

    # publish_tasks = []
    # #
    # for i in range(100):
    #     value = f"msg-{i}"
    #     publish_tasks.append(
    #         kafka.publish(
    #             Message(
    #                 topic="test-topic3",
    #                 value=value.encode(),
    #             )
    #         )
    #     )
    #
    # await asyncio.gather(*publish_tasks)


@kafka.handler(["test-topic"])
async def test(value: Model, message: Message):
    print("TEST 1111")
    print(message)
    # await asyncio.sleep(.001)
    await asyncio.sleep(random.random())
    # print(6)

    publish_tasks = []

    # for i in range(1000):
    #     value = f"msg-{i}"
    #     publish_tasks.append(
    #         kafka.publish(
    #             Message(
    #                 topic="test-topic2",
    #                 value=value.encode(),
    #             )
    #         )
    #     )
    #
    # await asyncio.gather(*publish_tasks)


# @kafka.handler(["test-topic"])
# def test2(value: Model, message: Message):
#     print("TEST 1")
#     print(message)
#     time.sleep(random.random())
#
# @kafka.handler(["test-topic2"])
# def test2(value: Model, message: Message):
#     print("TEST 2")
#     print(message)
#     time.sleep(random.random())
#
# @kafka.handler(["test-topic3"])
# def test2(value: Model, message: Message):
#     print("TEST 3")
#     print(message)
#     time.sleep(random.random())
#
# @kafka.handler(["test-topic4"])
# def test2(value: Model, message: Message):
#     print("TEST 4")
#     print(message)
#     time.sleep(random.random())


async def main():
    await kafka.run()


if __name__ == "__main__":
    asyncio.run(main())
