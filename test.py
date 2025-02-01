import asyncio
import random

from pydantic import BaseModel

from kafka import Kafka
from read.reader import Message

kafka = Kafka(
    order_processing=True,
)

class Model(BaseModel):
    value: int


@kafka.handler(["test-topic3", "test-topic4", "test-topic2", "test-topic"])
async def test2(value: Model, message: Message):
    print("TEST 66666")
    print(value)
    # await asyncio.sleep(.001)
    await asyncio.sleep(random.random())
    # print(1)
    # print(2)

# @kafka.handler(["test-topic3"])
# async def test2(message):
#     print("TEST 3333")
#     print(message)
#     # await asyncio.sleep(.001)
#     await asyncio.sleep(random.random())
#     # print(1)
#     # print(2)
#
# @kafka.handler(["test-topic4"])
# async def test2(message):
#     print("TEST 4444")
#     print(message)
#     # await asyncio.sleep(.001)
#     await asyncio.sleep(random.random())
#     # print(1)
#     # print(2)
#
# # @kafka.handler(["test-topic3", "test-topic4", "test-topic2", "test-topic"])
# # def sync_test2(msg):
# #     print(msg)
# #     # await asyncio.sleep(.001)
# #     time.sleep(random.random())
# #     # print(1)
# #     # print(2)
# #
# #
# @kafka.handler(["test-topic2"])
# async def test2(message):
#     print("TEST 2222")
#     print(message)
#     # await asyncio.sleep(.001)
#     await asyncio.sleep(random.random())
#     # print(1)
#     # print(2)
#
#     # publish_tasks = []
#     # #
#     # for i in range(100):
#     #     value = f"msg-{i}"
#     #     publish_tasks.append(
#     #         kafka.publish(
#     #             Message(
#     #                 topic="test-topic3",
#     #                 value=value.encode(),
#     #             )
#     #         )
#     #     )
#     #
#     # await asyncio.gather(*publish_tasks)
#
#
# @kafka.handler(["test-topic"])
# async def test(message):
#     print("TEST 1111")
#     print(message)
#     # await asyncio.sleep(.001)
#     await asyncio.sleep(random.random())
#     # print(6)
#
#     publish_tasks = []
#
#     # for i in range(1000):
#     #     value = f"msg-{i}"
#     #     publish_tasks.append(
#     #         kafka.publish(
#     #             Message(
#     #                 topic="test-topic2",
#     #                 value=value.encode(),
#     #             )
#     #         )
#     #     )
#     #
#     # await asyncio.gather(*publish_tasks)


async def main():
    await kafka.run()


if __name__ == "__main__":
    asyncio.run(main())
