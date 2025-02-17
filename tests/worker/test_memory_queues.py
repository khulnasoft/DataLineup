import asyncstdlib as alib
import pytest

from datalineup_engine.core import TopicMessage
from datalineup_engine.worker.topics.memory import MemoryOptions
from datalineup_engine.worker.topics.memory import MemoryTopic
from datalineup_engine.worker.topics.memory import join_all


@pytest.mark.asyncio
async def test_memory_queues() -> None:
    queue1 = MemoryTopic(MemoryOptions(name="test-1"))
    queue2 = MemoryTopic(MemoryOptions(name="test-2"))
    publisher1 = MemoryTopic(MemoryOptions(name="test-1"))
    publisher2 = MemoryTopic(MemoryOptions(name="test-2"))

    queue1generator = queue1.run()
    for i in range(10):
        await publisher1.publish(TopicMessage(args={"id": i}), wait=True)
        await publisher2.publish(TopicMessage(args={"id": i}), wait=True)
        processable = await alib.anext(queue1generator)
        async with processable as message:
            assert message.args["id"] == i

    queue2generator = queue2.run()
    for i in range(10):
        processable = await alib.anext(queue2generator)
        async with processable as message:
            assert message.args["id"] == i

    await queue1generator.aclose()
    await queue2generator.aclose()
    await join_all()
