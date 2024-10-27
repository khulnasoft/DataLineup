import typing as t

import asyncio
import concurrent.futures
import threading

import pytest

from datalineup_engine.client.datalineup import DatalineupClient
from datalineup_engine.client.datalineup import SyncDatalineupClient
from datalineup_engine.config import Config
from datalineup_engine.core import TopicMessage
from datalineup_engine.utils.inspect import get_import_name
from datalineup_engine.worker.topic import Topic
from datalineup_engine.worker.topics.memory import MemoryTopic
from datalineup_engine.worker.topics.memory import get_queue
from datalineup_engine.worker_manager.config.declarative import load_definitions_from_str
from datalineup_engine.worker_manager.config.static_definitions import StaticDefinitions
from tests.utils import HttpClientMock


class DelayedMemoryTopic(MemoryTopic):
    published_event: threading.Event
    publishing_event: asyncio.Event

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        self.published_event = threading.Event()

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        self.publishing_event = asyncio.Event()
        asyncio.create_task(self.delayed_publish(message, wait))
        return True

    async def delayed_publish(self, message: TopicMessage, wait: bool) -> None:
        await self.publishing_event.wait()
        await super().publish(message, wait)
        self.published_event.set()


class HangingTopic(Topic):
    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        self.publish_done = threading.Event()
        self.publish_result: t.Any = None

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        try:
            await asyncio.Event().wait()
        except BaseException as e:  # noqa: B036
            self.publish_result = e
        finally:
            self.publish_done.set()
        return True


def test_datalineup_client_publish_sync(
    config: Config,
    http_client_mock: HttpClientMock,
    static_definitions: StaticDefinitions,
) -> None:
    http_client_mock.get("http://127.0.0.1:5000/api/topics").return_value = {
        "items": [
            {
                "name": "test-topic",
                "options": {},
                "type": get_import_name(DelayedMemoryTopic),
            },
            {
                "name": "hanging-topic",
                "options": {},
                "type": get_import_name(HangingTopic),
            },
        ]
    }

    datalineup_client = SyncDatalineupClient.from_config(
        config=config,
        http_client=http_client_mock.client(),
    )
    assert datalineup_client.publish("test-topic", TopicMessage({"a": 0}), True)
    queue = get_queue("test-topic")
    assert queue.qsize() == 0

    topic = t.cast(DelayedMemoryTopic, datalineup_client._client.topics["test-topic"])

    async def set_event() -> None:
        topic.publishing_event.set()

    datalineup_client._run_sync(set_event())
    topic.published_event.wait()

    assert queue.get_nowait().args["a"] == 0
    queue.task_done()
    assert queue.qsize() == 0

    # Publish on blocking topic should cancel their async task.
    with pytest.raises(concurrent.futures.TimeoutError):
        datalineup_client.publish("hanging-topic", TopicMessage({"a": 0}), True, timeout=1)

    hanging_topic = t.cast(HangingTopic, datalineup_client._client.topics["hanging-topic"])
    hanging_topic.publish_done.wait()
    assert isinstance(hanging_topic.publish_result, asyncio.CancelledError)

    # Publish on invalid topic fail.
    with pytest.raises(KeyError):
        datalineup_client.publish("test-topic2", TopicMessage({"a": 0}), True)
    assert queue.qsize() == 0

    datalineup_client.close()
    assert not datalineup_client._loop_thread.is_alive()


@pytest.mark.asyncio
async def test_datalineup_client_publish_async(
    config: Config,
    http_client_mock: HttpClientMock,
    static_definitions: StaticDefinitions,
) -> None:
    http_client_mock.get("http://127.0.0.1:5000/api/topics").return_value = {
        "items": [{"name": "test-topic", "options": {}, "type": "MemoryTopic"}]
    }
    static_definitions.topics = load_definitions_from_str(
        """
apiVersion: datalineup.khulnasoft.io/v1alpha1
kind: DatalineupTopic
metadata:
  name: test-topic
spec:
  type: MemoryTopic
  options: {}
---
"""
    ).topics

    datalineup_client = await DatalineupClient.from_config(
        config,
        http_client=http_client_mock.client(),
    )
    assert await datalineup_client.publish("test-topic", TopicMessage({"a": 0}), True)

    with pytest.raises(KeyError):
        assert await datalineup_client.publish("test-topic2", TopicMessage({"a": 0}), True)

    queue = get_queue("test-topic")
    assert queue.get_nowait().args["a"] == 0
    queue.task_done()
    assert queue.qsize() == 0
    await datalineup_client.close()
