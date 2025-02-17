import typing as t
from typing import Callable
from typing import cast

import asyncio
from functools import partial
from unittest.mock import AsyncMock

import pytest

from datalineup_engine.core import PipelineInfo
from datalineup_engine.core import PipelineOutput
from datalineup_engine.core import PipelineResults
from datalineup_engine.core import TopicMessage
from datalineup_engine.core.api import ComponentDefinition
from datalineup_engine.core.api import ErrorHandler
from datalineup_engine.core.api import RepublishOptions
from datalineup_engine.core.error import ErrorMessageArgs
from datalineup_engine.worker.error_handling import HandledError
from datalineup_engine.worker.executors import Executor
from datalineup_engine.worker.executors.executable import ExecutableMessage
from datalineup_engine.worker.executors.parkers import Parkers
from datalineup_engine.worker.executors.queue import ExecutorQueue
from datalineup_engine.worker.resources.manager import ResourceData
from datalineup_engine.worker.topics.memory import MemoryTopic
from datalineup_engine.worker.topics.memory import get_queue
from tests.utils import TimeForwardLoop
from tests.worker.conftest import FakeResource


class FakeExecutor(Executor):
    concurrency = 1

    def __init__(self) -> None:
        self.execute_semaphore = asyncio.Semaphore(0)
        self.processing = 0
        self.processed = 0

    async def process_message(self, message: ExecutableMessage) -> PipelineResults:
        self.processing += 1
        await self.execute_semaphore.acquire()
        self.processed += 1
        return PipelineResults(
            outputs=[
                PipelineOutput(
                    channel="default", message=TopicMessage(args={"n": self.processed})
                )
            ],
            resources=[],
        )


class FakeFailingExecutor(FakeExecutor):
    async def process_message(self, message: ExecutableMessage) -> PipelineResults:
        self.processed += 1
        try:
            try:
                raise ValueError("CONTEXT")
            except Exception:
                cause = None
                try:
                    try:
                        raise ValueError("CAUSE_CONTEXT") from None
                    except Exception:
                        raise ValueError("CAUSE") from None
                except Exception as e:
                    cause = e
                raise Exception("TEST_EXCEPTION") from cause
        except Exception as e:
            self.error = e
            raise


@pytest.mark.asyncio
async def test_base_executor(
    executable_maker: Callable[[], ExecutableMessage],
    running_event_loop: TimeForwardLoop,
    executor_queue_maker: Callable[..., ExecutorQueue],
) -> None:
    executor = FakeExecutor()
    executor.concurrency = 5
    executor_manager = executor_queue_maker(executor=executor)

    async with running_event_loop.until_idle():
        for _ in range(10):
            asyncio.create_task(executor_manager.submit(executable_maker()))

    assert executor.processing == 5
    assert executor.processed == 0

    async with running_event_loop.until_idle():
        for _ in range(10):
            executor.execute_semaphore.release()
    assert executor.processed == 10


def pipeline(resource: FakeResource) -> None: ...


@pytest.mark.asyncio
async def test_executor_wait_resources_and_queue(
    executable_maker: Callable[..., ExecutableMessage],
    running_event_loop: TimeForwardLoop,
    executor_queue_maker: Callable[..., ExecutorQueue],
) -> None:
    executor = FakeExecutor()
    executor_manager = executor_queue_maker(executor=executor)
    await executor_manager.resources_manager.add(
        ResourceData(name="r1", type=FakeResource._typename(), data={})
    )
    await executor_manager.resources_manager.add(
        ResourceData(name="r2", type=FakeResource._typename(), data={})
    )
    parker = Parkers()
    executable_maker = partial(
        executable_maker,
        pipeline_info=PipelineInfo.from_pipeline(pipeline),
        parker=parker,
    )

    # Set up a scenario where there's 2 resource and 1 executor slot.
    # Queuing 3 items should have 1 waiting on the executor and 1 waiting on
    # the resources.
    async with running_event_loop.until_idle():
        for _ in range(2):
            await executor_manager.submit(executable_maker())

    assert executor.processing == 1
    assert not parker.locked()

    # Submit another task, stuck locking a resource, park the processable.
    async with running_event_loop.until_idle():
        await executor_manager.submit(executable_maker())

    assert executor.processing == 1
    assert parker.locked()

    # Process the task pending in the executor and release the resource.
    async with running_event_loop.until_idle():
        executor.execute_semaphore.release()
    assert executor.processed == 1
    assert executor.processing == 2
    assert not parker.locked()

    # Process the other task, release the resource.
    async with running_event_loop.until_idle():
        executor.execute_semaphore.release()
    assert executor.processed == 2
    assert executor.processing == 3
    assert not parker.locked()

    async with running_event_loop.until_idle():
        executor.execute_semaphore.release()


@pytest.mark.asyncio
async def test_executor_wait_pusblish_and_queue(
    executable_maker: Callable[..., ExecutableMessage],
    running_event_loop: TimeForwardLoop,
    executor_queue_maker: Callable[..., ExecutorQueue],
) -> None:
    executor = FakeExecutor()
    executor_manager = executor_queue_maker(executor=executor)
    await executor_manager.resources_manager.add(
        ResourceData(name="r1", type=FakeResource._typename(), data={})
    )
    await executor_manager.resources_manager.add(
        ResourceData(name="r2", type=FakeResource._typename(), data={})
    )
    output_queue = get_queue("q1", maxsize=1)
    output_topic = MemoryTopic(MemoryTopic.Options(name="q1"))
    parker = Parkers()
    executable_maker = partial(
        executable_maker,
        pipeline_info=PipelineInfo.from_pipeline(pipeline),
        parker=parker,
        output={"default": [output_topic]},
    )

    # Set up a scenario where there's 2 task, 1 executor slot and 1 publish slot.
    # Queuing 2 items should have 1 waiting on the executor and 1 waiting on publish
    # the resources.
    async with running_event_loop.until_idle():
        for _ in range(2):
            await executor_manager.submit(executable_maker())

    assert executor.processing == 1
    assert executor.processed == 0
    assert output_queue.qsize() == 0
    assert not parker.locked()

    # Process one task, take publish slot.
    async with running_event_loop.until_idle():
        executor.execute_semaphore.release()

    assert executor.processing == 2
    assert executor.processed == 1
    assert output_queue.qsize() == 1
    assert not parker.locked()

    # Process the other task, get stuck on publishing
    async with running_event_loop.until_idle():
        executor.execute_semaphore.release()

    assert executor.processing == 2
    assert executor.processed == 2
    assert output_queue.qsize() == 1
    assert parker.locked()

    # Pop the item in the publish queue, leaving room for the next item.
    async with running_event_loop.until_idle():
        assert output_queue.get_nowait().args == {"n": 1}

    assert executor.processing == 2
    assert executor.processed == 2
    assert output_queue.qsize() == 1
    assert not parker.locked()

    # Pop the other item in the publish queue, clearing the queue.
    async with running_event_loop.until_idle():
        assert output_queue.get_nowait().args == {"n": 2}

    assert executor.processing == 2
    assert executor.processed == 2
    assert output_queue.qsize() == 0
    assert not parker.locked()


@pytest.mark.asyncio
async def test_executor_error_handler(
    fake_executable_maker_with_output: Callable[..., ExecutableMessage],
    running_event_loop: TimeForwardLoop,
    executor_queue_maker: Callable[..., ExecutorQueue],
) -> None:
    executor = FakeFailingExecutor()
    executor.concurrency = 1
    executor_manager = executor_queue_maker(executor=executor)
    output_queue = get_queue("q1", maxsize=1)
    output_topics = {
        "error:TEST_EXCEPTION:Exception": [
            ComponentDefinition(
                "q1",
                "MemoryTopic",
            )
        ]
    }
    xmsg = fake_executable_maker_with_output(
        output=output_topics,
    )
    exc_infos = []

    async def collect_exit(*args: t.Any) -> None:
        exc_infos.append(args)

    xmsg._executing_context.push_async_exit(collect_exit)

    # Execute our failing message
    async with running_event_loop.until_idle():
        asyncio.create_task(executor_manager.submit(xmsg))

    # Our pipeline should cause a test exception and publish it in its channel
    assert output_queue.qsize() == 1
    output: TopicMessage = output_queue.get_nowait()

    # We can't really assert the traceback and id fields
    # so we just copy them from our output
    expected_message = TopicMessage(
        id=output.id,
        args={
            "cause": xmsg.message.message,
            "error": ErrorMessageArgs(
                type="builtins.Exception",
                module="tests.worker.test_executor",
                message="TEST_EXCEPTION",
                traceback=cast(ErrorMessageArgs, output.args["error"]).traceback,
            ),
        },
        config={},
        metadata={},
        tags={},
    )

    assert expected_message == output
    assert [e for e, *_ in exc_infos] == [HandledError]


@pytest.mark.asyncio
async def test_executor_error_handler_unhandled(
    fake_executable_maker_with_output: Callable[..., ExecutableMessage],
    running_event_loop: TimeForwardLoop,
    executor_queue_maker: Callable[..., ExecutorQueue],
) -> None:
    executor = FakeFailingExecutor()
    executor.concurrency = 1
    executor_manager = executor_queue_maker(executor=executor)
    output_queue = get_queue("q1", maxsize=1)
    output_topics = {
        "error:TEST_EXCEPTION:Exception": [
            ComponentDefinition(
                "q1",
                "MemoryTopic",
            ),
            ErrorHandler(
                set_handled=False,
            ),
        ]
    }
    xmsg = fake_executable_maker_with_output(
        output=output_topics,
    )

    mock = AsyncMock()
    xmsg._executing_context.push_async_exit(mock.executing)
    xmsg._context.push_async_exit(mock.context)

    # Execute our failing message
    async with running_event_loop.until_idle():
        asyncio.create_task(executor_manager.submit(xmsg))

    # A message is outputed to error topic.
    assert output_queue.qsize() == 1

    # The exception raised to the hooks and contexts is the original one.
    mock.executing.__aexit__.assert_awaited_once()
    mock.context.__aexit__.assert_awaited_once()
    e = mock.executing.__aexit__.call_args[0][2]
    assert e is mock.context.__aexit__.call_args[0][2]
    assert repr(e) == "Exception('TEST_EXCEPTION')"
    assert repr(e.__cause__) == "ValueError('CAUSE')"
    assert repr(e.__cause__.__context__) == "ValueError('CAUSE_CONTEXT')"
    assert repr(e.__context__) == "ValueError('CONTEXT')"


@pytest.mark.asyncio
async def test_executor_error_handler_republish(
    fake_executable_maker_with_output: Callable[..., ExecutableMessage],
    running_event_loop: TimeForwardLoop,
    executor_queue_maker: Callable[..., ExecutorQueue],
) -> None:
    executor = FakeFailingExecutor()
    executor.concurrency = 1
    executor_manager = executor_queue_maker(executor=executor)
    output_queue = get_queue("q1", maxsize=1)
    retry_queue = get_queue("q2", maxsize=1)
    output_topics = {
        "error:TEST_EXCEPTION:Exception": [
            ComponentDefinition(
                "q1",
                "MemoryTopic",
            ),
            ErrorHandler(
                republish=RepublishOptions(
                    channel="retry",
                    max_retry=1,
                )
            ),
        ],
        "retry": [
            ComponentDefinition(
                "q2",
                "MemoryTopic",
            ),
        ],
    }
    exc_infos = []

    async def collect_exit(*args: t.Any) -> None:
        exc_infos.append(args)

    xmsg = fake_executable_maker_with_output(
        output=output_topics,
    )
    xmsg._executing_context.push_async_exit(collect_exit)

    # Execute our failing message
    async with running_event_loop.until_idle():
        asyncio.create_task(executor_manager.submit(xmsg))

    # The error should be republished to the `retry` channel.
    assert output_queue.qsize() == 1
    output_queue.get_nowait()
    assert retry_queue.qsize() == 1
    retry_message = retry_queue.get_nowait()
    assert retry_message.args == {}
    assert [e for e, *_ in exc_infos] == [HandledError]
    exc_infos.clear()

    # Execute the retry message
    xmsg = fake_executable_maker_with_output(
        message=retry_message,
        output=output_topics,
    )
    xmsg._executing_context.push_async_exit(collect_exit)
    async with running_event_loop.until_idle():
        asyncio.create_task(executor_manager.submit(xmsg))

    # The error should reach max retry and not republish.
    # The original error is reraised unhandled.
    assert output_queue.qsize() == 1
    output_queue.get_nowait()
    assert retry_queue.qsize() == 0
    assert len(exc_infos) == 1
    assert repr(exc_infos[0][1]) == "Exception('TEST_EXCEPTION')"
