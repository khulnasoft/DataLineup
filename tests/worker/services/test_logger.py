import typing as t

import dataclasses
import logging
from datetime import timedelta

import pytest

from datalineup_engine.core import MessageId
from datalineup_engine.core import PipelineInfo
from datalineup_engine.core import PipelineOutput
from datalineup_engine.core import PipelineResults
from datalineup_engine.core import Resource
from datalineup_engine.core import ResourceUsed
from datalineup_engine.core import TopicMessage
from datalineup_engine.worker.executors.executable import ExecutableMessage
from datalineup_engine.worker.services.loggers.logger import Logger
from datalineup_engine.worker.services.manager import ServicesManager
from tests.conftest import FreezeTime


@dataclasses.dataclass(eq=False)
class FakeResource(Resource):
    data: str


def fake_pipeline(x: int, r: FakeResource) -> None:
    pass


@pytest.mark.asyncio
async def test_logger_message_executed(
    services_manager: ServicesManager,
    caplog: t.Any,
    frozen_time: FreezeTime,
    executable_maker: t.Callable[..., ExecutableMessage],
) -> None:
    logger = services_manager.services.cast_service(Logger)

    pipeline_info = PipelineInfo.from_pipeline(fake_pipeline)
    xmsg = executable_maker(pipeline_info=pipeline_info)
    xmsg.message.message = TopicMessage(id=MessageId("m1"), args={"x": 42})
    xmsg.message.update_with_resources(
        {FakeResource._typename(): {"name": "r1", "data": "foobar"}}
    )

    await services_manager.services.s.hooks.message_polled.emit(xmsg)

    results = PipelineResults(
        outputs=[
            PipelineOutput(
                channel="default",
                message=TopicMessage(id=MessageId("m2"), args={"foo": "bar"}),
            )
        ],
        resources=[ResourceUsed(type=FakeResource._typename(), release_at=10)],
    )

    with caplog.at_level(logging.DEBUG):
        hook_generator = logger.on_message_executed(xmsg)
        await hook_generator.__anext__()
        r = caplog.records[-1]
        assert r.message == "Executing message"
        assert r.data == {
            "input": "fake-topic",
            "executor": {"name": "default"},
            "job": {"name": "fake-queue"},
            "message": {
                "id": "m1",
                "tags": {},
            },
            "labels": {"owner": "team-datalineup"},
            "resources": {FakeResource._typename(): "r1"},
            "pipeline": "tests.worker.services.test_logger.fake_pipeline",
            "trace": {},
        }

        frozen_time.tick(delta=timedelta(seconds=1))

        with pytest.raises(StopAsyncIteration):
            await hook_generator.asend(results)

        r = caplog.records[-1]
        assert r.message == "Executed message"
        assert r.data == {
            "input": "fake-topic",
            "executor": {"name": "default"},
            "job": {
                "name": "fake-queue",
            },
            "message": {
                "id": "m1",
                "tags": {},
            },
            "labels": {"owner": "team-datalineup"},
            "resources": {FakeResource._typename(): "r1"},
            "pipeline": "tests.worker.services.test_logger.fake_pipeline",
            "result": {
                "events_count": 0,
                "output_count": 1,
                "output": [
                    {
                        "channel": "default",
                        "message": {"id": "m2", "tags": {}},
                    }
                ],
                "resources": {FakeResource._typename(): {"release_at": 10}},
            },
            "trace": {"duration_ms": 1000},
        }
