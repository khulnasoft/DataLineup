import typing as t

import pytest

from datalineup_engine.config import Config
from datalineup_engine.core import PipelineResults
from datalineup_engine.core import TopicMessage
from datalineup_engine.worker.executors import Executor
from datalineup_engine.worker.executors.executable import ExecutableMessage
from datalineup_engine.worker.services.manager import ServicesManager
from datalineup_engine.worker.services.tracing import Tracer
from tests.utils.span_exporter import InMemorySpanExporter


@pytest.fixture
def config(config: Config) -> Config:
    return config.load_object(
        {
            "services_manager": {
                "services": [
                    "datalineup_engine.worker.services.tracing.Tracer",
                ]
            }
        }
    )


@pytest.mark.asyncio
async def test_trace_message_executed(
    services_manager: ServicesManager,
    executor: Executor,
    executable_maker: t.Callable[..., ExecutableMessage],
    span_exporter: InMemorySpanExporter,
) -> None:
    services_manager.services.cast_service(Tracer)
    xmsg = executable_maker(
        message=TopicMessage(args={}, config={"tracer": {"rate": 0.5}})
    )

    @services_manager.services.s.hooks.message_executed.emit
    async def scope(xmsg: ExecutableMessage) -> PipelineResults:
        return await executor.process_message(xmsg)

    await services_manager.services.s.hooks.message_polled.emit(xmsg)
    await scope(xmsg)

    traces = span_exporter.get_finished_traces()
    assert len(traces) == 1
    assert traces[0].otel_span.name == "worker executing"
    assert traces[0].otel_span.attributes == {
        "datalineup.job.name": "fake-queue",
        "datalineup.job.labels.owner": "team-datalineup",
        "datalineup.input.name": "fake-topic",
        "datalineup.resources.names": (),
        "datalineup.message.id": xmsg.id,
        "datalineup.pipeline.name": "tests.conftest.pipeline",
        "datalineup.outputs.count": 0,
        "datalineup.sampling.rate": 0.5,
    }

    assert traces[0].children[0].otel_span.name == "executor executing"
    assert traces[0].children[0].otel_span.attributes == {
        "datalineup.resources.names": (),
        "datalineup.message.id": xmsg.id,
        "datalineup.pipeline.name": "tests.conftest.pipeline",
    }
