from collections import Counter
from collections.abc import AsyncGenerator

from opentelemetry.metrics import get_meter

from datalineup_engine.core import PipelineResults
from datalineup_engine.utils.telemetry import get_timer
from datalineup_engine.worker.error_handling import HandledError
from datalineup_engine.worker.executors.executable import ExecutableMessage
from datalineup_engine.worker.services.hooks import MessagePublished

from . import MinimalService


def executable_params(xmsg: ExecutableMessage) -> dict:
    return {
        "datalineup.executor.name": xmsg.queue.definition.executor,
        "datalineup.job.name": xmsg.queue.definition.name,
        "pipeline": xmsg.message.info.name,
    } | {f"datalineup.job.labels.{k}": v for k, v in xmsg.queue.definition.labels.items()}


class Metrics(MinimalService):
    name = "metrics"

    async def open(self) -> None:
        self.meter = get_meter("datalineup.metrics")
        self.message_counter = self.meter.create_counter(
            name="datalineup.pipeline.message",
            description="""
            Counts the message at different stage of the execution pipeline,
            from polling to publishing new ones.
            """,
        )

        self.resource_counter = self.meter.create_counter(
            name="datalineup.resources.used",
            description="Counts the resource usage.",
        )

        self.output_counter = self.meter.create_counter(
            name="datalineup.pipeline.outputs",
            description="Counts the resource usage.",
        )

        self.publish_counter = self.meter.create_counter(
            name="datalineup.pipeline.publish",
            description="Counts message published to topics.",
        )

        self.blocked_counter = self.meter.create_up_down_counter(
            name="datalineup.topic.blocked",
            description="Counts message blocked by topic backpressure.",
        )

        self.message_duration = self.meter.create_histogram(
            name="datalineup.pipeline.duration",
            unit="ms",
            description="""Time spent to execute a pipeline from the datalineup
            worker point of view.
            """,
        )

        self.services.hooks.message_polled.register(self.on_message_polled)
        self.services.hooks.message_scheduled.register(self.on_message_scheduled)
        self.services.hooks.message_submitted.register(self.on_message_submitted)
        self.services.hooks.message_executed.register(self.on_message_executed)
        self.services.hooks.message_published.register(self.on_message_published)
        self.services.hooks.output_blocked.register(self.on_output_blocked)

    async def on_message_polled(self, xmsg: ExecutableMessage) -> None:
        params = executable_params(xmsg)
        self.message_counter.add(1, params | {"state": "polled"})

    async def on_message_scheduled(self, xmsg: ExecutableMessage) -> None:
        params = executable_params(xmsg)
        self.message_counter.add(1, params | {"state": "scheduled"})

    async def on_message_submitted(self, xmsg: ExecutableMessage) -> None:
        params = executable_params(xmsg)
        self.message_counter.add(1, params | {"state": "submitted"})

    async def on_message_executed(
        self, xmsg: ExecutableMessage
    ) -> AsyncGenerator[None, PipelineResults]:
        params = executable_params(xmsg)
        self.message_counter.add(1, params | {"state": "executing"})
        try:
            with get_timer(self.message_duration).time(params):
                try:
                    results = yield
                except HandledError as e:
                    results = e.results
                    self.message_counter.add(1, params | {"state": "failed_handled"})
                else:
                    self.message_counter.add(1, params | {"state": "success"})

            for resource in results.resources:
                self.resource_counter.add(1, {"type": resource.type})

            output_counters: Counter[str] = Counter()
            for output in results.outputs:
                output_counters[output.channel] += 1
            for channel, count in output_counters.items():
                self.output_counter.add(count, params | {"channel": channel})
        except Exception:
            self.message_counter.add(1, params | {"state": "failed"})

    async def on_message_published(
        self, event: MessagePublished
    ) -> AsyncGenerator[None, None]:
        params = executable_params(event.xmsg) | {"topic": event.topic.name}
        self.publish_counter.add(1, params | {"state": "before"})
        try:
            yield
            self.publish_counter.add(1, params | {"state": "success"})
        except Exception:
            self.publish_counter.add(1, params | {"state": "failed"})

    async def on_output_blocked(
        self, event: MessagePublished
    ) -> AsyncGenerator[None, None]:
        params = executable_params(event.xmsg) | {"topic": event.topic.name}
        try:
            self.blocked_counter.add(1, params)
            yield
        finally:
            self.blocked_counter.add(-1, params)
