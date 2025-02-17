import asyncio
from unittest.mock import Mock

import pytest

from datalineup_engine.core import PipelineOutput
from datalineup_engine.core import PipelineResults
from datalineup_engine.core import ResourceUsed
from datalineup_engine.worker.services.hooks import MessagePublished
from datalineup_engine.worker.services.manager import ServicesManager
from datalineup_engine.worker.services.metrics import Metrics
from tests.utils.metrics import MetricsCapture


@pytest.fixture
def fake_published_message() -> Mock:
    data = Mock()
    data.xmsg.queue.definition.executor = "exec"
    data.xmsg.queue.definition.name = "test-job"
    data.xmsg.queue.definition.labels = {"k": "v"}
    data.xmsg.message.info.name = "test.fake.pipeline"
    data.topic.name = "test.fake.topic"
    return data


@pytest.mark.asyncio
async def test_message_metrics(
    services_manager: ServicesManager, metrics_capture: MetricsCapture
) -> None:
    data = Mock()
    data.queue.definition.name = "test-job"
    data.queue.definition.executor = "exec"
    data.queue.definition.labels = {"k": "v"}
    data.message.info.name = "test.fake.pipeline"
    pipeline_params = {
        "pipeline": data.message.info.name,
        "datalineup.executor.name": "exec",
        "datalineup.job.name": "test-job",
        "datalineup.job.labels.k": "v",
    }
    metric = services_manager.services.cast_service(Metrics)
    await metric.on_message_polled(data)
    await metric.on_message_scheduled(data)
    await metric.on_message_submitted(data)

    metrics_capture.assert_metric_expected(
        "datalineup.pipeline.message",
        [
            metrics_capture.create_number_data_point(
                1,
                attributes=pipeline_params | {"state": "polled"},
            ),
            metrics_capture.create_number_data_point(
                1,
                attributes=pipeline_params | {"state": "scheduled"},
            ),
            metrics_capture.create_number_data_point(
                1,
                attributes=pipeline_params | {"state": "submitted"},
            ),
        ],
    )


@pytest.mark.asyncio
async def test_metrics_message_executed(
    services_manager: ServicesManager, metrics_capture: MetricsCapture
) -> None:
    data = Mock()
    data.queue.definition.name = "test-job"
    data.queue.definition.executor = "exec"
    data.queue.definition.labels = {"k": "v"}
    data.message.info.name = "test.fake.pipeline"
    pipeline_params = {
        "pipeline": data.message.info.name,
        "datalineup.executor.name": "exec",
        "datalineup.job.name": "test-job",
        "datalineup.job.labels.k": "v",
    }
    metric = services_manager.services.cast_service(Metrics)

    results = PipelineResults(
        outputs=[PipelineOutput(channel="default", message=data)],
        resources=[ResourceUsed(type="Resource", release_at=10)],
    )

    hook_generator = metric.on_message_executed(data)
    await hook_generator.__anext__()
    with pytest.raises(StopAsyncIteration):
        await asyncio.sleep(1)
        await hook_generator.asend(results)

    metrics_capture.assert_metric_expected(
        "datalineup.pipeline.message",
        [
            metrics_capture.create_number_data_point(
                1,
                attributes=pipeline_params | {"state": "executing"},
            ),
            metrics_capture.create_number_data_point(
                1,
                attributes=pipeline_params | {"state": "success"},
            ),
        ],
    )

    metrics_capture.assert_metric_expected(
        "datalineup.resources.used",
        [
            metrics_capture.create_number_data_point(
                1,
                attributes={"type": "Resource"},
            ),
        ],
    )

    metrics_capture.assert_metric_expected(
        "datalineup.pipeline.outputs",
        [
            metrics_capture.create_number_data_point(
                1,
                attributes=pipeline_params | {"channel": "default"},
            ),
        ],
    )

    metrics_capture.assert_metric_expected(
        "datalineup.pipeline.duration",
        [
            metrics_capture.create_histogram_data_point(
                count=1,
                sum_data_point=1000,
                max_data_point=1000,
                min_data_point=1000,
                attributes=pipeline_params,
            ),
        ],
    )


@pytest.mark.asyncio
async def test_metrics_message_execute_failed(
    services_manager: ServicesManager, metrics_capture: MetricsCapture
) -> None:
    data = Mock()
    data.queue.definition.name = "test-job"
    data.queue.definition.executor = "exec"
    data.queue.definition.labels = {"k": "v"}
    data.message.info.name = "test.fake.pipeline"
    pipeline_params = {
        "pipeline": data.message.info.name,
        "datalineup.executor.name": "exec",
        "datalineup.job.name": "test-job",
        "datalineup.job.labels.k": "v",
    }
    metric = services_manager.services.cast_service(Metrics)

    hook_generator = metric.on_message_executed(data)
    await hook_generator.__anext__()
    with pytest.raises(StopAsyncIteration):
        await asyncio.sleep(1)
        await hook_generator.athrow(Exception())

    metrics_capture.assert_metric_expected(
        "datalineup.pipeline.message",
        [
            metrics_capture.create_number_data_point(
                1,
                attributes=pipeline_params | {"state": "executing"},
            ),
            metrics_capture.create_number_data_point(
                1,
                attributes=pipeline_params | {"state": "failed"},
            ),
        ],
    )

    metrics_capture.assert_metric_expected(
        "datalineup.pipeline.duration",
        [
            metrics_capture.create_histogram_data_point(
                count=1,
                sum_data_point=1000,
                max_data_point=1000,
                min_data_point=1000,
                attributes=pipeline_params,
            ),
        ],
    )


@pytest.mark.asyncio
async def test_metrics_message_published(
    services_manager: ServicesManager,
    metrics_capture: MetricsCapture,
    fake_published_message: MessagePublished,
) -> None:
    params = {
        "pipeline": "test.fake.pipeline",
        "topic": "test.fake.topic",
        "datalineup.executor.name": "exec",
        "datalineup.job.name": "test-job",
        "datalineup.job.labels.k": "v",
    }
    metric = services_manager.services.cast_service(Metrics)

    hook_generator = metric.on_message_published(fake_published_message)
    await hook_generator.asend(None)
    with pytest.raises(StopAsyncIteration):
        await hook_generator.asend(None)

    metrics_capture.assert_metric_expected(
        "datalineup.pipeline.publish",
        [
            metrics_capture.create_number_data_point(
                1,
                attributes=params | {"state": "before"},
            ),
            metrics_capture.create_number_data_point(
                1,
                attributes=params | {"state": "success"},
            ),
        ],
    )


@pytest.mark.asyncio
async def test_metrics_message_publish_failed(
    services_manager: ServicesManager,
    metrics_capture: MetricsCapture,
    fake_published_message: MessagePublished,
) -> None:
    params = {
        "pipeline": "test.fake.pipeline",
        "topic": "test.fake.topic",
        "datalineup.executor.name": "exec",
        "datalineup.job.name": "test-job",
        "datalineup.job.labels.k": "v",
    }
    metric = services_manager.services.cast_service(Metrics)

    hook_generator = metric.on_message_published(fake_published_message)
    await hook_generator.asend(None)
    with pytest.raises(StopAsyncIteration):
        await hook_generator.athrow(Exception())

    metrics_capture.assert_metric_expected(
        "datalineup.pipeline.publish",
        [
            metrics_capture.create_number_data_point(
                1,
                attributes=params | {"state": "before"},
            ),
            metrics_capture.create_number_data_point(
                1,
                attributes=params | {"state": "failed"},
            ),
        ],
    )


@pytest.mark.asyncio
async def test_metrics_topic_blocked(
    services_manager: ServicesManager,
    metrics_capture: MetricsCapture,
    fake_published_message: MessagePublished,
) -> None:
    params = {
        "pipeline": "test.fake.pipeline",
        "topic": "test.fake.topic",
        "datalineup.executor.name": "exec",
        "datalineup.job.name": "test-job",
        "datalineup.job.labels.k": "v",
    }
    metric = services_manager.services.cast_service(Metrics)

    hook_generator = metric.on_output_blocked(fake_published_message)
    await hook_generator.asend(None)
    with pytest.raises(StopAsyncIteration):
        metrics_capture.assert_metric_expected(
            "datalineup.topic.blocked",
            [
                metrics_capture.create_number_data_point(1, attributes=params),
            ],
        )

        await hook_generator.asend(None)

    metrics_capture.collect()
    metrics_capture.assert_metric_expected(
        "datalineup.topic.blocked",
        [
            metrics_capture.create_number_data_point(0, attributes=params),
        ],
    )
