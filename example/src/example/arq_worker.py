import logging

import arq

from datalineup_engine.worker.executors.arq.worker import WorkerOptions
from datalineup_engine.worker.executors.arq.worker import WorkerSettings
from datalineup_engine.worker.executors.bootstrap import PipelineBootstrap
from datalineup_engine.worker.pipeline_message import PipelineMessage
from datalineup_engine.worker.services.loggers.console_logging import setup


def pipeline_hook(message: PipelineMessage) -> None:
    logging.info(f"processing {message}")


def initializer(bootstrap: PipelineBootstrap) -> None:
    logging.info("Initiliazing")
    bootstrap.pipeline_hook.register(pipeline_hook)


def main() -> None:
    setup()
    options = WorkerOptions(
        worker_concurrency=2,
        worker_initializer=initializer,
    )
    arq.run_worker(WorkerSettings, ctx=options)  # type: ignore[arg-type]


if __name__ == "__main__":
    main()
