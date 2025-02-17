import typing as t

import os
import socket

from .config import Env
from .config import RabbitMQConfig
from .config import RedisConfig
from .config import DatalineupConfig
from .config import ServicesManagerConfig
from .config import WorkerManagerConfig


class config(DatalineupConfig):
    env = Env(os.environ.get("DATALINEUP_ENV", "development"))
    worker_id = socket.gethostname()
    selector: t.Optional[str] = os.environ.get("DATALINEUP_SELECTOR")
    executors: list[str] | None = os.environ.get("DATALINEUP_EXECUTORS", "").split(",")
    worker_manager_url = os.environ.get(
        "DATALINEUP_WORKER_MANAGER_URL", "http://127.0.0.1:5000"
    )
    standalone = os.environ.get("DATALINEUP_STANDALONE") == "1"

    class services_manager(ServicesManagerConfig):
        base_services = [
            "datalineup_engine.worker.services.tasks_runner.TasksRunnerService",
            "datalineup_engine.worker.services.http_client.HttpClient",
            "datalineup_engine.worker.services.api_client.ApiClient",
            "datalineup_engine.worker.services.job_state.service.JobStateService",
        ]

        services = [
            "datalineup_engine.worker.services.tracing.Tracer",
            "datalineup_engine.worker.services.metrics.Metrics",
            "datalineup_engine.worker.services.loggers.Logger",
            "datalineup_engine.worker.services.rabbitmq.RabbitMQService",
        ]

    class rabbitmq(RabbitMQConfig):
        url = os.environ.get("DATALINEUP_AMQP_URL", "amqp://127.0.0.1/")
        urls = {}
        reconnect_interval = 10

    class worker_manager(WorkerManagerConfig):
        flask_host = os.environ.get("DATALINEUP_FLASK_HOST", "127.0.0.1")
        flask_port = int(os.environ.get("DATALINEUP_FLASK_PORT", 5000))
        database_url: str = os.environ.get("DATALINEUP_DATABASE_URL", "sqlite:///test.db")
        database_connection_creator: t.Optional[str] = None
        database_pool_recycle: int = -1
        database_pool_pre_ping: bool = False
        static_definitions_directories: list[str] = os.environ.get(
            "DATALINEUP_STATIC_DEFINITIONS_DIRS", os.getcwd()
        ).split(":")
        static_definitions_jobs_selector: t.Optional[str] = os.environ.get(
            "DATALINEUP_STATIC_DEFINITIONS_JOBS_SELECTOR"
        )
        work_items_per_worker = 10

    class redis(RedisConfig):
        dsn = "redis://localhost:6379"

    class tracer:
        rate: float = 0.0


class client_config(config):
    class services_manager(config.services_manager):
        services = [
            "datalineup_engine.worker.services.rabbitmq.RabbitMQService",
        ]
