from datalineup_engine.config import Env
from datalineup_engine.config import DatalineupConfig
from datalineup_engine.config import ServicesManagerConfig
from datalineup_engine.config_definitions import WorkerManagerConfig


class config(DatalineupConfig):
    env = Env.TEST

    class services_manager(ServicesManagerConfig):
        services: list[str] = [
            "datalineup_engine.worker.services.tracing.Tracer",
            "datalineup_engine.worker.services.metrics.Metrics",
            "datalineup_engine.worker.services.usage_metrics.UsageMetrics",
            "datalineup_engine.worker.services.loggers.Logger",
        ]

    class job_state:
        auto_flush = False

    class worker_manager(WorkerManagerConfig):
        static_definitions_directories = []
