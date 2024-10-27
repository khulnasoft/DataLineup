import os

from datalineup_engine.config import DatalineupConfig
from datalineup_engine.config import ServicesManagerConfig
from datalineup_engine.default_config import config as default_config
from datalineup_engine.worker.services.extras.sentry import Sentry


class config(DatalineupConfig):
    class services_manager(ServicesManagerConfig):
        services = default_config.services_manager.services + [
            "datalineup_engine.worker.services.tracing.TracerConfig",
            "datalineup_engine.worker.services.loggers.ConsoleLogging",
        ]

    class sentry(Sentry.Options):
        dsn = os.environ.get("DATALINEUP_SENTRY")
