from typing import Any
from typing import cast

import pydantic.v1.json
from flask import Flask
from flask import current_app as _current_app
from flask.json.provider import DefaultJSONProvider

from datalineup_engine.worker_manager.context import WorkerManagerContext


class JSONProvider(DefaultJSONProvider):
    @staticmethod
    def default(o: Any) -> Any:
        return pydantic.v1.json.pydantic_encoder(o)


class DatalineupApp(Flask):
    json_provider_class = JSONProvider

    def __init__(
        self,
        worker_manager_context: WorkerManagerContext,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.datalineup = worker_manager_context


current_app: DatalineupApp = cast(DatalineupApp, _current_app)
