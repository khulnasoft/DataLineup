import dataclasses

from datalineup_engine.utils.traceback_data import TracebackData


@dataclasses.dataclass
class ErrorMessageArgs:
    type: str
    module: str
    message: str
    traceback: TracebackData
