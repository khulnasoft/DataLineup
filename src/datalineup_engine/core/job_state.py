import dataclasses

from datalineup_engine.core import Cursor
from datalineup_engine.core.pipeline import PipelineEvent


@dataclasses.dataclass
class CursorStateUpdated(PipelineEvent):
    state: dict
    cursor: Cursor | None = None


class CursorState:
    pass
