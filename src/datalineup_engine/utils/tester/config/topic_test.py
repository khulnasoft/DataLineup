from typing import Optional

import dataclasses

from datalineup_engine.core import TopicMessage
from datalineup_engine.utils.declarative_config import BaseObject


@dataclasses.dataclass
class TopicSelector:
    topic: str


@dataclasses.dataclass
class TopicTestSpec:
    selector: TopicSelector
    messages: list[TopicMessage]
    limit: Optional[int] = None
    skip: Optional[int] = None


@dataclasses.dataclass
class TopicTest(BaseObject):
    spec: TopicTestSpec
