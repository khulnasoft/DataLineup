from typing import Optional

import dataclasses

from datalineup_engine.core import Cursor
from datalineup_engine.utils.declarative_config import BaseObject
from datalineup_engine.worker.inventories import Item


@dataclasses.dataclass
class InventorySelector:
    inventory: str


@dataclasses.dataclass
class InventoryTestSpec:
    selector: InventorySelector
    items: list[Item]
    limit: Optional[int] = None
    after: Optional[Cursor] = None


@dataclasses.dataclass
class InventoryTest(BaseObject):
    spec: InventoryTestSpec
