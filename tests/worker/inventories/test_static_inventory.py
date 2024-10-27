import pytest

from datalineup_engine.core import Cursor
from datalineup_engine.core import MessageId
from datalineup_engine.worker.inventories import Item
from datalineup_engine.worker.inventories import StaticInventory


@pytest.mark.asyncio
async def test_static_inventory() -> None:
    inventory = StaticInventory.from_options({"items": [{"n": 1}, {"n": 2}]})
    batch = list(await inventory.next_batch())
    assert batch == [
        Item(id=MessageId("0"), args={"n": 1}),
        Item(id=MessageId("1"), args={"n": 2}),
    ]

    inventory = StaticInventory.from_options({"items": [{}] * 9})
    assert len(list(await inventory.next_batch())) == 9
    assert len(list(await inventory.next_batch(after=Cursor("4")))) == 4
    assert not list(await inventory.next_batch(after=Cursor("8")))

    inventory = StaticInventory.from_options({"items": [{"a": None}]})
    batch = list(await inventory.next_batch())
    assert batch == [
        Item(id=MessageId("0"), args={"a": None}),
    ]
