import json
from collections import Counter

import asyncstdlib as alib
import pytest

from datalineup_engine.core.types import Cursor
from datalineup_engine.core.types import MessageId
from datalineup_engine.worker.inventories.fanin import FanIn
from datalineup_engine.worker.inventories.fanin import PriorityFanIn
from datalineup_engine.worker.inventory import Item


@pytest.mark.asyncio
async def test_fanin_inventory() -> None:
    inventory = FanIn.from_options(
        {
            "inputs": [
                {
                    "name": "a",
                    "type": "StaticInventory",
                    "options": {"items": [{"n": 0}, {"n": 1}, {"n": 2}, {"n": 3}]},
                },
                {
                    "name": "b",
                    "type": "StaticInventory",
                    "options": {"items": [{"n": 4}, {"n": 5}]},
                },
            ],
            "batch_size": 10,
        },
        services=None,
    )
    messages = await alib.list(inventory.run())
    assert {m.args["n"] for m in messages} == set(range(6))
    m = messages[-1]
    assert m.cursor == "3"
    assert (c := inventory.cursor)
    assert json.loads(c) == {}

    for m in messages:
        async with m:
            pass

    assert (c := inventory.cursor)
    assert json.loads(c) == {
        "a": '{"v": 1, "a": "3"}',
        "b": '{"v": 1, "a": "1"}',
    }

    messages = await alib.list(inventory.iterate(after=Cursor('{"a": "3", "b": "0"}')))
    assert messages == [
        Item(
            id=MessageId("1"),
            cursor="1",
            args={"n": 5},
            tags={"inventory.name": "b"},
        )
    ]


@pytest.mark.asyncio
async def test_priority_fanin_inventory() -> None:
    inventory = PriorityFanIn.from_options(
        {
            "inputs": [
                {
                    "priority": 1,
                    "inventory": {
                        "name": "a",
                        "type": "StaticInventory",
                        "options": {"items": [{"n": "a"}] * 100},
                    },
                },
                {
                    "priority": 2,
                    "inventory": {
                        "name": "b",
                        "type": "StaticInventory",
                        "options": {"items": [{"n": "b"}] * 100},
                    },
                },
            ],
        },
        services=None,
    )
    messages = (await alib.list(inventory.iterate()))[:75]
    counter = Counter([m.args["n"] for m in messages])
    assert counter["a"] in range(49, 51)
    assert counter["b"] in range(24, 26)

    messages = (
        await alib.list(inventory.iterate(after=Cursor('{"a": "70", "b": "30"}')))
    )[:50]
    counter = Counter([m.args["n"] for m in messages])
    assert counter["a"] == 29
    assert counter["b"] == 21
