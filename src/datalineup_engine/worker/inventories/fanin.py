import typing as t

import dataclasses
import json
from contextlib import AsyncExitStack

import asyncstdlib as alib

from datalineup_engine.core.api import ComponentDefinition
from datalineup_engine.core.types import Cursor
from datalineup_engine.utils import iterators
from datalineup_engine.worker.inventory import Item
from datalineup_engine.worker.inventory import IteratorInventory
from datalineup_engine.worker.services import Services
from datalineup_engine.worker.work_factory import build_inventory

T = t.TypeVar("T")


class FanIn(IteratorInventory):
    name = "fanin"

    @dataclasses.dataclass
    class Options:
        inputs: list[ComponentDefinition]

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        super().__init__()

        self.inputs = {
            input_def.name: build_inventory(input_def, services=services)
            for input_def in options.inputs
        }

    async def open(self) -> None:
        for inventory in self.inputs.values():
            await inventory.open()

    async def iterate(self, after: t.Optional[Cursor] = None) -> t.AsyncIterator[Item]:
        cursors = json.loads(after) if after else {}

        async with AsyncExitStack() as ctx:
            aiters: list[t.AsyncIterator[tuple[str, Item]]] = []
            for k, inventory in self.inputs.items():
                to_tuple: t.Callable = lambda m, k=k: (k, m)
                i_iter = inventory.run(after=cursors.get(k))
                j_iter = alib.map(to_tuple, i_iter)
                k_iter = alib.scoped_iter(j_iter)
                aiters.append(await ctx.enter_async_context(k_iter))

            scheduler = self.make_scheduler(aiters)
            async for name, message in scheduler:
                message.tags.setdefault("inventory.name", name)
                yield message

    def make_scheduler(
        self, aiters: list[t.AsyncIterator[T]]
    ) -> iterators.Scheduler[T]:
        return iterators.Scheduler(aiters)

    @property
    def cursor(self) -> t.Optional[Cursor]:
        return Cursor(
            json.dumps(
                {
                    name: c
                    for name, inv in self.inputs.items()
                    if (c := inv.cursor) is not None
                }
            )
        )


@dataclasses.dataclass
class PriorityInput:
    priority: int
    inventory: ComponentDefinition


class PriorityFanIn(FanIn):
    name = "priority_fanin"

    @dataclasses.dataclass
    class Options:
        inputs: list[PriorityInput]

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        super().__init__(
            options=FanIn.Options(inputs=[i.inventory for i in options.inputs]),
            services=services,
        )
        self.priority = [i.priority for i in options.inputs]

    def make_scheduler(
        self, aiters: list[t.AsyncIterator[T]]
    ) -> iterators.Scheduler[T]:
        credits = [
            iterators.IteratorPriority(
                priority=p,
                iterator=i,
            )
            for i, p in zip(aiters, self.priority, strict=True)
        ]

        return iterators.CreditsScheduler(credits)
