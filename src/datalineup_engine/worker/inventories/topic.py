import typing as t

import dataclasses
from contextlib import AsyncExitStack

from datalineup_engine.core.api import ComponentDefinition
from datalineup_engine.core.types import Cursor
from datalineup_engine.worker.inventory import Item
from datalineup_engine.worker.inventory import IteratorInventory
from datalineup_engine.worker.services import Services


class TopicAdapter(IteratorInventory):
    @dataclasses.dataclass
    class Options:
        topic: ComponentDefinition

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        # This import must be done late since work_factory depends on this module.
        from datalineup_engine.worker.work_factory import build_topic

        self.topic = build_topic(options.topic, services=services)

    async def open(self) -> None:
        await self.topic.open()

    async def iterate(self, after: t.Optional[Cursor] = None) -> t.AsyncIterator[Item]:
        async for message_ctx in self.topic.run():
            try:
                async with AsyncExitStack() as stack:
                    message = await stack.enter_async_context(message_ctx)
                    yield Item(
                        id=message.id,
                        cursor=None,
                        args=message.args,
                        tags=message.tags,
                        metadata=message.metadata,
                        context=stack.pop_all(),
                    )
            except Exception:
                self.logger.exception("Failed to convert message")
