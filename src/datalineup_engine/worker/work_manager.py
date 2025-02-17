from typing import Generic
from typing import Iterator
from typing import Optional
from typing import Type
from typing import TypeVar

import asyncio
import dataclasses
from datetime import datetime
from datetime import timedelta

from datalineup_engine.client.worker_manager import WorkerManagerClient
from datalineup_engine.core import JobId
from datalineup_engine.core.api import ComponentDefinition
from datalineup_engine.core.api import LockResponse
from datalineup_engine.core.api import QueueItemWithState
from datalineup_engine.core.api import ResourceItem
from datalineup_engine.utils.log import getLogger
from datalineup_engine.worker import work_factory
from datalineup_engine.worker.context import job_context
from datalineup_engine.worker.executors.executable import ExecutableQueue
from datalineup_engine.worker.resources.manager import ResourceData
from datalineup_engine.worker.resources.manager import ResourceRateLimit
from datalineup_engine.worker.resources.provider import ResourcesProvider
from datalineup_engine.worker.services import Services
from datalineup_engine.worker.services.api_client import ApiClient

T = TypeVar("T")


@dataclasses.dataclass
class ItemsSync(Generic[T]):
    add: list[T]
    drop: list[T]

    @classmethod
    def empty(cls: Type["ItemsSync[T]"]) -> "ItemsSync[T]":
        return cls(add=[], drop=[])


@dataclasses.dataclass
class WorkSync:
    queues: ItemsSync[ExecutableQueue]
    resources: ItemsSync[ResourceData]
    resources_providers: ItemsSync[ResourcesProvider]
    executors: ItemsSync[ComponentDefinition]

    @classmethod
    def empty(cls: Type["WorkSync"]) -> "WorkSync":
        return cls(
            queues=ItemsSync.empty(),
            resources=ItemsSync.empty(),
            resources_providers=ItemsSync.empty(),
            executors=ItemsSync.empty(),
        )


WorkerItems = dict[JobId, ExecutableQueue]


class WorkManager:
    def __init__(
        self, *, services: Services, client: Optional[WorkerManagerClient] = None
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.client = client or services.cast_service(ApiClient).client
        self.worker_items: WorkerItems = {}
        self.worker_resources: dict[str, ResourceData] = {}
        self.worker_resources_providers: dict[str, ResourcesProvider] = {}
        self.worker_executors: dict[str, ComponentDefinition] = {}
        self.last_sync_at: Optional[datetime] = None
        self.sync_period = timedelta(seconds=60)
        self.services = services

    async def sync(self) -> WorkSync:
        if self.last_sync_at:
            last_sync_elapsed = datetime.now() - self.last_sync_at
            if last_sync_elapsed < self.sync_period:
                await asyncio.sleep(
                    (self.sync_period - last_sync_elapsed).total_seconds()
                )
        self.last_sync_at = datetime.now()
        lock_response = await self.client.lock()

        queues_sync = await self.load_queues(lock_response)
        resources_sync = await self.load_resources(lock_response)
        resources_providers_sync = await self.load_resources_providers(lock_response)
        executors_sync = await self.load_executors(lock_response)

        return WorkSync(
            queues=queues_sync,
            resources=resources_sync,
            resources_providers=resources_providers_sync,
            executors=executors_sync,
        )

    async def load_queues(
        self, lock_response: LockResponse
    ) -> ItemsSync[ExecutableQueue]:
        current_items = set(self.worker_items.keys())
        sync_items = {item.name: item for item in lock_response.items}
        sync_items_ids = set(sync_items.keys())
        add = sync_items_ids - current_items
        drop = current_items - sync_items_ids

        added_items = await self.build_queues_for_worker_items(
            sync_items[i] for i in add
        )
        self.worker_items.update(added_items)
        add_items = list(added_items.values())
        drop_items = [self.worker_items.pop(k) for k in drop]

        return ItemsSync(add=add_items, drop=drop_items)

    def work_queue_by_name(self, name: JobId) -> Optional[ExecutableQueue]:
        return self.worker_items.get(name)

    async def build_queues_for_worker_items(
        self, items: Iterator[QueueItemWithState]
    ) -> WorkerItems:
        return {
            item.name: queue
            for item in items
            if (queue := await self.build_queue_for_worker_item(item))
        }

    async def build_queue_for_worker_item(
        self, item: QueueItemWithState
    ) -> Optional[ExecutableQueue]:
        with job_context(item):
            try:

                @self.services.s.hooks.work_queue_built.emit
                async def scope(item: QueueItemWithState) -> ExecutableQueue:
                    return work_factory.build(item, services=self.services)

                return await scope(item)
            except Exception:
                return None

    async def load_resources(
        self, lock_response: LockResponse
    ) -> ItemsSync[ResourceData]:
        current_items = set(self.worker_resources.keys())
        sync_items = {item.name: item for item in lock_response.resources}
        sync_items_ids = set(sync_items.keys())
        add = sync_items_ids - current_items
        drop = current_items - sync_items_ids

        added_items = {i: self.build_resource_data(sync_items[i]) for i in add}
        self.worker_resources.update(added_items)
        add_items = list(added_items.values())
        drop_items = [self.worker_resources.pop(k) for k in drop]

        return ItemsSync(add=add_items, drop=drop_items)

    async def load_resources_providers(
        self, lock_response: LockResponse
    ) -> ItemsSync[ResourcesProvider]:
        current_items = set(self.worker_resources_providers.keys())
        sync_items = {item.name: item for item in lock_response.resources_providers}
        sync_items_ids = set(sync_items.keys())
        add = sync_items_ids - current_items
        drop = current_items - sync_items_ids

        added_items = {
            i: work_factory.build_resources_provider(
                sync_items[i], services=self.services
            )
            for i in add
        }
        self.worker_resources_providers.update(added_items)
        add_items = list(added_items.values())
        drop_items = [self.worker_resources_providers.pop(k) for k in drop]

        return ItemsSync(add=add_items, drop=drop_items)

    async def load_executors(
        self, lock_response: LockResponse
    ) -> ItemsSync[ComponentDefinition]:
        current_items = set(self.worker_executors.keys())
        sync_items = {item.name: item for item in lock_response.executors}
        sync_items_ids = set(sync_items.keys())
        add = sync_items_ids - current_items
        drop = current_items - sync_items_ids

        added_items = {i: sync_items[i] for i in add}
        self.worker_executors.update(added_items)
        add_items = list(added_items.values())
        drop_items = [self.worker_executors.pop(k) for k in drop]

        return ItemsSync(add=add_items, drop=drop_items)

    def build_resource_data(self, item: ResourceItem) -> ResourceData:
        return ResourceData(
            name=item.name,
            type=item.type,
            data=item.data,
            default_delay=item.default_delay,
            rate_limit=(
                ResourceRateLimit(
                    rate_limits=item.rate_limit.rate_limits,
                    strategy=item.rate_limit.strategy,
                )
                if item.rate_limit
                else None
            ),
        )
