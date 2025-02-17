import typing as t

from datalineup_engine.core.api import ComponentDefinition
from datalineup_engine.core.api import QueueItemWithState
from datalineup_engine.core.api import ResourcesProviderItem
from datalineup_engine.utils import inspect as extra_inspect
from datalineup_engine.worker.resources.provider import (
    BUILTINS as resources_provider_builtins,
)
from datalineup_engine.worker.resources.provider import ResourcesProvider

from .executors.executable import ExecutableQueue
from .inventories import Inventory
from .job import Job
from .services import Services
from .topics import Topic


def build(queue_item: QueueItemWithState, *, services: Services) -> ExecutableQueue:
    input_ = build_item(queue_item.input, services=services)
    if isinstance(input_, Inventory):
        input_ = build_inventory_job(input_, queue_item=queue_item, services=services)

    output = {
        k: [
            output
            for t in ts
            if isinstance(t, ComponentDefinition)
            and isinstance(output := build_item(t, services=services), Topic)
        ]
        for k, ts in queue_item.output.items()
    }

    return ExecutableQueue(
        definition=queue_item,
        topic=input_,
        output=output,
        services=services,
    )


def build_item(
    item_definition: ComponentDefinition, *, services: Services
) -> t.Union[Inventory, Topic]:
    klass = BUILTINS.get(item_definition.type)
    if klass is None:
        klass = extra_inspect.import_name(item_definition.type)
    if klass is None:
        raise ValueError(f"Unknown topic type: {item_definition.type}")
    if not issubclass(klass, (Topic, Inventory)):
        raise ValueError(f"{klass} must be a Topic or Inventory")

    options = {"name": item_definition.name} | item_definition.options
    item = klass.from_options(options, services=services)
    item.name = item_definition.name
    return item


def build_topic(topic_item: ComponentDefinition, *, services: Services) -> Topic:
    topic = build_item(topic_item, services=services)
    if not isinstance(topic, Topic):
        raise ValueError(f"{topic} must be a Topic")
    return topic


def build_inventory(
    inventory_item: ComponentDefinition, *, services: Services
) -> Inventory:
    inventory = build_item(inventory_item, services=services)
    if not isinstance(inventory, Inventory):
        raise ValueError(f"{inventory} must be an Inventory")
    return inventory


def build_inventory_job(
    inventory: Inventory, *, queue_item: QueueItemWithState, services: Services
) -> Job:
    return Job(inventory=inventory, queue_item=queue_item, services=services)


def build_resources_provider(
    item: ResourcesProviderItem, *, services: Services
) -> ResourcesProvider:
    klass = resources_provider_builtins.get(item.type)
    if klass is None:
        klass = extra_inspect.import_name(item.type)
    if klass is None:
        raise ValueError(f"Unknown resources provider type: {item.type}")
    if not issubclass(klass, ResourcesProvider):
        raise ValueError(f"{klass} must be a ResourcesProvider")
    options = {"name": item.name} | item.options
    return klass.from_options(options, services=services, definition=item)


from datalineup_engine.worker.inventories.batching import BatchingInventory
from datalineup_engine.worker.inventories.chained import ChainedInventory
from datalineup_engine.worker.inventories.dummy import DummyInventory
from datalineup_engine.worker.inventories.periodic import PeriodicInventory
from datalineup_engine.worker.inventories.static import StaticInventory
from datalineup_engine.worker.topics.batching import BatchingTopic
from datalineup_engine.worker.topics.delayed import DelayedTopic
from datalineup_engine.worker.topics.dummy import DummyTopic
from datalineup_engine.worker.topics.file import FileTopic
from datalineup_engine.worker.topics.logger import LoggingTopic
from datalineup_engine.worker.topics.memory import MemoryTopic
from datalineup_engine.worker.topics.null import NullTopic
from datalineup_engine.worker.topics.periodic import PeriodicTopic
from datalineup_engine.worker.topics.rabbitmq import RabbitMQTopic
from datalineup_engine.worker.topics.static import StaticTopic

BUILTINS: dict[str, t.Union[t.Type[Inventory], t.Type[Topic]]] = {
    "DummyInventory": DummyInventory,
    "StaticInventory": StaticInventory,
    "ChainedInventory": ChainedInventory,
    "PeriodicInventory": PeriodicInventory,
    "BatchingInventory": BatchingInventory,
    "DelayedTopic": DelayedTopic,
    "DummyTopic": DummyTopic,
    "FileTopic": FileTopic,
    "LoggingTopic": LoggingTopic,
    "MemoryTopic": MemoryTopic,
    "PeriodicTopic": PeriodicTopic,
    "RabbitMQTopic": RabbitMQTopic,
    "StaticTopic": StaticTopic,
    "BatchingTopic": BatchingTopic,
    "NullTopic": NullTopic,
}
