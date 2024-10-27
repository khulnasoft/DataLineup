from datalineup_engine.utils.declarative_config import ObjectMetadata
from datalineup_engine.worker_manager.config.declarative_inventory import Inventory
from datalineup_engine.worker_manager.config.declarative_inventory import InventorySpec
from datalineup_engine.worker_manager.config.static_definitions import StaticDefinitions


def build(definitions: StaticDefinitions) -> None:
    definitions.add(
        Inventory(
            metadata=ObjectMetadata(name="test-inventory"),
            apiVersion="datalineup.khulnasoft.io/v1alpha1",
            kind="DatalineupInventory",
            spec=InventorySpec(type="testtype"),
        )
    )
