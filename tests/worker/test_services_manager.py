from typing import Optional

import dataclasses

import pytest

from datalineup_engine.config import Config
from datalineup_engine.utils.inspect import get_import_name
from datalineup_engine.worker.services import BaseServices
from datalineup_engine.worker.services import MinimalService
from datalineup_engine.worker.services import Service
from datalineup_engine.worker.services.manager import ServicesManager


class FakeServiceWithConfig(Service[BaseServices, "FakeServiceWithConfig.Options"]):
    name = "fake_with_config"

    @dataclasses.dataclass
    class Options:
        a: int


class FakestService(MinimalService):
    name = "fake_with_config"


class FakeServiceWithServices(Service[BaseServices, None]):
    name = "fake_with_services"

    class Services:
        fake_with_config: FakeServiceWithConfig


class FakeService(Service["FakeService.Services", "FakeService.Options"]):
    name = "fake"

    class Services(BaseServices):
        config: Config
        fake_with_config: FakeServiceWithConfig

    @dataclasses.dataclass
    class Options:
        b: int

    state: Optional[str] = None

    async def open(self) -> None:
        assert self.services.fake_with_config.options.a is not None
        assert self.options.b is not None

        self.state = "opened"

    async def close(self) -> None:
        self.state = "closed"


@pytest.mark.asyncio
async def test_services_manager(config: Config) -> None:
    config = config.load_object(
        {
            "services_manager": {
                "services": [
                    get_import_name(FakeServiceWithConfig),
                    get_import_name(FakeService),
                ],
            },
            "fake_with_config": {"a": 1},
            "fake": {"b": 2},
        }
    )
    sm = ServicesManager(config)

    class FakeServices:
        fake: FakeService
        fake_with_config: FakeServiceWithConfig

    services = sm.services.cast(FakeServices)
    assert services.s.fake_with_config.options.a == 1
    assert services.s.fake.options.b == 2
    assert services.s.fake.state is None

    # Service Manager register service options to config.
    assert sm.services.s.config.cast_namespace("fake", FakeService.Options).b == 2

    await sm.open()
    assert services.s.fake.state == "opened"
    await sm.close()
    assert services.s.fake.state == "closed"


async def test_services_manager_check_options(config: Config) -> None:
    # Loading a services expecting config requires config.
    config = config.load_object(
        {
            "services_manager": {
                "services": [
                    get_import_name(FakeServiceWithConfig),
                ],
            },
        }
    )
    with pytest.raises(
        ValueError, match="Invalid service 'fake_with_config' configuration"
    ):
        ServicesManager(config)

    # But the config must also be valid
    config = config.load_object({"fake_with_config": {"a": "foo"}})
    with pytest.raises(
        ValueError, match="Invalid service 'fake_with_config' configuration"
    ):
        ServicesManager(config)

    # Finally, load if the config is right.
    config = config.load_object({"fake_with_config": {"a": 1}})
    ServicesManager(config)

    # Cannot load a service twice.
    config = config.load_object(
        {
            "services_manager": {
                "services": [
                    get_import_name(FakeServiceWithConfig),
                    get_import_name(FakeServiceWithConfig),
                ],
            },
        }
    )
    with pytest.raises(ValueError, match="Cannot load 'fake_with_config' twice"):
        ServicesManager(config)


async def test_services_manager_check_services(config: Config) -> None:
    # Loading a services expecting other services require these service to be loaded.
    config = config.load_object(
        {
            "services_manager": {
                "services": [
                    get_import_name(FakeServiceWithServices),
                ],
            },
        }
    )
    with pytest.raises(
        ValueError,
        match="Can't load services",
    ):
        ServicesManager(config)

    config = config.load_object(
        {
            "services_manager": {
                "services": [
                    get_import_name(FakestService),
                    get_import_name(FakeServiceWithServices),
                ],
            },
        }
    )
    ServicesManager(config)

    config = config.load_object(
        {
            "services_manager": {
                "services": [
                    get_import_name(FakeServiceWithConfig),
                    get_import_name(FakeServiceWithServices),
                ],
            },
            "fake_with_config": {"a": 1},
        }
    )
    ServicesManager(config)


async def test_default_services(config: Config) -> None:
    ServicesManager(config)
    assert True
