from typing import Iterator

import pytest

from datalineup_engine.worker_manager.config.static_definitions import StaticDefinitions


@pytest.fixture
def static_definitions() -> Iterator[StaticDefinitions]:
    new_definitions = StaticDefinitions()
    yield new_definitions
