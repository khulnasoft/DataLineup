import typing as t

import dataclasses

from datalineup_engine.core import api
from datalineup_engine.utils.declarative_config import BaseObject
from datalineup_engine.worker_manager.config.declarative_job import JobSpec

from .static_definitions import StaticDefinitions

JOB_DEFINITION_KIND: t.Final[str] = "DatalineupJobDefinition"


@dataclasses.dataclass
class JobDefinitionSpec:
    template: JobSpec
    minimalInterval: str


@dataclasses.dataclass(kw_only=True)
class JobDefinition(BaseObject):
    spec: JobDefinitionSpec
    kind: str = JOB_DEFINITION_KIND

    def to_core_objects(
        self,
        static_definitions: StaticDefinitions,
    ) -> t.Iterator[api.JobDefinition]:
        for template in self.spec.template.to_core_objects(
            self.metadata.name,
            self.metadata.labels,
            static_definitions,
        ):
            yield api.JobDefinition(
                name=template.name,
                template=template,
                minimal_interval=self.spec.minimalInterval,
            )
