import pytest

from datalineup_engine.models import Queue
from datalineup_engine.worker_manager.config.declarative import load_definitions_from_str

static_job_definition: str = """
---
apiVersion: datalineup.khulnasoft.io/v1alpha1
kind: DatalineupJobDefinition
metadata:
  name: test-job-definition
  labels:
    owner: team-datalineup
spec:
  minimalInterval: "@weekly"
  template:
    input:
      inventory: test-inventory

    pipeline:
      name: something.datalineup.pipelines.aa.bb
---
apiVersion: datalineup.khulnasoft.io/v1alpha1
kind: DatalineupInventory
metadata:
  name: test-inventory
spec:
  type: testtype
---

"""


def test_join_definitions_should_raise_exception_on_undefined_job_object() -> None:
    with pytest.raises(NotImplementedError):
        static_definitions = load_definitions_from_str(static_job_definition)
        Queue(name="test").join_definitions(static_definitions)
