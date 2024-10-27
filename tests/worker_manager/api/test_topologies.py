from unittest import mock

from flask.testing import FlaskClient
from sqlalchemy.orm import Session

from datalineup_engine.worker_manager.app import DatalineupApp


def test_put_topology_patch(client: FlaskClient) -> None:
    resp = client.put(
        "/api/topologies/patch",
        json={
            "apiVersion": "datalineup.khulnasoft.io/v1alpha1",
            "kind": "DatalineupTopic",
            "metadata": {"name": "test-topic"},
            "spec": {"type": "RabbitMQTopic", "options": {"queue_name": "queue_1"}},
        },
    )
    assert resp.status_code == 200
    assert resp.json == {
        "apiVersion": "datalineup.khulnasoft.io/v1alpha1",
        "kind": "DatalineupTopic",
        "metadata": {"name": "test-topic", "labels": {}},
        "spec": {"type": "RabbitMQTopic", "options": {"queue_name": "queue_1"}},
    }

    # We add a new patch, overriding the last one
    resp = client.put(
        "/api/topologies/patch",
        json={
            "apiVersion": "datalineup.khulnasoft.io/v1alpha1",
            "kind": "DatalineupTopic",
            "metadata": {"name": "test-topic"},
            "spec": {"type": "RabbitMQTopic", "options": {"queue_name": "queue_2"}},
        },
    )
    assert resp.status_code == 200
    assert resp.json == {
        "apiVersion": "datalineup.khulnasoft.io/v1alpha1",
        "kind": "DatalineupTopic",
        "metadata": {"name": "test-topic", "labels": {}},
        "spec": {"type": "RabbitMQTopic", "options": {"queue_name": "queue_2"}},
    }


def test_put_topology_patch_ensure_topology_changed(
    tmp_path: str, app: DatalineupApp, client: FlaskClient, session: Session
) -> None:
    topology = """
apiVersion: datalineup.khulnasoft.io/v1alpha1
kind: DatalineupExecutor
metadata:
  name: default
spec:
  type: ARQExecutor
  options:
    redis_url: "redis://redis"
    queue_name: "arq:datalineup-default"
    redis_pool_args:
      max_connections: 10000
    concurrency: 108
---
apiVersion: datalineup.khulnasoft.io/v1alpha1
kind: DatalineupInventory
metadata:
    name: test-inventory
spec:
    type: testtype
---
apiVersion: datalineup.khulnasoft.io/v1alpha1
kind: DatalineupJobDefinition
metadata:
    name: job_1
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
    """
    with open(f"{tmp_path}/topology.yaml", "+w") as f:
        f.write(topology)

    app.datalineup.config.static_definitions_directories = [tmp_path]
    app.datalineup.load_static_definition(session=session)

    resp = client.post("/api/jobs/sync")
    assert resp.status_code == 200
    assert resp.json == {}
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.json == {
        "executors": [
            {
                "name": "default",
                "options": {
                    "concurrency": 108,
                    "queue_name": "arq:datalineup-default",
                    "redis_pool_args": {"max_connections": 10000},
                    "redis_url": "redis://redis",
                },
                "type": "ARQExecutor",
            }
        ],
        "items": [
            {
                "config": {},
                "executor": "default",
                "input": {"name": "test-inventory", "options": {}, "type": "testtype"},
                "labels": {
                    "owner": "team-datalineup",
                    "internal.job-definition-name": "job_1",
                },
                "name": mock.ANY,
                "output": {},
                "pipeline": {
                    "args": {},
                    "info": {
                        "name": "something.datalineup.pipelines.aa.bb",
                        "resources": {},
                    },
                },
                "state": {
                    "cursor": None,
                    "started_at": mock.ANY,
                },
            }
        ],
        "resources": [],
        "resources_providers": [],
    }

    # Let's change the pipeline name
    resp = client.put(
        "/api/topologies/patch",
        json={
            "apiVersion": "datalineup.khulnasoft.io/v1alpha1",
            "kind": "DatalineupJobDefinition",
            "metadata": {"name": "job_1"},
            "spec": {
                "template": {
                    "pipeline": {"name": "something.else.datalineup.pipelines.aa.bb"},
                },
            },
        },
    )

    # And reset the static definition
    session.commit()
    app.datalineup.load_static_definition(session=session)

    # Make sure we have the new topology version
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.json == {
        "executors": [
            {
                "name": "default",
                "options": {
                    "concurrency": 108,
                    "queue_name": "arq:datalineup-default",
                    "redis_pool_args": {"max_connections": 10000},
                    "redis_url": "redis://redis",
                },
                "type": "ARQExecutor",
            }
        ],
        "items": [
            {
                "config": {},
                "executor": "default",
                "input": {"name": "test-inventory", "options": {}, "type": "testtype"},
                "labels": {
                    "owner": "team-datalineup",
                    "internal.job-definition-name": "job_1",
                },
                "name": mock.ANY,
                "output": {},
                "pipeline": {
                    "args": {},
                    "info": {
                        "name": "something.else.datalineup.pipelines.aa.bb",
                        "resources": {},
                    },
                },
                "state": {
                    "cursor": None,
                    "started_at": mock.ANY,
                },
            }
        ],
        "resources": [],
        "resources_providers": [],
    }
