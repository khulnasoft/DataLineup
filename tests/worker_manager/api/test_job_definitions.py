from flask.testing import FlaskClient

from datalineup_engine.worker_manager.config.declarative import StaticDefinitions
from datalineup_engine.worker_manager.config.declarative import load_definitions_from_str


def test_api_job_definitions_empty(client: FlaskClient) -> None:
    resp = client.get("/api/job_definitions")
    assert resp.status_code == 200
    assert resp.json == {"items": []}


def test_api_job_definitions_loaded_from_str(
    client: FlaskClient,
    static_definitions: StaticDefinitions,
) -> None:
    new_definitions = load_definitions_from_str(
        """
apiVersion: datalineup.khulnasoft.io/v1alpha1
kind: DatalineupTopic
metadata:
  name: test-topic
spec:
  type: RabbitMQTopic
  options: {}
---
apiVersion: datalineup.khulnasoft.io/v1alpha1
kind: DatalineupInventory
metadata:
  name: test-inventory
spec:
  type: testtype
  options: {}
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

    output:
      default:
        - topic: test-topic

    pipeline:
      name: something.datalineup.pipelines.aa.bb
      resources: {"api_key": "GithubApiKey"}

    config:
      tracer:
        sampler:
          type: TraceIdRatioBased
          options: {ratio: 0.1}
"""
    )
    static_definitions.job_definitions = new_definitions.job_definitions
    resp = client.get("/api/job_definitions")
    assert resp.status_code == 200
    assert resp.json == {
        "items": [
            {
                "minimal_interval": "@weekly",
                "name": "test-job-definition",
                "template": {
                    "input": {
                        "name": "test-inventory",
                        "options": {},
                        "type": "testtype",
                    },
                    "name": "test-job-definition",
                    "executor": "default",
                    "output": {
                        "default": [
                            {
                                "name": "test-topic",
                                "options": {},
                                "type": "RabbitMQTopic",
                            }
                        ]
                    },
                    "pipeline": {
                        "args": {},
                        "info": {
                            "name": "something.datalineup.pipelines.aa.bb",
                            "resources": {"api_key": "GithubApiKey"},
                        },
                    },
                    "labels": {
                        "owner": "team-datalineup",
                        "internal.job-definition-name": "test-job-definition",
                    },
                    "config": {
                        "tracer": {
                            "sampler": {
                                "type": "TraceIdRatioBased",
                                "options": {"ratio": 0.1},
                            }
                        }
                    },
                },
            }
        ]
    }
