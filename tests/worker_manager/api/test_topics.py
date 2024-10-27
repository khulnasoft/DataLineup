from flask.testing import FlaskClient

from datalineup_engine.worker_manager.config.declarative import StaticDefinitions
from datalineup_engine.worker_manager.config.declarative import load_definitions_from_str


def test_api_topics_empty(client: FlaskClient) -> None:
    resp = client.get("/api/topics")
    assert resp.status_code == 200
    assert resp.json == {"items": []}


def test_api_topics_loaded_from_str(
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
kind: DatalineupTopic
metadata:
  name: test-topic-2
spec:
  type: FileTopic
  options:
    path: "-"
    mode: "w"
"""
    )
    static_definitions.topics = new_definitions.topics
    resp = client.get("/api/topics")
    assert resp.status_code == 200
    assert resp.json == {
        "items": [
            {"name": "test-topic", "options": {}, "type": "RabbitMQTopic"},
            {
                "name": "test-topic-2",
                "options": {"mode": "w", "path": "-"},
                "type": "FileTopic",
            },
        ]
    }
