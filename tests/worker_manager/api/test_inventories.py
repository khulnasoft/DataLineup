from flask.testing import FlaskClient

from datalineup_engine.worker_manager.config.declarative import StaticDefinitions
from datalineup_engine.worker_manager.config.declarative import load_definitions_from_str


def test_api_inventories_empty(client: FlaskClient) -> None:
    resp = client.get("/api/inventories")
    assert resp.status_code == 200
    assert resp.json == {"items": []}


def test_api_inventories_loaded_from_file(
    client: FlaskClient,
    static_definitions: StaticDefinitions,
) -> None:
    new_definitions = load_definitions_from_str(
        """
apiVersion: datalineup.khulnasoft.io/v1alpha1
kind: DatalineupInventory
metadata:
  name: testinv
spec:
  type: something.datalineup.inventories.AA
  options:
    source: sourcename
"""
    )
    static_definitions.inventories = new_definitions.inventories
    resp = client.get("/api/inventories")
    assert resp.status_code == 200
    assert resp.json == {
        "items": [
            {
                "name": "testinv",
                "type": "something.datalineup.inventories.AA",
                "options": {"source": "sourcename"},
            },
        ]
    }
