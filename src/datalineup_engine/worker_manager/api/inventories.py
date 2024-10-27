from flask import Blueprint

from datalineup_engine.core.api import InventoriesResponse
from datalineup_engine.utils.flask import Json
from datalineup_engine.utils.flask import jsonify
from datalineup_engine.worker_manager.app import current_app

bp = Blueprint("inventories", __name__, url_prefix="/api/inventories")


@bp.route("", methods=("GET",))
def get_inventories() -> Json[InventoriesResponse]:
    inventories = list(current_app.datalineup.static_definitions.inventories.values())
    return jsonify(InventoriesResponse(items=inventories))
