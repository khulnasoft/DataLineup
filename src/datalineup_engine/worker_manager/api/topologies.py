from flask import Blueprint

from datalineup_engine.database import session_scope
from datalineup_engine.stores import topologies_store
from datalineup_engine.utils.declarative_config import BaseObject
from datalineup_engine.utils.flask import Json
from datalineup_engine.utils.flask import jsonify
from datalineup_engine.utils.flask import marshall_request

bp = Blueprint("topologies", __name__, url_prefix="/api/topologies")


@bp.route("/patch", methods=("PUT",))
def put_patch() -> Json[BaseObject]:
    patch = marshall_request(BaseObject)
    with session_scope() as session:
        saved_patch = topologies_store.patch(session=session, patch=patch)
        return jsonify(saved_patch.as_base_object())
