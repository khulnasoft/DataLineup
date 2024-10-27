import threading

from flask import Blueprint

from datalineup_engine.core.api import LockInput
from datalineup_engine.core.api import LockResponse
from datalineup_engine.database import session_scope
from datalineup_engine.utils.flask import Json
from datalineup_engine.utils.flask import jsonify
from datalineup_engine.utils.flask import marshall_request
from datalineup_engine.worker_manager.app import current_app
from datalineup_engine.worker_manager.services.lock import lock_jobs

bp = Blueprint("lock", __name__, url_prefix="/api/lock")

_LOCK_LOCK = threading.Lock()


@bp.route("", methods=("POST",))
def post_lock() -> Json[LockResponse]:
    with _LOCK_LOCK:
        lock_input = marshall_request(LockInput)
        max_assigned_items: int = current_app.datalineup.config.work_items_per_worker
        static_definitions = current_app.datalineup.static_definitions
        with session_scope() as session:
            lock_response = lock_jobs(
                lock_input,
                max_assigned_items=max_assigned_items,
                static_definitions=static_definitions,
                session=session,
            )

        return jsonify(lock_response)
