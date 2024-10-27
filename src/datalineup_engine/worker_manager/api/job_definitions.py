from flask import Blueprint

from datalineup_engine.core.api import JobDefinitionsResponse
from datalineup_engine.utils.flask import Json
from datalineup_engine.utils.flask import jsonify
from datalineup_engine.worker_manager.app import current_app

bp = Blueprint("job_definitions", __name__, url_prefix="/api/job_definitions")


@bp.route("", methods=("GET",))
def get_job_definitions() -> Json[JobDefinitionsResponse]:
    job_definitions = list(
        current_app.datalineup.static_definitions.job_definitions.values()
    )
    return jsonify(JobDefinitionsResponse(items=job_definitions))
