from typing import Optional

from datalineup_engine.config import Config
from datalineup_engine.config import default_config_with_env
from datalineup_engine.database import create_all
from datalineup_engine.database import scoped_session
from datalineup_engine.database import session_scope
from datalineup_engine.utils.flask import register_http_exception_error_handler
from datalineup_engine.worker_manager.app import DatalineupApp
from datalineup_engine.worker_manager.app import current_app
from datalineup_engine.worker_manager.context import WorkerManagerContext
from datalineup_engine.worker_manager.services.sync import sync_jobs


def get_app(
    datalineup_config: Config | None = None,
    app_config: Optional[dict] = None,
) -> DatalineupApp:
    config = default_config_with_env()
    if datalineup_config:
        config = config.load_object(datalineup_config.c)

    worker_manager_context = WorkerManagerContext(config=config.c.worker_manager)

    app = DatalineupApp(
        worker_manager_context,
        __name__,
    )

    if app_config:
        app.config.from_mapping(app_config)

    from .api.inventories import bp as bp_inventories
    from .api.job_definitions import bp as bp_job_definitions
    from .api.jobs import bp as bp_jobs
    from .api.lock import bp as bp_lock
    from .api.status import bp as bp_status
    from .api.topics import bp as bp_topics
    from .api.topologies import bp as bp_topologies

    app.register_blueprint(bp_status)
    app.register_blueprint(bp_jobs)
    app.register_blueprint(bp_job_definitions)
    app.register_blueprint(bp_topics)
    app.register_blueprint(bp_lock)
    app.register_blueprint(bp_inventories)
    app.register_blueprint(bp_topologies)

    @app.teardown_appcontext  # type: ignore
    def shutdown_session(response_or_exc: Optional[BaseException]) -> None:
        scoped_session().remove()

    register_http_exception_error_handler(app)

    return app


def init_all(app: Optional[DatalineupApp] = None) -> None:
    if app is None:
        app = get_app()

    with app.app_context():
        create_all()
        with session_scope() as session:
            current_app.datalineup.load_static_definition(session=session)
            sync_jobs(
                static_definitions=current_app.datalineup.static_definitions,
                session=session,
            )


def main() -> None:
    app = get_app()
    init_all(app=app)
    app.run(
        host=app.datalineup.config.flask_host,
        port=app.datalineup.config.flask_port,
    )


if __name__ == "__main__":
    main()
