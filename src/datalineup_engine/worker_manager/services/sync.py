import logging
import threading
import time
from datetime import datetime

from croniter import croniter

from datalineup_engine.stores import jobs_store
from datalineup_engine.stores import queues_store
from datalineup_engine.utils import utcnow
from datalineup_engine.utils.sqlalchemy import AnySyncSession
from datalineup_engine.worker_manager.config.declarative import StaticDefinitions

_SYNC_LOCK = threading.Lock()


def sync_jobs(
    *,
    static_definitions: StaticDefinitions,
    session: AnySyncSession,
) -> None:
    if not _SYNC_LOCK.locked():
        with _SYNC_LOCK:
            _sync_jobs(static_definitions=static_definitions, session=session)


def _sync_jobs(
    *,
    static_definitions: StaticDefinitions,
    session: AnySyncSession,
) -> None:
    logger = logging.getLogger(__name__)

    # Jobs with no interval
    for datalineup_job in static_definitions.jobs.values():
        try:
            # Check if job exists and create if not
            existing_job = jobs_store.get_job(datalineup_job.name, session)
            if not existing_job:
                job_queue = queues_store.create_queue(
                    session=session, name=datalineup_job.name
                )
                jobs_store.create_job(
                    name=datalineup_job.name,
                    session=session,
                    queue_name=job_queue.name,
                )
        except Exception:
            logger.exception("Failed to create %s", datalineup_job.name)

    # Jobs ran at an interval
    for job_definition in static_definitions.job_definitions.values():
        try:
            last_job = jobs_store.get_last_job(
                session=session,
                job_definition_name=job_definition.name,
            )

            if last_job:
                # If a job already exists, check it has completed and
                # the interval has elapsed to start a new one.
                if not last_job.completed_at:
                    continue

                # If the last job completed with success, we check for
                # the job definition interval.
                if not last_job.error:
                    scheduled_at = croniter(
                        job_definition.minimal_interval,
                        last_job.started_at,
                    ).get_next(ret_type=datetime)
                    if scheduled_at > utcnow():
                        continue

            job_name: str = f"{job_definition.name}-{int(time.time())}"
            queue = queues_store.create_queue(session=session, name=job_name)
            job = jobs_store.create_job(
                name=job_name,
                session=session,
                queue_name=queue.name,
                job_definition_name=job_definition.name,
            )

            # If the last job was an error, we resume from where we were.
            if last_job and last_job.error:
                job.cursor = last_job.cursor

            session.commit()
        except Exception:
            logger.exception("Failed to create %s", job_definition.name)
