import typing as t

import datetime

from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.orm import joinedload

from datalineup_engine.models import Queue
from datalineup_engine.utils.sqlalchemy import AnySession
from datalineup_engine.utils.sqlalchemy import AnySyncSession


def create_queue(
    *,
    session: AnySession,
    name: str,
) -> Queue:
    queue = Queue(name=name)
    session.add(queue)
    return queue


def get_assigned_queues(
    *,
    session: AnySyncSession,
    worker_id: str,
    assigned_after: datetime.datetime,
    selector: t.Optional[str] = None,
) -> list[Queue]:
    extra_filters = []
    if selector:
        extra_filters.append(Queue.name.regexp_match(selector))
    assigned_jobs: t.Sequence[Queue] = (
        session.execute(
            select(Queue)
            .options(joinedload(Queue.job))
            .where(
                Queue.enabled.is_(True),
                Queue.assigned_to == worker_id,
                Queue.assigned_at >= assigned_after,
                *extra_filters,
            )
            .order_by(Queue.name)
        )
        .scalars()
        .all()
    )
    return list(assigned_jobs)


def get_unassigned_queues(
    *,
    session: AnySyncSession,
    assigned_before: datetime.datetime,
    selector: t.Optional[str] = None,
    limit: int | None,
) -> list[Queue]:
    extra_filters = []
    if selector:
        extra_filters.append(Queue.name.regexp_match(selector))

    query = (
        select(Queue)
        .options(joinedload(Queue.job))
        .where(
            Queue.enabled.is_(True),
            or_(
                Queue.assigned_at.is_(None),
                Queue.assigned_at < assigned_before,
            ),
            *extra_filters,
        )
    )
    if limit:
        query = query.limit(limit=limit)

    unassigned_queues: t.Sequence[Queue] = session.execute(query).scalars().all()
    return list(unassigned_queues)


def disable_queue(
    *,
    name: str,
    session: AnySyncSession,
) -> None:
    session.execute(update(Queue).where(Queue.name == name).values(enabled=False))
