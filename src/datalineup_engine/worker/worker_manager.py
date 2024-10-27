import asyncio

from sqlalchemy.orm import sessionmaker

from datalineup_engine.client.worker_manager import AbstractWorkerManagerClient
from datalineup_engine.config import Config
from datalineup_engine.core.api import FetchCursorsStatesInput
from datalineup_engine.core.api import FetchCursorsStatesResponse
from datalineup_engine.core.api import JobsStatesSyncInput
from datalineup_engine.core.api import JobsStatesSyncResponse
from datalineup_engine.core.api import LockInput
from datalineup_engine.core.api import LockResponse
from datalineup_engine.stores import jobs_store
from datalineup_engine.worker_manager.context import WorkerManagerContext
from datalineup_engine.worker_manager.services.lock import lock_jobs
from datalineup_engine.worker_manager.services.sync import sync_jobs


class StandaloneWorkerManagerClient(AbstractWorkerManagerClient):
    def __init__(
        self,
        *,
        config: Config,
        sessionmaker: sessionmaker,
    ) -> None:
        self.sessionmaker = sessionmaker
        self.worker_id = config.c.worker_id
        self.max_assigned_items = config.c.worker_manager.work_items_per_worker

        self.context = WorkerManagerContext(config=config.c.worker_manager)
        with self.sessionmaker() as session:
            self.context.load_static_definition(session=session)

    async def lock(self) -> LockResponse:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            self._sync_lock,
        )

    async def sync(self, sync_input: JobsStatesSyncInput) -> JobsStatesSyncResponse:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            self._sync_state,
            sync_input,
        )

    async def fetch_cursors_states(
        self, cursors: FetchCursorsStatesInput
    ) -> FetchCursorsStatesResponse:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            self._sync_fetch_cursors_states,
            cursors,
        )

    async def sync_jobs(self) -> None:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            self._sync_jobs,
        )

    def _sync_lock(self) -> LockResponse:
        with self.sessionmaker() as session:
            lock = lock_jobs(
                LockInput(worker_id=self.worker_id),
                max_assigned_items=self.max_assigned_items,
                static_definitions=self.context.static_definitions,
                session=session,
            )
            session.commit()
            return lock

    def _sync_state(self, sync_input: JobsStatesSyncInput) -> JobsStatesSyncResponse:
        with self.sessionmaker() as session:
            jobs_store.sync_jobs_states(
                state=sync_input.state,
                session=session,
            )
            session.commit()
            return JobsStatesSyncResponse()

    def _sync_fetch_cursors_states(
        self, cursors_input: FetchCursorsStatesInput
    ) -> FetchCursorsStatesResponse:
        with self.sessionmaker() as session:
            cursors = jobs_store.fetch_cursors_states(
                cursors_input.cursors,
                session=session,
            )
            return FetchCursorsStatesResponse(cursors=cursors)

    def _sync_jobs(self) -> None:
        with self.sessionmaker() as session:
            # We reset static definition at each jobs sync
            self.context.load_static_definition(session=session)
            sync_jobs(
                static_definitions=self.context.static_definitions,
                session=session,
            )
            session.commit()
