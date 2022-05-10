from __future__ import annotations

import datetime as dt
from typing import List, Sequence

import src as library


class InMemoryDBHelper(library.DBHelper):
    @classmethod
    def _create_audit_record(
        cls, audit_record: library.AuditRecord, as_at: dt.datetime
    ) -> library.AuditRecord:
        audit_record.id = 1
        audit_records_in_db.append(audit_record)
        return audit_record

    @classmethod
    def _create_context(
        cls,
        context: library.ProcessContext,
        as_at: dt.datetime,
        version: int | None = None,
    ) -> library.ProcessContext:
        if contexts_in_db:
            latest_pk = int(contexts_in_db[-1].id) + 1
        else:
            latest_pk = 1

        context.id = latest_pk

        contexts_in_db.append(context)
        return context

    @classmethod
    def _create_process(cls, process: library.Process, as_at: dt.datetime) -> library.Process:
        process.id = 1
        processes_in_db.append(process)
        return process

    @classmethod
    def _create_async_record(
        cls, async_record: library.ProcessAsyncRequest, as_at: dt.datetime
    ) -> library.ProcessAsyncRequest:
        if async_calls_in_db:
            latest_pk = int(contexts_in_db[-1].id) + 1
        else:
            latest_pk = 1

        async_record.id = latest_pk
        async_calls_in_db.append(async_record)
        return async_record

    # QUERIES

    @classmethod
    def get_process(cls, process_id: int | str) -> library.Process | None:
        """
        Get the process from the DB and transform it into a Process object.
        """
        for p in processes_in_db:
            if p.id == process_id:
                return p

        return None

    @classmethod
    def get_active_processes_for_idempotency_key(
        cls, *, idempotency_key: str, as_at: dt.datetime
    ) -> List[library.Process]:
        """
        Get the process from the DB and transform it into a Process object.
        """
        processes = []
        for process in processes_in_db:
            if process.idempotency_key == idempotency_key:
                processes.append(process)

        return processes

    @classmethod
    def get_async_record_by_process_id(
        cls, process_id: int | str
    ) -> Sequence[library.ProcessAsyncRequest]:
        records = []

        for r in async_calls_in_db:
            if r.process_id == process_id:
                records.append(r)
        return records

    @classmethod
    def get_async_record_by_correlation_id(
        cls, correlation_id: str
    ) -> library.ProcessAsyncRequest | None:
        for r in async_calls_in_db:
            if r.correlation_id == correlation_id:
                return r
        return None

    @classmethod
    def get_latest_context(cls, process_id: int | str) -> library.ProcessContext | None:
        for context in reversed(contexts_in_db):
            if context.process_id == process_id:
                return context

        return None

    # Setters / Updates

    @classmethod
    def set_async_response_as_received(
        cls, async_request: library.ProcessAsyncRequest
    ) -> library.ProcessAsyncRequest:
        for a in async_calls_in_db:
            if a.id == async_request.id:
                a.status = library.ProcessAsyncRequest.Status.RESPONSE_RECEIVED
                return a

        raise library.ProcessError("Async record not found.")

    @classmethod
    def update_process(cls, process: library.Process, as_at: dt.datetime) -> library.Process:
        id = process.id

        for p in processes_in_db:
            if p.id == id:
                p.name = process.name
                p.idempotency_key = process.idempotency_key
                p.status = process.status
                p.updated_at = as_at

            return p

        raise library.ProcessError("Process not found.")


class MockSystemClock(library.SystemClock):
    def now(self) -> dt.datetime:
        return dt.datetime.now()


# Mock DB

processes_in_db: List[library.Process] = []
audit_records_in_db: List[library.AuditRecord] = []
contexts_in_db: List[library.ProcessContext] = []
async_calls_in_db: List[library.ProcessAsyncRequest] = []
