import datetime as dt
from typing import Any

import src as library
from tests.example_processes import mock_db


class StubContext(library.Context):
    pass


class _SyncTargetStep(library.ScheduledSynchronousStep[StubContext]):
    _name = "do_sync_task"

    def _call(self, context: StubContext) -> StubContext | None:
        return context


class PutSyncTaskOnQueue(library.TaskQueueStep):
    target_step = _SyncTargetStep

    def _add_to_task_queue(self, context: library.Context) -> None:
        return None


class TestTaskQueueStepWithSyncStep:
    def test_initialise_expected_values(self):
        initial_context = PutSyncTaskOnQueue.initialise(StubContext())
        assert initial_context == StubContext(
            do_sync_task_is_queued=False,
            do_sync_task_is_triggered_from_task_queue=False,
            do_sync_task_is_complete=False,
        )

    def test_call(self):
        initial_context = PutSyncTaskOnQueue.initialise(StubContext())

        step = PutSyncTaskOnQueue(
            process_id=1,
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
        )
        step.call(initial_context)

        latest_process_context = mock_db.contexts_in_db[-1]
        latest_context = StubContext.deserialize(latest_process_context.context)

        assert latest_context == StubContext(
            do_sync_task_is_queued=True,
            do_sync_task_is_triggered_from_task_queue=False,
            do_sync_task_is_complete=False,
        )

        triggered_context = step.set_as_triggered(latest_context)

        final_context = step.call(triggered_context)

        assert final_context == StubContext(
            do_sync_task_is_queued=True,
            do_sync_task_is_triggered_from_task_queue=True,
            do_sync_task_is_complete=True,
        )
        _clear_db()


class _AsyncTargetStep(library.ScheduledAsyncStep[StubContext]):
    _name = "do_async_task"

    def _make_request(self, context: StubContext) -> str:
        return "foo"

    def _handle_response(self, context: StubContext, response: Any) -> StubContext | None:
        return context


class PutAsyncTaskOnQueue(library.TaskQueueStep):
    target_step = _AsyncTargetStep

    def _add_to_task_queue(self, context: library.Context) -> None:
        return None


class TestTaskQueueWithAsyncStep:
    def test_initialise_expected_values(self):
        initial_context = PutAsyncTaskOnQueue.initialise(StubContext())
        assert initial_context == StubContext(
            do_async_task_is_queued=False,
            do_async_task_is_triggered_from_task_queue=False,
            do_async_task_correlation_id="",
            do_async_task_has_response_received=False,
            do_async_task_is_complete=False,
        )

    def test_call(self):
        initial_context = PutAsyncTaskOnQueue.initialise(StubContext())

        step = PutAsyncTaskOnQueue(
            process_id=1,
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
        )
        step.call(initial_context)

        latest_process_context = mock_db.contexts_in_db[-1]
        latest_context = StubContext.deserialize(latest_process_context.context)
        assert latest_context == StubContext(
            do_async_task_is_queued=True,
            do_async_task_is_triggered_from_task_queue=False,
            do_async_task_correlation_id="",
            do_async_task_has_response_received=False,
            do_async_task_is_complete=False,
        )

        triggered_context = step.set_as_triggered(latest_context)
        step.call(triggered_context)

        next_process_context = mock_db.contexts_in_db[-1]
        next_context = StubContext.deserialize(next_process_context.context)

        assert next_context == StubContext(
            do_async_task_is_queued=True,
            do_async_task_is_triggered_from_task_queue=True,
            do_async_task_correlation_id="foo",
            do_async_task_has_response_received=False,
            do_async_task_is_complete=False,
        )

        # Response is received
        final_context = step.target_step(
            process_id=1, db=mock_db.InMemoryDBHelper, system_clock=dt.datetime
        ).handle_response(
            context=next_context,
            correlation_id="foo",
            response=None,
        )

        assert final_context == StubContext(
            do_async_task_is_queued=True,
            do_async_task_is_triggered_from_task_queue=True,
            do_async_task_correlation_id="foo",
            do_async_task_has_response_received=True,
            do_async_task_is_complete=True,
        )
        _clear_db()


def _clear_db():
    mock_db.processes_in_db = []
    mock_db.audit_records_in_db = []
    mock_db.contexts_in_db = []
    mock_db.async_calls_in_db = []
