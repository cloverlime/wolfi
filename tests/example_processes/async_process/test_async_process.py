from __future__ import annotations

import datetime as dt

import src as library

from .. import mock_db
from . import _async_process


class TestAsyncExampleProcess:
    def test_context_changes_in_all_stopping_points(self):
        """
        Test that the DB state is correct throughout a simple process where no exceptions and no async calls occur.
        """

        # Clear DB in example module.
        self._clear_db()
        process_definition = _async_process.AsyncExampleProcess

        _async_process.initiate_async_process(
            account_number="account_number1", house_id="foo", system_clock=dt.datetime
        )

        # Process
        processes = mock_db.InMemoryDBHelper.get_active_processes_for_idempotency_key(
            idempotency_key="account_number1", as_at=dt.datetime.now()
        )
        assert len(processes) == 1

        process = processes[0]

        assert process.name == process_definition.name
        assert process.status == library.Process.Status.IN_PROGRESS
        assert process.idempotency_key == "account_number1"

        # The process should have run as far as sending the first request.
        # The following section checks out all the DB state at this stage.

        # Contexts
        contexts = mock_db.contexts_in_db

        assert len(contexts) == 2

        initial_context = process_definition.get_context(contexts[0])
        assert initial_context == _async_process.AsyncExampleContext(
            # User submitted or specified
            house_id="foo",
            data_point_one="",
            data_point_two="",
            data_point_three=None,
            alert_code="",
            # Automatic population
            send_first_request_correlation_id="",
            send_first_request_has_response_received=False,
            send_first_request_is_complete=False,
            #
            send_second_request_correlation_id="",
            send_second_request_has_response_received=False,
            send_second_request_is_complete=False,
            #
            send_evening_request_is_queued=False,
            send_evening_request_is_triggered_from_task_queue=False,
            send_evening_request_is_complete=False,
            #
            queue_up_parallel_tasks_is_all_queued=False,
            #
            parallel_step_one_is_queued=False,
            parallel_step_one_is_triggered_from_task_queue=False,
            parallel_step_one_is_complete=False,
            #
            parallel_step_two_is_queued=False,
            parallel_step_two_is_triggered_from_task_queue=False,
            parallel_step_two_is_complete=False,
            #
            queue_up_parallel_tasks_is_complete=False,
            #
            final_step_is_complete=False,
            wait_for_alert_is_complete=False,
        )

        assert not process_definition.is_complete(initial_context)

        first_stopped_context = process_definition.get_context(contexts[-1])

        assert_specified_context_change(
            earlier=initial_context,
            later=first_stopped_context,
            changes=dict(
                send_first_request_correlation_id="correlation_id1",
            ),
        )

        assert not process_definition.is_complete(first_stopped_context)

        # Async calls
        async_records = mock_db.InMemoryDBHelper.get_async_record_by_process_id(process.id)
        assert len(async_records) == 1

        first_request_record = async_records[0]
        assert first_request_record.status == library.ProcessAsyncRequest.Status.AWAITING_RESPONSE
        assert first_request_record.correlation_id == "correlation_id1"

        # ----------------------------------
        # Receive response to first request.
        # ----------------------------------

        library.handle_response_received(
            correlation_id="correlation_id1",
            idempotency_keys=[],
            response=None,
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
        )
        second_stopped_context = process_definition.get_context(contexts[-1])
        assert_specified_context_change(
            earlier=first_stopped_context,
            later=second_stopped_context,
            changes=dict(
                send_first_request_has_response_received=True,
                send_first_request_is_complete=True,
                data_point_one="foo",
                data_point_two="bar",
                send_second_request_correlation_id="correlation_id2",
            ),
        )

        # ----------------------------------
        # Receive response to second request.
        # ----------------------------------

        library.handle_response_received(
            correlation_id="correlation_id2",
            idempotency_keys=[],
            response=None,
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
        )
        third_stopped_context = process_definition.get_context(contexts[-1])
        assert_specified_context_change(
            earlier=second_stopped_context,
            later=third_stopped_context,
            changes=dict(
                send_second_request_has_response_received=True,
                send_second_request_is_complete=True,
                send_evening_request_is_queued=True,
            ),
        )
        # ------------------------------
        # Receive unsolicited event now.
        # ------------------------------

        library.handle_response_received(
            correlation_id="",
            idempotency_keys=["account_number1"],
            response={"alert": {"code": "fizz"}},
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
        )
        fourth_stopped_context = process_definition.get_context(contexts[-1])
        assert_specified_context_change(
            earlier=third_stopped_context,
            later=fourth_stopped_context,
            changes=dict(
                wait_for_alert_is_complete=True,
                alert_code="fizz",
            ),
        )

        # -----------------------------------
        # Task queue triggers evening request
        # -----------------------------------

        library.perform_queued_task(
            process_id=process.id,
            step_name=_async_process.ScheduleEveningRequest.get_name(),
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
        )
        fifth_stopped_context = process_definition.get_context(contexts[-1])
        assert_specified_context_change(
            earlier=fourth_stopped_context,
            later=fifth_stopped_context,
            changes=dict(
                send_evening_request_is_triggered_from_task_queue=True,
                send_evening_request_is_complete=True,
                parallel_step_one_is_queued=True,
                parallel_step_two_is_queued=True,
                queue_up_parallel_tasks_is_all_queued=True,
            ),
        )

        # ----------------------------------------------
        # Parallel step one gets triggered and completed
        # ----------------------------------------------
        library.perform_queued_task(
            process_id=process.id,
            step_name=_async_process.ScheduleParallelStepOne.get_name(),
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
        )
        sixth_stopped_context = process_definition.get_context(contexts[-1])

        assert_specified_context_change(
            earlier=fifth_stopped_context,
            later=sixth_stopped_context,
            changes=dict(
                parallel_step_one_is_triggered_from_task_queue=True,
                parallel_step_one_is_complete=True,
            ),
        )
        self._clear_db()

    def _clear_db(self):
        mock_db.processes_in_db = []
        mock_db.audit_records_in_db = []
        mock_db.contexts_in_db = []
        mock_db.async_calls_in_db = []
        return


def assert_specified_context_change(
    *,
    changes: dict,
    additions: dict | None = None,
    earlier: library.Context,
    later: library.Context,
):
    """
    Given a list of changes, assert that they've all changed in the `later_context` and the remaining keys
    have the same values in both contexts.
    """
    # TODO Allow this sort of access on the class.
    keys = set(earlier.__dict__.keys())
    later_keys = set(later.__dict__.keys())

    assert keys == later_keys

    changed_keys = set(changes.keys())

    if additions:
        for k, v in additions.items():
            if k not in later_keys:
                raise KeyError(f"New key '{k}' was not found in the later context!")

            if k in keys:
                raise KeyError(f"{k} was unexpectedly found in the older context.")

            assert later[k] == v

    if not changed_keys.issubset(keys):
        raise KeyError(
            f"Expected a key from '{changed_keys}' to have changed that didn't already exist."
        )

    for k, v in changes.items():
        if not later[k] == v:
            raise KeyError(f"Expected key '{k}' to have value '{str(v)}' in later context.")
        if not earlier[k] != v:
            raise KeyError(f"Expected key '{k}' to have value '{str(v)}' in earlier context.")
        keys.remove(k)

    # Now all remaining keys should return identical values from both contexts.

    for k in keys:
        if not earlier[k] == later[k]:
            raise KeyError(
                f"Expected same values for '{k}' but got '{earlier[k]}' and '{later[k]}'"
            )
