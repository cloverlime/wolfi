import datetime as dt
from typing import Any

import pytest
import src as library
from tests.example_processes import mock_db


class StubContext(library.Context):
    pass


class _TargetStepOne(library.ScheduledSynchronousStep):
    _name = "step_one"

    def _call(self, context: StubContext) -> StubContext | None:
        return context


class _TargetStepTwo(library.ScheduledSynchronousStep):
    _name = "step_two"

    def _call(self, context: StubContext) -> StubContext | None:
        return context


class StepOne(library.TaskQueueStep):
    target_step = _TargetStepOne

    def _add_to_task_queue(self, context: StubContext) -> None:
        return None


class StepTwo(library.TaskQueueStep):
    target_step = _TargetStepTwo

    def _add_to_task_queue(self, context: StubContext) -> None:
        return None


class TestCreationOfAggregateSteps:
    def test_schedule_simultaneously_creates_an_aggregate_class(self):
        aggregate_class = library.schedule_simultaneously(
            StepOne, StepTwo, name_of_step="aggregate_step_name"
        )

        assert aggregate_class.get_name() == "aggregate_step_name"
        assert StepOne in aggregate_class.steps
        assert StepTwo in aggregate_class.steps
        assert len(aggregate_class.steps) == 2

        aggregate_step = aggregate_class(
            db=mock_db.InMemoryDBHelper, system_clock=dt.datetime, process_id=123
        )

        assert isinstance(aggregate_step, library.Step)
        assert isinstance(aggregate_step, library._library._AggregateStep)

    def test_works_with_only_one_step(self):
        aggregate_class = library.schedule_simultaneously(
            StepOne, name_of_step="aggregate_step_name"
        )

        assert aggregate_class.get_name() == "aggregate_step_name"
        assert StepOne in aggregate_class.steps
        assert len(aggregate_class.steps) == 1

        aggregate_step = aggregate_class(
            db=mock_db.InMemoryDBHelper, system_clock=dt.datetime, process_id=123
        )
        assert isinstance(aggregate_step, library.Step)
        assert isinstance(aggregate_step, library._library._AggregateStep)

    def test_raises_with_no_steps(self):
        with pytest.raises(library.ValidationError):
            library.schedule_simultaneously(name_of_step="test")

    def test_raises_assertion_error_if_steps_are_wrong_type(self):
        with pytest.raises(AssertionError):
            library.schedule_simultaneously("baa", 4, name_of_step="test")


class TestCallAggregateStepWithSynchronousSteps:
    def test_modifies_context_correctly(self):
        self._clear_db()

        class AggregateProcess(library.ProcessDefinition):
            name = "aggregate_process"
            context_class = StubContext
            steps = [
                library.schedule_simultaneously(StepOne, StepTwo, name_of_step="aggregate_step")
            ]

        library.register_process_definition(AggregateProcess)

        library.initiate_process(
            idempotency_key="fizz",
            process_definition=AggregateProcess,
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
            starting_context_values={},
        )
        latest_process_context = mock_db.contexts_in_db[-1]
        latest_context = AggregateProcess.get_context(latest_process_context)

        assert latest_context == StubContext(
            aggregate_step_is_complete=False,
            aggregate_step_is_all_queued=True,
            step_one_is_queued=True,
            step_one_is_triggered_from_task_queue=False,
            step_one_is_complete=False,
            step_two_is_queued=True,
            step_two_is_triggered_from_task_queue=False,
            step_two_is_complete=False,
        )
        # Test idempotency
        process = mock_db.processes_in_db[0]
        library.run_process(
            process=process,
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
        )

        process_context_after_rerun = mock_db.contexts_in_db[-1]
        context_after_rerun = AggregateProcess.get_context(process_context_after_rerun)
        assert context_after_rerun == latest_context

        library.perform_queued_task(
            process_id=process.id,
            step_name=StepOne.get_name(),
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
        )

        context_after_step_1 = AggregateProcess.get_context(mock_db.contexts_in_db[-1])
        assert context_after_step_1 == StubContext(
            aggregate_step_is_complete=False,
            aggregate_step_is_all_queued=True,
            step_one_is_queued=True,
            step_one_is_triggered_from_task_queue=True,
            step_one_is_complete=True,
            step_two_is_queued=True,
            step_two_is_triggered_from_task_queue=False,
            step_two_is_complete=False,
        )

        library.perform_queued_task(
            process_id=process.id,
            step_name=StepTwo.get_name(),
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
        )
        context_after_step_2 = AggregateProcess.get_context(mock_db.contexts_in_db[-1])
        assert context_after_step_2 == StubContext(
            aggregate_step_is_complete=True,
            aggregate_step_is_all_queued=True,
            step_one_is_queued=True,
            step_one_is_triggered_from_task_queue=True,
            step_one_is_complete=True,
            step_two_is_queued=True,
            step_two_is_triggered_from_task_queue=True,
            step_two_is_complete=True,
        )

        assert AggregateProcess.is_complete(context_after_step_2)

        self._clear_db()

    def _clear_db(self):
        mock_db.processes_in_db = []
        mock_db.audit_records_in_db = []
        mock_db.contexts_in_db = []
        mock_db.async_calls_in_db = []
        return


class TestCallAggregateStepWithAsyncSteps:
    def test_modifies_context_correctly(self):
        class _AsyncStepTwo(library.ScheduledAsyncStep):
            _name = "step_two"

            def _make_request(self, context: StubContext) -> str:
                return "two"

            def _handle_response(self, context: StubContext, response: Any) -> StubContext | None:
                return context

        class _AsyncStepOne(library.ScheduledAsyncStep):
            _name = "step_one"

            def _make_request(self, context: StubContext) -> str:
                return "one"

            def _handle_response(self, context: StubContext, response: Any) -> StubContext | None:
                return context

        class ScheduleAsyncStepOne(library.TaskQueueStep):
            target_step = _AsyncStepOne

            def _add_to_task_queue(self, context: StubContext) -> None:
                return None

        class ScheduleAsyncStepTwo(library.TaskQueueStep):
            target_step = _AsyncStepTwo

            def _add_to_task_queue(self, context: StubContext) -> None:
                return None

        class AggregateProcess(library.ProcessDefinition):
            name = "aggregate_process"
            context_class = StubContext
            steps = [
                library.schedule_simultaneously(
                    ScheduleAsyncStepOne,
                    ScheduleAsyncStepTwo,
                    name_of_step="aggregate_step",
                )
            ]

        library.register_process_definition(AggregateProcess)

        self._clear_db()

        library.initiate_process(
            idempotency_key="fizz",
            process_definition=AggregateProcess,
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
            starting_context_values={},
        )
        latest_context = AggregateProcess.get_context(mock_db.contexts_in_db[-1])
        assert latest_context == StubContext(
            aggregate_step_is_all_queued=True,
            step_one_is_queued=True,
            step_one_is_triggered_from_task_queue=False,
            step_one_correlation_id="",
            step_one_has_response_received=False,
            step_one_is_complete=False,
            step_two_is_queued=True,
            step_two_is_triggered_from_task_queue=False,
            step_two_correlation_id="",
            step_two_has_response_received=False,
            step_two_is_complete=False,
            aggregate_step_is_complete=False,
        )

        # Test idempotency
        process = mock_db.processes_in_db[0]
        library.run_process(
            process=process,
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
        )

        process_context_after_rerun = mock_db.contexts_in_db[-1]
        context_after_rerun = AggregateProcess.get_context(process_context_after_rerun)
        assert context_after_rerun == latest_context

        library.perform_queued_task(
            process_id=process.id,
            step_name=StepOne.get_name(),
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
        )

        context_after_step_1 = AggregateProcess.get_context(mock_db.contexts_in_db[-1])
        assert context_after_step_1 == StubContext(
            aggregate_step_is_all_queued=True,
            step_one_is_queued=True,
            step_one_is_triggered_from_task_queue=True,
            step_one_correlation_id="one",
            step_one_has_response_received=False,
            step_one_is_complete=False,
            step_two_is_queued=True,
            step_two_is_triggered_from_task_queue=False,
            step_two_correlation_id="",
            step_two_has_response_received=False,
            step_two_is_complete=False,
            aggregate_step_is_complete=False,
        )

        library.perform_queued_task(
            process_id=process.id,
            step_name=StepTwo.get_name(),
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
        )
        context_after_step_2 = AggregateProcess.get_context(mock_db.contexts_in_db[-1])
        assert context_after_step_2 == StubContext(
            aggregate_step_is_all_queued=True,
            step_one_is_queued=True,
            step_one_is_triggered_from_task_queue=True,
            step_one_correlation_id="one",
            step_one_has_response_received=False,
            step_one_is_complete=False,
            step_two_is_queued=True,
            step_two_is_triggered_from_task_queue=True,
            step_two_correlation_id="two",
            step_two_has_response_received=False,
            step_two_is_complete=False,
            aggregate_step_is_complete=False,
        )

        library.handle_response_received(
            correlation_id="one",
            idempotency_keys=[],
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
            response=None,
        )
        context_after_first_response = AggregateProcess.get_context(mock_db.contexts_in_db[-1])
        assert context_after_first_response == StubContext(
            aggregate_step_is_all_queued=True,
            step_one_is_queued=True,
            step_one_is_triggered_from_task_queue=True,
            step_one_correlation_id="one",
            step_one_has_response_received=True,
            step_one_is_complete=True,
            step_two_is_queued=True,
            step_two_is_triggered_from_task_queue=True,
            step_two_correlation_id="two",
            step_two_has_response_received=False,
            step_two_is_complete=False,
            aggregate_step_is_complete=False,
        )
        library.handle_response_received(
            correlation_id="two",
            idempotency_keys=[],
            db=mock_db.InMemoryDBHelper,
            system_clock=dt.datetime,
            response=None,
        )
        context_after_second_response = AggregateProcess.get_context(mock_db.contexts_in_db[-1])
        assert context_after_second_response == StubContext(
            aggregate_step_is_all_queued=True,
            step_one_is_queued=True,
            step_one_is_triggered_from_task_queue=True,
            step_one_correlation_id="one",
            step_one_has_response_received=True,
            step_one_is_complete=True,
            step_two_is_queued=True,
            step_two_is_triggered_from_task_queue=True,
            step_two_correlation_id="two",
            step_two_has_response_received=True,
            step_two_is_complete=True,
            aggregate_step_is_complete=True,
        )

        self._clear_db()

    def _clear_db(self):
        mock_db.processes_in_db = []
        mock_db.audit_records_in_db = []
        mock_db.contexts_in_db = []
        mock_db.async_calls_in_db = []
        return
