"""
Set up an entirely fictional process for testing purposes only.

This represents all the things that client code has to implement.
"""
from __future__ import annotations

from typing import Any

import src as library

from .. import mock_db


class AsyncExampleContext(library.Context):
    # Starting values
    house_id: str

    # Data that comes back from the first request.
    data_point_one: str = ""
    data_point_two: str = ""

    # This one comes from the evening request.
    data_point_three: int | None = None

    alert_code: str = ""


# Actions


class SendFirstRequest(library.AsyncStep):
    _name = "send_first_request"

    def _make_request(self, context: AsyncExampleContext) -> str:
        # fire off some request here
        # save id somewhere in the context
        return "correlation_id1"

    def _handle_response(self, context: AsyncExampleContext, response: Any) -> AsyncExampleContext:
        one, two = _extract_data_from_response_to_first_request(self.correlation_id(context))
        context.data_point_one = one
        context.data_point_two = two

        return context


class SendSecondRequest(library.AsyncStep):
    _name = "send_second_request"

    def _make_request(self, context: AsyncExampleContext) -> str:
        return "correlation_id2"

    def _handle_response(self, context: AsyncExampleContext, response: Any) -> AsyncExampleContext:
        # Do something with the `response` object.
        return context


class _SendEveningRequest(library.ScheduledSynchronousStep):
    _name = "send_evening_request"

    def _call(self, context: AsyncExampleContext) -> AsyncExampleContext | None:
        return context


class ScheduleEveningRequest(library.TaskQueueStep):
    target_step = _SendEveningRequest

    def _add_to_task_queue(self, context: AsyncExampleContext) -> None:
        # Pretend that this exists and is always successful.
        return None


class WaitForAlert(library.UnsolicitedStep):
    _name = "wait_for_alert"

    def _handle_event(
        self, *, context: AsyncExampleContext, event: Any
    ) -> AsyncExampleContext | None:
        if context.alert_code:
            return context

        if not isinstance(event, dict):
            return None

        if "alert" in event:
            code = event["alert"].get("code")

            if code:
                context.alert_code = str(code)

            return context

        return None


# Parallel task queue steps


class _ParallelStepOne(library.ScheduledSynchronousStep):
    _name = "parallel_step_one"

    def _call(self, context: AsyncExampleContext) -> AsyncExampleContext | None:
        return context


class _ParallelStepTwo(library.ScheduledSynchronousStep):
    _name = "parallel_step_two"

    def _call(self, context: AsyncExampleContext) -> AsyncExampleContext | None:

        return context


class ScheduleParallelStepOne(library.TaskQueueStep):
    target_step = _ParallelStepOne

    def _add_to_task_queue(self, context: AsyncExampleContext) -> None:
        return None


class ScheduleParallelStepTwo(library.TaskQueueStep):
    target_step = _ParallelStepTwo

    def _add_to_task_queue(self, context: AsyncExampleContext) -> None:
        return None


# Final step


class FinalStep(library.SynchronousStep):
    """
    A step that is to be completed only after everything else has finished running.
    """

    _name = "final_step"

    def _call(self, context: AsyncExampleContext) -> AsyncExampleContext | None:
        return context


# Stubs


def _extract_data_from_response_to_first_request(correlation_id: str):
    return "foo", "bar"


# Process definition


class AsyncExampleProcess(library.ProcessDefinition):
    name = "ASYNC_EXAMPLE_PROCESS"
    context_class = AsyncExampleContext

    unsolicited_steps = [
        WaitForAlert,
    ]

    steps = [
        SendFirstRequest,  # AsyncStep
        SendSecondRequest,  # AsyncStep
        ScheduleEveningRequest,  # TaskQueueStep
        library.schedule_simultaneously(  # _AggregateStep for task queues only
            ScheduleParallelStepOne,  # ScheduledSimpleStep
            ScheduleParallelStepTwo,
            name_of_step="queue_up_parallel_tasks",
        ),
        FinalStep,  # Requires all scheduled tasks to be correct before running.
    ]


library.register_process_definition(definition=AsyncExampleProcess)


def initiate_async_process(
    account_number: str,
    house_id: str,
    system_clock: library.SystemClock,
    user_id: int | None = None,
) -> library.ProcessContext | None:
    starting_context_values = dict(house_id=house_id)
    context = library.initiate_process(
        idempotency_key=account_number,
        process_definition=AsyncExampleProcess,
        starting_context_values=starting_context_values,
        user_id=user_id,
        db=mock_db.InMemoryDBHelper,
        system_clock=system_clock,
    )
    return context
