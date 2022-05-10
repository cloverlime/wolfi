"""
An example of a process that uses only synchronous actions. It is like the minimal process but
contains user-defined context variables.
"""
from __future__ import annotations

import datetime as dt

import src as library

from .. import mock_db


class AdditionContext(library.Context):
    start_value: int
    total_value_after_first_addition: int | None = None
    total_value_after_second_addition: int | None = None
    total_value_after_third_addition: int | None = None


# Actions


class AddFirstValue(library.SynchronousStep):
    _name = "add_first_value"

    def _call(self, context: AdditionContext) -> AdditionContext | None:
        start_value = context.start_value

        first_added_value = 1
        added_together = start_value + first_added_value

        context.total_value_after_first_addition = added_together

        return context


class AddSecondValue(library.SynchronousStep):
    _name = "add_second_value"

    def _call(self, context: AdditionContext) -> AdditionContext | None:
        start_value = context.total_value_after_first_addition
        assert start_value is not None

        second_added_value = 2
        added_together = start_value + second_added_value

        context.total_value_after_second_addition = added_together
        return context


class AddThirdValue(library.SynchronousStep):
    _name = "add_third_value"

    def _call(self, context: AdditionContext) -> AdditionContext | None:
        start_value = context.total_value_after_second_addition
        assert start_value is not None

        third_added_value = 3
        added_together = start_value + third_added_value

        context.total_value_after_third_addition = added_together
        return context


def i_expect_to_go_wrong(context: AdditionContext) -> AdditionContext:
    raise TypeError("Oops, someone snuck in a TypeError!")


# Process definition


class AdditionProcess(library.ProcessDefinition):
    name = "ADDITION_PROCESS"
    context_class = AdditionContext

    steps = [AddFirstValue, AddSecondValue, AddThirdValue]


library.register_process_definition(definition=AdditionProcess)


def initiate_addition_process(
    account_number: str, start_value: int, user_id: int | None = None
) -> library.ProcessContext | None:
    starting_context_values = dict(start_value=start_value)

    return library.initiate_process(
        idempotency_key=account_number,
        process_definition=AdditionProcess,
        starting_context_values=starting_context_values,
        user_id=user_id,
        db=mock_db.InMemoryDBHelper,
        system_clock=mock_db.MockSystemClock(),
    )
