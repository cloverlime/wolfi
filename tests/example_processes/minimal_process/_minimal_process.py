"""
An example of a minimal process that has no extra context and no asynchronous steps.
"""
from __future__ import annotations

import src as library
from tests.example_processes import mock_db

# Context definition


class MinimalContext(library.Context):
    starting_number: int


# Steps


class FirstStep(library.SynchronousStep):
    _name = "perform_first_step"

    def _call(self, context: MinimalContext) -> MinimalContext | None:
        return context


class SecondStep(library.SynchronousStep):
    _name = "perform_second_step"

    def _call(self, context: MinimalContext) -> MinimalContext | None:
        return context


class ThirdStep(library.SynchronousStep):
    _name = "perform_third_step"

    def _call(self, context: MinimalContext) -> MinimalContext | None:
        return context


# Process definition


class MinimalProcess(library.ProcessDefinition):
    name = "minimal_process"
    context_class = MinimalContext

    steps = [FirstStep, SecondStep, ThirdStep]


library.register_process_definition(definition=MinimalProcess)

# Entrypoint


def initialise_minimal_process(
    idempotency_key: str, starting_number: int
) -> library.ProcessContext | None:
    return library.initiate_process(
        idempotency_key=idempotency_key,
        process_definition=MinimalProcess,
        system_clock=mock_db.MockSystemClock(),
        db=mock_db.InMemoryDBHelper,
        starting_context_values={"starting_number": starting_number},
    )
