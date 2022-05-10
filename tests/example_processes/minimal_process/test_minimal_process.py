from __future__ import annotations

import datetime as dt

import src as library

from .. import mock_db
from . import _minimal_process


class TestMinimalProcess:
    def test_happy_path_creates_expected_context(self):
        """
        Test that the DB state is correct throughout a simple process where no exceptions and no async calls occur.
        """
        # Arrange
        self._clear_db()

        # Act
        final_process_context = _minimal_process.initialise_minimal_process(
            idempotency_key="foo", starting_number=1
        )
        # Assert
        process_definition = _minimal_process.MinimalProcess
        final_context = process_definition.get_context(final_process_context)

        assert final_context == _minimal_process.MinimalContext(
            starting_number=1,
            perform_first_step_is_complete=True,
            perform_second_step_is_complete=True,
            perform_third_step_is_complete=True,
        )
        # Processes
        processes = mock_db.processes_in_db
        assert len(processes) == 1

        process = processes[0]
        assert process.idempotency_key == "foo"

        # Contexts in DB

        process_contexts = mock_db.contexts_in_db
        assert len(process_contexts) == 1 + 3

        starting_context = process_definition.get_context(process_contexts[0])

        assert starting_context["starting_number"] == 1
        assert starting_context == _minimal_process.MinimalContext(
            starting_number=1,
            perform_first_step_is_complete=False,
            perform_second_step_is_complete=False,
            perform_third_step_is_complete=False,
        )
        assert process_definition.is_complete(starting_context) is False

        first_context = process_definition.get_context(process_contexts[1])
        assert first_context == _minimal_process.MinimalContext(
            starting_number=1,
            perform_first_step_is_complete=True,
            perform_second_step_is_complete=False,
            perform_third_step_is_complete=False,
        )
        assert process_definition.is_complete(first_context) is False

        second_context = process_definition.get_context(process_contexts[2])

        assert second_context == _minimal_process.MinimalContext(
            starting_number=1,
            perform_first_step_is_complete=True,
            perform_second_step_is_complete=True,
            perform_third_step_is_complete=False,
        )
        assert process_definition.is_complete(second_context) is False

        third_context = process_definition.get_context(process_contexts[3])
        assert third_context == final_context
        assert process_definition.is_complete(third_context) is True

        # Audit records
        audit_records = mock_db.audit_records_in_db

        assert len(audit_records) == 4

        first_record = audit_records[0]
        first_record.process_id = process.id
        first_record.step_name = "initialisation"
        first_record.status = "COMPLETE"

        second_record = audit_records[1]
        second_record.process_id = process.id
        second_record.step_name = "perform_first_step"
        second_record.status = "COMPLETE"

        third_record = audit_records[2]
        third_record.process_id = process.id
        third_record.step_name = "perform_second_step"
        third_record.status = "COMPLETE"

        fourth_record = audit_records[3]
        fourth_record.process_id = process.id
        fourth_record.step_name = "perform_third_step"
        fourth_record.status = "COMPLETE"

        # Run again to check idempotency
        library.run_process(process=process, db=mock_db.InMemoryDBHelper, system_clock=dt.datetime)

        assert len(mock_db.processes_in_db) == 1
        assert len(mock_db.contexts_in_db) == 4
        assert len(mock_db.audit_records_in_db) == 4
        assert len(mock_db.async_calls_in_db) == 0

        self._clear_db()

    def _clear_db(self):
        mock_db.processes_in_db = []
        mock_db.audit_records_in_db = []
        mock_db.contexts_in_db = []
        mock_db.async_calls_in_db = []
        return
