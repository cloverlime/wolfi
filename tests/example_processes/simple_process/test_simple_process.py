import datetime as dt

import src as library

from .. import mock_db
from . import _simple_process


class TestExampleProcess:
    def test_happy_path_creates_expected_context(self):
        """
        Test that the DB state is correct throughout a simple process where no exceptions and no async calls occur.
        """
        process_definition = _simple_process.AdditionProcess

        # Clear DB in example module.
        self._clear_db()
        start_value = 1
        final_process_context = _simple_process.initiate_addition_process(
            account_number="foo", start_value=start_value
        )
        final_context = process_definition.get_context(final_process_context)
        # Assert final context
        assert final_context == _simple_process.AdditionContext(
            start_value=start_value,
            add_first_value_is_complete=True,
            add_second_value_is_complete=True,
            add_third_value_is_complete=True,
            total_value_after_first_addition=(start_value + 1),
            total_value_after_second_addition=(start_value + 1 + 2),
            total_value_after_third_addition=(start_value + 1 + 2 + 3),
        )
        # Assert in DB

        # Process
        processes = mock_db.processes_in_db
        assert len(processes) == 1

        process = processes[0]

        assert process.name == _simple_process.AdditionProcess.name
        assert process.status == library.Process.Status.COMPLETE
        assert process.idempotency_key == "foo"

        # Audit records
        audit_records = mock_db.audit_records_in_db

        # One for initialisation, the rest for each step
        assert len(audit_records) == 4

        first_audit_record = audit_records[0]
        assert first_audit_record.process_id == process.id
        assert first_audit_record.step_name == "INITIATION"
        assert first_audit_record.error == ""
        assert first_audit_record.status == library.AuditRecord.Status.INITIATION_COMPLETE
        assert first_audit_record.user_id is None

        second_audit_record = audit_records[1]
        assert second_audit_record.process_id == process.id
        assert second_audit_record.step_name == "add_first_value"
        assert second_audit_record.error == ""
        assert second_audit_record.status == library.AuditRecord.Status.SYNC_STEP_COMPLETE
        assert second_audit_record.user_id is None

        third_audit_record = audit_records[2]
        assert third_audit_record.process_id == process.id
        assert third_audit_record.step_name == "add_second_value"
        assert third_audit_record.error == ""
        assert third_audit_record.status == library.AuditRecord.Status.SYNC_STEP_COMPLETE
        assert third_audit_record.user_id is None

        fourth_audit_record = audit_records[3]
        assert fourth_audit_record.process_id == process.id
        assert fourth_audit_record.step_name == "add_third_value"
        assert fourth_audit_record.error == ""
        assert fourth_audit_record.status == library.AuditRecord.Status.SYNC_STEP_COMPLETE
        assert fourth_audit_record.user_id is None

        # Contexts
        contexts = mock_db.contexts_in_db

        assert len(contexts) == 4
        initial_context = process_definition.get_context(contexts[0])
        assert initial_context == _simple_process.AdditionContext(
            start_value=start_value,
            add_first_value_is_complete=False,
            add_second_value_is_complete=False,
            add_third_value_is_complete=False,
            total_value_after_first_addition=None,
            total_value_after_second_addition=None,
            total_value_after_third_addition=None,
        )
        assert not process_definition.is_complete(initial_context)

        first_context = process_definition.get_context(contexts[1])
        assert first_context == _simple_process.AdditionContext(
            start_value=start_value,
            add_first_value_is_complete=True,
            add_second_value_is_complete=False,
            add_third_value_is_complete=False,
            total_value_after_first_addition=start_value + 1,
            total_value_after_second_addition=None,
            total_value_after_third_addition=None,
        )

        assert not process_definition.is_complete(first_context)

        second_deserialized_context = process_definition.get_context(contexts[2])
        assert second_deserialized_context == _simple_process.AdditionContext(
            start_value=start_value,
            add_first_value_is_complete=True,
            add_second_value_is_complete=True,
            add_third_value_is_complete=False,
            total_value_after_first_addition=start_value + 1,
            total_value_after_second_addition=start_value + 1 + 2,
            total_value_after_third_addition=None,
        )
        assert not process_definition.is_complete(second_deserialized_context)

        third_deserialized_context = process_definition.get_context(contexts[3])
        assert third_deserialized_context == _simple_process.AdditionContext(
            start_value=start_value,
            add_first_value_is_complete=True,
            add_second_value_is_complete=True,
            add_third_value_is_complete=True,
            total_value_after_first_addition=start_value + 1,
            total_value_after_second_addition=start_value + 1 + 2,
            total_value_after_third_addition=start_value + 1 + 2 + 3,
        )
        assert process_definition.is_complete(third_deserialized_context)

        # Async calls
        # There are no async calls in this simple process.
        assert mock_db.async_calls_in_db == []

        library.run_process(process=process, db=mock_db.InMemoryDBHelper, system_clock=dt.datetime)

        assert len(mock_db.processes_in_db) == 1
        assert len(mock_db.audit_records_in_db) == 4
        assert len(mock_db.contexts_in_db) == 4
        assert len(mock_db.async_calls_in_db) == 0

        self._clear_db()

    def _clear_db(self):
        mock_db.processes_in_db = []
        mock_db.audit_records_in_db = []
        mock_db.contexts_in_db = []
        mock_db.async_calls_in_db = []
        return
