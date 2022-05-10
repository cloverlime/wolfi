from __future__ import annotations

import abc
import copy
import datetime as dt
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Protocol,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    final,
)

from . import _serializers as serializers
from . import inspect


class ProcessError(Exception):
    pass


class ValidationError(Exception):
    pass


# --------------------
# DB Schema / Objects
# --------------------

"""
The following classes depict the objects that need to be instantiated when loaded from the DB. The actual DB schema
needs to be implemented by the user in whatever system they like.

"""


@dataclass
class Process:
    """
    An instance of this class depicts a single process that is associated with a particular `idempotency_key`.

    The `idempotency_key` is an identifier that the process is linked with. There can be no more than one active
    process of a certain type per `idempotency_key`.

    """

    class Status(Enum):
        INITIATED = "INITIATED"
        IN_PROGRESS = "IN_PROGRESS"
        COMPLETE = "COMPLETE"
        FAILED = "FAILED"  # Process has reached a failure state and has terminated itself
        ERRORED = "ERRORED"  # An unexpected error has occurred.
        CANCELLED = "CANCELLED"  # Process has been terminated by a user before it was finished.

    id: int | str

    # The name of the process e.g. "ONBOARD_NEW_USER". This field should be case-insensitive if possible.
    name: str

    # There may not be more than one active process (per process type) at a time per idempotency key.
    # This could, for example, be an identifier for an account, a user, a property etc.
    idempotency_key: str

    status: Status

    created_at: dt.datetime
    updated_at: dt.datetime

    # Queries

    @property
    def is_terminated(self) -> bool:
        return self.status in (self.Status.COMPLETE, self.Status.CANCELLED)

    @property
    def is_in_progress(self) -> bool:
        return self.status == self.Status.IN_PROGRESS

    @property
    def is_complete(self) -> bool:
        return self.status == self.Status.COMPLETE

    # Setters

    def mark_as_complete(self, as_at: dt.datetime) -> None:
        self.status = self.Status.COMPLETE
        self.updated_at = as_at

    def mark_as_failed(self, as_at: dt.datetime) -> None:
        self.status = self.Status.FAILED
        self.updated_at = as_at

    def mark_as_errored(self, as_at: dt.datetime) -> None:
        self.status = self.Status.ERRORED
        self.updated_at = as_at

    def mark_as_in_progress(self, as_at: dt.datetime) -> None:
        self.status = self.Status.IN_PROGRESS
        self.updated_at = as_at


@dataclass
class ProcessContext:
    """
    A store of all the data associated with processes. This includes (automatic) tracking of process steps as well
    as user-defined data that is filled in as the process progresses.

    One context is stored every time the process moves forward a step so you will always get a timeline of how the
    context has progressed.

    Each entry should be immutable.

    Notes
    -----

    The Context is a rich object that is passed from step to step in a process. It must be serialised before
    being saved to the DB and deserialised for use in the process. This is taken care of automatically by the library.

    # TODO automatic versioning
    """

    id: int | str

    process_id: int | str
    version: int | None
    context: Dict
    created_at: dt.datetime


@dataclass
class AuditRecord:
    """
    Records some metadata and statuses of things that have happened in the process.

    This doesn't have to be saved in the DB; you can use your favourite logger instead.

    Each record is immutable.

    """

    class Status(Enum):
        INITIATION_COMPLETE = "INITIATION_COMPLETE"
        SYNC_STEP_COMPLETE = "SYNC_STEP_COMPLETE"
        ASYNC_REQUEST_SENT = "ASYNC_REQUEST_SENT"
        ASYNC_RESPONSE_RECEIVED = "ASYNC_RESPONSE_RECEIVED"
        TASK_IS_QUEUED = "TASK_IS_QUEUED"
        TARGET_STEP_TRIGGERED = "TARGET_STEP_TRIGGERED"
        UNSOLICITED_STEP_PROCESSED = "UNSOLICITED_STEP_PROCESSED"

        ERROR = "ERROR"

    id: int | str

    process_id: int | str
    context_id: int | str
    step_name: str
    error: str
    user_id: int | str | None
    status: AuditRecord.Status

    created_at: dt.datetime


@dataclass
class ProcessAsyncRequest:
    """
    Stores the correlation ID of an asynchronous request, and links it back to the process that requested it.
    """

    class Status(Enum):
        AWAITING_RESPONSE = "AWAITING_RESPONSE"
        RESPONSE_RECEIVED = "RESPONSE_RECEIVED"

    id: int | str | None

    process_id: int | str
    correlation_id: str
    status: ProcessAsyncRequest.Status
    step_name: str

    created_at: dt.datetime
    updated_at: dt.datetime

    @property
    def is_response_received(self) -> bool:
        return self.status == self.Status.RESPONSE_RECEIVED

    def set_as_response_received(self, as_at: dt.datetime) -> None:
        self.status = self.Status.RESPONSE_RECEIVED
        self.updated_at = as_at


# --------------------
# USER-DEFINED-CLASSES
# --------------------


class Context(abc.ABC):
    """
    An abstract base class which every process-specific context must inherit.

    Context instances to be accessed like dictionaries i.e. with square brackets.

    The serialize and deserialize methods work for a limited set of rich values. If your context contains more complex
    values, you must provide custom serialize and deserialize methods.

    Private keys exist (expand!) TODO
    """

    def __init__(self, **kwargs) -> None:
        # This is for setting the starting kwargs.
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __getitem__(self, item: str) -> Any:
        return self.__dict__[item]

    def __setitem__(self, key: str, value: Any) -> None:
        self.__dict__[key] = value

    def __delitem__(self, item: str) -> None:
        del self.__dict__[item]

    def __eq__(self, other: Any) -> bool:
        if not type(other) == type(self):
            raise TypeError(
                f"Cannot compare type {other.__class__.__name__} with {self.__class__.__name__}"
            )

        # Ignore private context attributes.
        this_ = {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        other_ = {k: v for k, v in other.__dict__.items() if not k.startswith("_")}
        return this_ == other_

    def __str__(self) -> str:
        return str(self.__dict__)

    def __repr__(self) -> str:
        return str(self.__dict__)

    # The following methods work for a limited set of rich values. If your context contains more complex
    # values, you must provide custom serialize and deserialize methods.

    def serialize(self) -> dict:
        # The developer may choose to use this
        return serializers.serialize(payload=self.__dict__)

    @classmethod
    def deserialize(cls, serialized_dict: dict) -> Context:
        deserialized_dict = cls._deserialize(serialized_dict)
        return cls(**deserialized_dict)

    @classmethod
    def _deserialize(cls, serialized_dict: dict) -> Dict:
        """
        Override or extend this method to perform customise deserialization.
        """
        # When we have Python 3.10+
        annotations = inspect.get_annotations(cls, eval_str=True)
        return serializers.deserialize(serialized_dict, annotations)


C = TypeVar("C", bound=Context)

# ------------------
# PROCESS DEFINITION
# ------------------


class ProcessDefinition(Generic[C]):
    """
    This class defines the sequence of actions required for a process.
    """

    name: str
    context_class: Type[C]

    # This is the sequence that the process runner steps through with the (latest) context through every time the
    # process is run.
    steps: List[Type[Step]]

    # These are listeners for messages or events that happen without the process triggering it explicitly.
    # Unsolicited messages must be traceable back to a process's `idempotency_key`.
    unsolicited_steps: List[Type[UnsolicitedStep]] = []

    @final
    @classmethod
    def get_context(cls, process_context: ProcessContext) -> C:
        """
        Return the deserialized Context object.
        """
        return cls.context_class.deserialize(process_context.context)

    @final
    @classmethod
    def initialise_context(cls, **starting_context_values) -> C:
        """
        Return the initial context that every process should start off with.

        :param starting_context_values: All the initial values that the context should begin with.
        :return: A Context object.

        :raises ProcessError: if `starting_context_values` contains keys that are not predefined in the user-defined
        Context class.
        """
        starting_context_keys = list(inspect.get_annotations(cls.context_class).keys())

        if not all([k in starting_context_keys for k in starting_context_values]):
            raise ProcessError("Context initialised with unexpected keys.")

        context = cls.context_class(**starting_context_values)

        for step in cls.steps:
            context = step.initialise(context)

        for unsolicited_step in cls.unsolicited_steps:
            context = unsolicited_step.initialise(context)

        # Initialise user-specified values as their defaults.
        class_attribute_defaults = {
            k: v
            for k, v in cls.context_class.__dict__.items()
            if not k.startswith("_") and not isinstance(v, Callable)  # type: ignore
        }
        context.__dict__.update(class_attribute_defaults)
        return context

    @classmethod
    def is_complete(cls, context: C) -> bool:
        """
        A process is complete if all the registered steps are complete.

        Overwrite this method if there are more complex criteria for a process to count as complete.
        """
        return all(step.is_complete(context) for step in cls.steps)


# ------------------
# USER-DEFINED STEPS
# ------------------


class Step(Generic[C]):
    _name: str

    # You can use this to declare any steps that *must* be complete before this step runs. You can also
    # use the context.
    prerequisite_steps: List[Type[Step]] = []

    # This prevents the step from being directly registered as part of a process's steps
    _is_hidden: bool = False

    def __init__(
        self, *, process_id: str | int, db: Type[DBHelper], system_clock: SystemClock
    ) -> None:
        self.db = db
        self.process_id = process_id
        self.system_clock = system_clock

    # Private keys
    @classmethod
    def _is_complete_key(cls) -> str:
        return f"{cls.get_name()}_is_complete"

    @classmethod
    def _has_errored_key(cls) -> str:
        return f"{cls.get_name()}_has_errored"

    @classmethod
    def _error_info_key(cls) -> str:
        """
        Some processes may wish to store additional data about failure states.
        """
        return f"{cls.get_name()}_error_info"

    # Queries
    @classmethod
    def initialise(cls, context: C) -> C:
        """
        Populates the context with the fact that this particular step has not yet been completed.
        This is called when initialising the starting context of a process.
        """
        context[cls._is_complete_key()] = False
        return context

    @classmethod
    def is_complete(cls, context: C) -> bool:
        return context[cls._is_complete_key()]

    @classmethod
    def has_errored(cls, context: C) -> bool:
        return context[cls._has_errored_key()] is True

    @classmethod
    def fulfils_prerequisites(cls, context: C) -> bool:
        """
        Returns whether the step should run or not.

        This allows the developer to define any special requirements for a step to be run.

        If a step does not fulfil all stated requirements, it will not be attempted.
        """
        if cls.prerequisite_steps and not all(
            [step.is_complete(context) for step in cls.prerequisite_steps]
        ):
            return False

        return cls._fulfils_prerequisites(context)

    # Getters

    @classmethod
    def get_name(cls) -> str:
        return cls._name

    # Setters

    @classmethod
    def set_as_complete(cls, context: C) -> C:
        context[cls._is_complete_key()] = True
        return context

    @classmethod
    def set_error_info(cls, context: C, errors: str | dict) -> C:
        context[cls._error_info_key()] = errors
        return context

    # --------------------------------------------------------------------------------------------

    @abc.abstractmethod
    def call(self, context: C) -> C | None:
        raise NotImplementedError

    @classmethod
    def _fulfils_prerequisites(cls, context: C) -> bool:
        """
        To be implemented optionally by the user if there are any special data requirements.

        Required steps may be stated in the class attribute `prerequisite_steps`.
        """

        return True


class SynchronousStep(Step[C]):
    """
    All subclasses must implement `_call`.
    """

    @final
    def call(self, context: C) -> C | None:
        """
        Call the user-defined function and automatically keep track of the step's completion status.
        """
        if self.is_complete(context):
            return context

        if not self.fulfils_prerequisites(context):
            return None

        new_context = self._call(context)

        if not new_context:
            return None

        new_context = self.set_as_complete(context)

        return new_context

    # ------------------------------------------------------------

    @abc.abstractmethod
    def _call(self, context: C) -> C | None:
        """
        The user implements this.

        Return None to stop the process.
        """
        raise NotImplementedError


class AsyncStep(Step[C]):
    """
     An extension of a simple step. Use for asynchronous responses that you eventually will correlate a response back to
     at some unknown future time.

     * ~ * ~ * ~ * ~ * ~ * ~

     All steps must extend:

         _make_request
         _handle_response

    * ~ * ~ * ~ * ~ * ~ * ~

     All Async Steps will add the following keys to the context:

         {self.name}_correlation_id

     The presence of the above implies that the request / attempt has been made.

         {self.name}_has_response_received

     The above updates to True when the response with the correct correlation ID is processed.

         {self.name}_is_complete

     The same as for all steps, this is marked as True when everything to do with the step is complete.

    """

    # Properties
    @final
    @classmethod
    def _correlation_id_key(cls) -> str:
        return f"{cls.get_name()}_correlation_id"

    @final
    @classmethod
    def _has_response_received_key(cls) -> str:
        return f"{cls.get_name()}_has_response_received"

    # Setters
    @final
    @classmethod
    def initialise(cls, context: C) -> C:
        """
        This is called when initialising the starting context of a process.
        """
        context[cls._correlation_id_key()] = ""
        context[cls._has_response_received_key()] = False
        context[cls._is_complete_key()] = False
        return context

    @final
    @classmethod
    def set_correlation_id(cls, context: C, correlation_id: str) -> C:
        context[cls._correlation_id_key()] = str(correlation_id)
        return context

    @final
    @classmethod
    def set_as_response_received(cls, context: C) -> C:
        context[cls._has_response_received_key()] = True
        return context

    # Getters

    @final
    @classmethod
    def correlation_id(cls, context: C) -> str:
        return context[cls._correlation_id_key()]

    # Core

    @final
    def call(self, context: C) -> C | None:
        """
        Call the user-defined function and automatically keep track of the step's completion status.

        Expects a correlation ID to be returned by the user-defined function.

        Expects a message handler to update the _response_received attribute.
        """
        if self.is_complete(context):
            return context

        if not self.fulfils_prerequisites(context):
            return None

        if self.correlation_id(context):
            # The process is not complete, but there is already a correlation ID. This means we haven't received
            # a response yet. Return None to signify that we're still waiting.
            return None

        # If it's not complete *and* there is no correlation ID, it's time to send the request.
        correlation_id = self._make_request(context)

        if not correlation_id:
            # We've decided not to send the request after all. Continue?
            return context

        context = self.set_correlation_id(context=context, correlation_id=correlation_id)

        now = self.system_clock.now()

        # We are going to return None to pause the process. This code assumes that the response can't come back
        # before these records are created.
        self.db.create_async_record(
            process_id=self.process_id,
            correlation_id=correlation_id,
            step_name=self.get_name(),
            as_at=now,
        )
        process_context = self.db.create_context(
            process_id=self.process_id,
            context=context,
            as_at=now,
        )

        self.db.create_audit_record(
            process_id=self.process_id,
            context_id=process_context.id,
            step_name=self.get_name(),
            status=AuditRecord.Status.ASYNC_REQUEST_SENT,
            created_at=now,
        )

        return None

    @final
    def handle_response(self, *, context: C, correlation_id: str, response: Any) -> C | None:
        if not correlation_id == self.correlation_id(context):
            return None

        context = self.set_as_response_received(context)

        new_context: C | None
        new_context = self._handle_response(context, response)

        if new_context is not None:
            new_context = self.set_as_complete(new_context)

        return new_context

    # --------------------
    # User implementations
    # --------------------

    @abc.abstractmethod
    def _make_request(self, context: C) -> str:
        """
        The user implements this.

        Must return the correlation ID of the request or action. If it does not naturally come with one, you can
        generate your own unique string key.

        # TODO allow this to return the context as well.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _handle_response(self, context: C, response: Any) -> C | None:
        """
        The user implements this.
        """
        raise NotImplementedError


class TaskQueueStep(Step[C]):
    """
    A step that queues a step onto an asynchronous task queue e.g. if you use need to use Celery to delay or
    rate-limit requests

    """

    # The name of the TaskQueueStep and its target step need to be identical, so we just use the name of
    # its target step.

    _name = "_"
    # ATM we don't support triggering another TaskQueueStep, though we could consider relaxing it.
    target_step: Type[Union[ScheduledSynchronousStep[C], ScheduledAsyncStep[C]]]

    @final
    @classmethod
    def get_name(cls) -> str:
        return cls.target_step.get_name()

    @final
    @classmethod
    def _is_queued_key(cls) -> str:
        return f"{cls.get_name()}_is_queued"

    @final
    @classmethod
    def _is_triggered_key_from_task_queue(cls) -> str:
        return f"{cls.get_name()}_is_triggered_from_task_queue"

    # Queries
    @final
    @classmethod
    def is_queued(cls, context: C) -> bool:
        return context[cls._is_queued_key()] is True

    @final
    @classmethod
    def is_triggered(cls, context: C) -> bool:
        return context[cls._is_triggered_key_from_task_queue()] is True

    # Setters
    @final
    @classmethod
    def initialise(cls, context: C) -> C:
        context[cls._is_queued_key()] = False
        context[cls._is_triggered_key_from_task_queue()] = False
        context = cls.target_step.initialise(context=context)
        return context

    @final
    @classmethod
    def set_as_queued(cls, context: C) -> C:
        """
        This may only be called when the task in the queue is run.
        """
        context[cls._is_queued_key()] = True
        return context

    @final
    @classmethod
    def set_as_triggered(cls, context: C) -> C:
        """
        This may only be called when the task in the queue is run.
        """
        context[cls._is_triggered_key_from_task_queue()] = True
        return context

    @final
    def call(self, context: C) -> C | None:
        """
        Queue the task, if not queued.

        Call the task if triggered from task queue.

        """
        if self.is_complete(context):
            return context

        if not self.fulfils_prerequisites(context):
            return None

        if self.is_triggered(context):
            context_ = self._call_target_step(context)
            if context_ is None:
                return None

            context = context_

            if self.target_step.is_complete(context):
                self.set_as_complete(context)
                return context

            return None

        if self.is_queued(context):
            return None

        # Assume that the context does not need to be mutated by `add_to_task_queue`.
        self._add_to_task_queue(context)
        self.set_as_queued(context)

        now = self.system_clock.now()
        process_context = self.db.create_context(
            process_id=self.process_id,
            context=context,
            as_at=now,
        )
        self.db.create_audit_record(
            process_id=self.process_id,
            context_id=process_context.id,
            step_name=self.get_name(),
            status=AuditRecord.Status.TASK_IS_QUEUED,
            created_at=now,
        )
        return None

    @final
    def _call_target_step(self, context: C) -> C | None:
        if not self.is_triggered(context):
            return None

        step = self.target_step(
            process_id=self.process_id, db=self.db, system_clock=self.system_clock
        )
        return step.call(copy.deepcopy(context))

    # ------------------------------------------------------------------------------------------

    @abc.abstractmethod
    def _add_to_task_queue(self, context: C) -> None:
        raise NotImplementedError


class _AggregateStep(Step[C]):
    """
    An aggregation of task queue steps that are meant to be scheduled immediately, with no need to wait for one
    to finish before another starts.

    Use `schedule_simultaneously` to generate them on the fly.

    NB the number 1 assumption here is that all scheduled steps need to complete (and not just trigger) for the whole
    aggregate step to be considered complete. If you have a step where it doesn't matter if it fails, that must be
    taken care of in the ScheduledStep itself.
    """

    steps: List[Type[TaskQueueStep[C]]]

    @final
    @classmethod
    def _is_all_queued_key(cls) -> str:
        return f"{cls.get_name()}_is_all_queued"

    @final
    @classmethod
    def initialise(cls, context: C) -> C:
        context[cls._is_all_queued_key()] = False

        for step in cls.steps:
            step.initialise(context)

        super().initialise(context)

        return context

    @final
    @classmethod
    def is_complete(cls, context: C) -> bool:
        """
        An aggregate step is considered complete if all of its scheduled steps have been completed.
        """
        return all([step.is_complete(context) for step in cls.steps])

    @final
    @classmethod
    def is_all_queued(cls, context: C) -> bool:
        """
        An aggregate step is considered complete if all of its scheduled steps have been completed.
        """
        return all([step.is_queued(context) for step in cls.steps])

    @final
    @classmethod
    def set_as_queued(cls, context: C) -> C:
        context[cls._is_all_queued_key()] = True
        return context

    @final
    def call(self, context: C) -> C | None:
        if not self.fulfils_prerequisites(context):
            return None

        if self.is_complete(context):
            context = self.set_as_complete(context)
            return context

        for step_class in self.steps:
            _, _, latest_context = _get_process_and_latest_context(  # type: ignore
                process_id=self.process_id, db=self.db
            )
            step = step_class(
                process_id=self.process_id, db=self.db, system_clock=self.system_clock
            )

            if step.is_complete(latest_context):
                continue

            # The calls from TaskQueueSteps always return None but in this specific case, we do want to continue
            # to queue despite returning essentially a pause.
            # If the step has not been queued, it well be queued. If it has been triggered from a task queue, it will
            # perform its action.
            next_context = step.call(copy.deepcopy(latest_context))
            if next_context:
                self.db.create_context(
                    process_id=self.process_id,
                    context=next_context,
                    as_at=self.system_clock.now(),
                )
                if self.is_complete(next_context):
                    next_context = self.set_as_complete(next_context)
                    return next_context

        _, _, latest_context = _get_process_and_latest_context(
            process_id=self.process_id, db=self.db
        )
        if self.is_all_queued(latest_context):
            latest_context = self.set_as_queued(latest_context)
            self.db.create_context(
                process_id=self.process_id,
                context=latest_context,
                as_at=self.system_clock.now(),
            )
        # This step is expected ONLY to queue tasks, so we don't want the run to continue.
        return None


def schedule_simultaneously(*args, name_of_step: str) -> Type[_AggregateStep[C]]:
    """
    Schedule two or more TaskQueueSteps to be queued one after another before waiting for the results of any
    of them to be returned first.

    All args must be subclasses of TaskQueueStep.

    :raises ValidationError: if no steps are passed in.
    :raises AssertionError: if the wrong type of step is passed in. Using type checking should prevent this.

    Usage
    -----

    In your ProcessDefinition:

    steps = [
        schedule_simultaneously(
            StepOne,
            StepTwo
        ),
        StepThree
    ]

    The step is only complete when everything has been scheduled.

    """
    step_classes = []

    for arg in args:
        try:
            assert issubclass(arg, TaskQueueStep)
        except TypeError:
            raise AssertionError("Arguments are not TaskQueueStep classes.")

        step_classes.append(arg)

    if not step_classes:
        raise ValidationError("No steps were provided for aggregating.")

    class MyAggregateStep(_AggregateStep[C]):
        _name = name_of_step
        steps = step_classes

    return MyAggregateStep


"""
Wrapper classes to ensure that these steps are not added to a process definition's main list of steps.
"""


class ScheduledSynchronousStep(SynchronousStep[C]):
    """
    A task that may only be called by a task queue step. It behaves like a Sync or async step but is not directly
    accessible from the task runner, so it can never be run without a task queue.
    """

    _is_hidden = True


class ScheduledAsyncStep(AsyncStep[C]):
    """
    A task that may only be called by a task queue step. It behaves like an async or async step but is not directly
    accessible from the task runner, so it can never be run without a task queue.
    """

    _is_hidden = True


# Handling unsolicited messages


class UnsolicitedStep(Step[C]):
    """
    # TODO more docs
    A good convention would be for the `_name` to start with "wait_for_*"
    """

    @final
    def call(self, context: C) -> C | None:
        if self.is_complete(context):
            return context
        else:
            return None

    @final
    def handle_event(self, *, context: C, event: Any) -> C | None:
        new_context = self._handle_event(context=context, event=event)
        if new_context is not None:
            context[self._is_complete_key()] = True
            return context

        return None

    # ------------------------------------------------------------------

    @abc.abstractmethod
    def _handle_event(self, *, context: C, event: Any) -> C | None:
        raise NotImplementedError


# Helper classes


class SystemClock(Protocol):
    def now(self) -> dt.datetime:
        ...


@final
class ProcessRuntime:
    """
    A helper class that is passed into each action/step of the process.
    """

    def __init__(
        self,
        *,
        process_id: int | str,
        db_helper: Type[DBHelper],
        system_clock: SystemClock,
    ) -> None:
        self.process_id = process_id
        self.db_helper = db_helper
        self.system_clock = system_clock

    def is_response_received(self, correlation_id: str) -> bool:
        record = self.db_helper.get_async_record_by_correlation_id(correlation_id)

        if not record:
            return False

        return record.is_response_received


@final
class _ProcessRunner(Generic[C]):
    def __init__(
        self, *, process: Process, db_helper: Type[DBHelper], system_clock: SystemClock
    ) -> None:
        self.process = process
        self.db_helper = db_helper
        self.process_runtime = ProcessRuntime(
            process_id=self.process.id,
            db_helper=self.db_helper,
            system_clock=system_clock,
        )
        self.system_clock = system_clock

    def run(self, latest_context_record: ProcessContext | None = None) -> ProcessContext | None:
        """
        Run the process from its given or latest state as far as possible.
        """
        process_definition = _get_process_definition_by_name(self.process.name)

        if not latest_context_record:
            latest_context_record = self.db_helper.get_latest_context(self.process.id)
            if not latest_context_record:
                raise ProcessError(
                    "No context found for process. This should never happen as there should always "
                    "be a starting context saved before a process is run."
                )

        if self.process.is_terminated:
            return latest_context_record

        context_record = self._run(
            starting_context_record=latest_context_record,
            process_definition=process_definition,
        )
        if not context_record:
            if not self.process.is_in_progress:
                now = self.system_clock.now()
                self.process.mark_as_in_progress(now)
                self.db_helper.update_process(self.process, now)
            return None

        context: C = process_definition.get_context(context_record)

        now = self.system_clock.now()
        if process_definition.is_complete(context):
            self.process.mark_as_complete(now)
            self.db_helper.update_process(self.process, now)
            return context_record
        else:
            if not self.process.is_in_progress:
                self.process.mark_as_in_progress(now)
                self.db_helper.update_process(self.process, now)

        return context_record

    def _run(
        self,
        *,
        process_definition: Type[ProcessDefinition],
        starting_context_record: ProcessContext,
    ) -> ProcessContext | None:
        context: C = process_definition.get_context(starting_context_record)
        latest_process_context = None
        for step_class in process_definition.steps:
            step = step_class(
                process_id=self.process.id,
                db=self.db_helper,
                system_clock=self.system_clock,
            )
            try:
                # New context is spat out by each fn.
                new_context = step.call(copy.deepcopy(context))
            except ProcessError as e:
                now = self.system_clock.now()
                self.db_helper.create_audit_record(
                    process_id=self.process.id,
                    context_id=starting_context_record.id,
                    step_name=step.get_name(),
                    error=_format_exception(e),
                    status=AuditRecord.Status.ERROR,
                    created_at=now,
                )
                self.process.mark_as_errored(now)
                self.db_helper.update_process(self.process, now)
                # Something unexpected happened. Don't continue running all the steps.
                return None
            except Exception as e:
                now = self.system_clock.now()
                self.db_helper.create_audit_record(
                    context_id=starting_context_record.id,
                    process_id=self.process.id,
                    step_name=step.get_name(),
                    error=_format_exception(e),
                    status=AuditRecord.Status.ERROR,
                    created_at=now,
                )
                self.process.mark_as_errored(now)
                self.db_helper.update_process(self.process, now)
                # Something unexpected happened. Don't continue running all the steps and raise again.
                raise
            else:
                if new_context is None:
                    # The action step has signalled that the process is paused, for example we know we are waiting
                    # for a response to a synchronous request.
                    return None

                if new_context == context:
                    continue

                now = self.system_clock.now()
                process_context = self.db_helper.create_context(
                    process_id=self.process.id,
                    context=new_context,
                    as_at=now,
                )
                self.db_helper.create_audit_record(
                    process_id=self.process.id,
                    context_id=process_context.id,
                    step_name=step.get_name(),
                    status=AuditRecord.Status.SYNC_STEP_COMPLETE,
                    created_at=now,
                )
                context = new_context
                latest_process_context = process_context

        return latest_process_context


# DB helper


class DBHelper(Generic[C]):
    """
    Encapsulates all DB interactions that the library requires.

    The user must implement this class in their application for their DB of choice.
    """

    # -------
    # Writes
    # -------

    @classmethod
    def update_process(cls, process: Process, as_at: dt.datetime) -> Process:
        raise NotImplementedError

    @classmethod
    def set_async_response_as_received(
        cls, async_request: ProcessAsyncRequest
    ) -> ProcessAsyncRequest:
        raise NotImplementedError

    @classmethod
    @final
    def create_audit_record(
        cls,
        *,
        process_id: int | str,
        context_id: int | str,
        step_name: str,
        status: AuditRecord.Status,
        error: str = "",
        user_id: int | str | None = None,
        created_at: dt.datetime,
    ) -> AuditRecord:
        audit_record = AuditRecord(
            id=0,
            process_id=process_id,
            context_id=context_id,
            step_name=step_name,
            error=error,
            user_id=user_id,
            created_at=created_at,
            status=status,
        )
        return cls._create_audit_record(audit_record, as_at=created_at)

    @classmethod
    @final
    def create_context(
        cls,
        *,
        process_id: int | str,
        context: C,
        version: int | None = None,
        as_at: dt.datetime,
    ) -> ProcessContext:
        # TODO version numbers.
        process_context = ProcessContext(
            id=0,  # This is filled in by the private method below.
            process_id=process_id,
            version=version,
            context=context.serialize(),
            created_at=as_at,
        )
        return cls._create_context(context=process_context, as_at=as_at, version=version)

    @classmethod
    @final
    def create_process(
        cls, *, idempotency_key: str, name: str, status: Process.Status, as_at: dt.datetime
    ) -> Process:

        process = Process(
            id=0,  # This is filled in by the private method below.
            name=name,
            idempotency_key=idempotency_key,
            status=status,
            created_at=as_at,
            updated_at=as_at,
        )
        return cls._create_process(process, as_at)

    @classmethod
    @final
    def create_async_record(
        cls,
        *,
        process_id: int | str,
        correlation_id: str,
        step_name: str,
        as_at: dt.datetime,
    ) -> ProcessAsyncRequest:
        record = ProcessAsyncRequest(
            id=0,  # This is filled in by the private method below.
            process_id=process_id,
            correlation_id=correlation_id,
            step_name=step_name,
            status=ProcessAsyncRequest.Status.AWAITING_RESPONSE,
            created_at=as_at,
            updated_at=as_at,
        )
        return cls._create_async_record(record, as_at)

    @classmethod
    @final
    def create_process_for_idempotency_key(
        cls,
        *,
        idempotency_key: str,
        starting_context: C,
        process_definition_name: str,
        as_at: dt.datetime,
        user_id: int | str | None = None,
    ) -> Process:
        process = cls.create_process(
            idempotency_key=idempotency_key,
            status=Process.Status.INITIATED,
            name=process_definition_name,
            as_at=as_at,
        )
        process_context = cls.create_context(
            process_id=process.id, context=starting_context, as_at=as_at
        )
        cls.create_audit_record(
            process_id=process.id,
            context_id=process_context.id,
            step_name="INITIATION",
            status=AuditRecord.Status.INITIATION_COMPLETE,
            user_id=user_id,
            error="",
            created_at=process.created_at,
        )
        return process

    # -------
    # Queries
    # -------

    @classmethod
    def get_process(cls, process_id: int | str) -> Process | None:
        """
        Get the process from the DB and transform it into a Process object.
        """
        raise NotImplementedError

    @classmethod
    def get_active_processes_for_idempotency_key(
        cls, *, idempotency_key: str, as_at: dt.datetime
    ) -> List[Process]:
        """
        Get the process from the DB and transform it into a Process object.
        """
        raise NotImplementedError

    @classmethod
    def get_async_record_by_correlation_id(cls, correlation_id: str) -> ProcessAsyncRequest | None:
        raise NotImplementedError

    @classmethod
    def get_latest_context(cls, process_id: int | str) -> ProcessContext | None:
        raise NotImplementedError

    # --------------------
    # User implementations
    # --------------------

    @classmethod
    def _create_audit_record(cls, audit_record: AuditRecord, as_at: dt.datetime) -> AuditRecord:
        raise NotImplementedError

    @classmethod
    def _create_context(
        cls, context: ProcessContext, as_at: dt.datetime, version: int | None = None
    ) -> ProcessContext:
        raise NotImplementedError

    @classmethod
    def _create_process(cls, process: Process, as_at: dt.datetime) -> Process:
        raise NotImplementedError

    @classmethod
    def _create_async_record(
        cls, async_record: ProcessAsyncRequest, as_at: dt.datetime
    ) -> ProcessAsyncRequest:
        raise NotImplementedError


# Registry

PROCESS_NAMES_TO_DEFINITIONS: Dict[str, Type[ProcessDefinition]] = dict()
STEPS_FOR_PROCESSES: Dict[str, Dict[str, Type[Step]]] = dict()


def register_process_definition(definition: Type[ProcessDefinition]) -> None:
    PROCESS_NAMES_TO_DEFINITIONS[definition.name] = definition
    STEPS_FOR_PROCESSES[definition.name] = dict()

    for step_class in definition.steps:
        if step_class._is_hidden:
            # A step may be classed as hidden if it must not be accessed directly by the runner.
            raise ValidationError("Cannot register hidden steps to a process definition.")

        if issubclass(step_class, _AggregateStep):
            for task_queue_step in step_class.steps:
                STEPS_FOR_PROCESSES[definition.name][task_queue_step.get_name()] = task_queue_step
        else:
            STEPS_FOR_PROCESSES[definition.name][step_class.get_name()] = step_class


# ------------
# ENTRY POINTS
# ------------


def initiate_process(
    *,
    idempotency_key: str,
    process_definition: Type[ProcessDefinition],
    db: Type[DBHelper],
    starting_context_values: dict,
    system_clock: SystemClock,
    user_id: int | str | None = None,
) -> ProcessContext | None:
    """
    Create a new process with the initial arguments given in `starting_context` dictionary.

    :param idempotency_key:
    :param process_definition:
    :param starting_context_values:
    :param system_clock: A class or module that implements a `now` method, e.g. datetime.datetime.
    :param user_id:
    :return:
    """
    starting_context = process_definition.initialise_context(**starting_context_values)

    process = db.create_process_for_idempotency_key(
        idempotency_key=idempotency_key,
        starting_context=starting_context,
        user_id=user_id,
        process_definition_name=process_definition.name,
        as_at=system_clock.now(),
    )
    return run_process(process=process, db=db, system_clock=system_clock)


def handle_response_received(
    *,
    correlation_id: str,
    idempotency_keys: Sequence[str],
    response: Any,
    db: Type[DBHelper],
    system_clock: SystemClock,
) -> ProcessContext | None:
    """
    Handle a message or event that a process may be interested in.

    These messages may be solicited, in which case there should be a correlation ID in the DB, or unsolicited,
    in which case there isn't.
    """
    async_record = db.get_async_record_by_correlation_id(correlation_id)

    if not async_record and idempotency_keys:
        _maybe_handle_unsolicited_message(
            idempotency_keys=idempotency_keys,
            response=response,
            db=db,
            system_clock=system_clock,
        )
        return None

    if not async_record and not idempotency_keys:
        return None

    assert async_record is not None

    if async_record.is_response_received is True:
        return None

    async_record.set_as_response_received(system_clock.now())
    async_record = db.set_async_response_as_received(async_record)
    process_id = async_record.process_id

    process, latest_process_context, latest_context = _get_process_and_latest_context(  # type: ignore
        process_id=process_id, db=db
    )
    step_class = _get_step_class(process_name=process.name, step_name=async_record.step_name)

    if issubclass(step_class, TaskQueueStep):
        step_class = step_class.target_step

    assert issubclass(step_class, AsyncStep)

    try:
        # The handler specifies a context change.
        step: AsyncStep = step_class(
            process_id=process.id,
            db=db,
            system_clock=system_clock,
        )
        new_context = step.handle_response(
            context=copy.deepcopy(latest_context),
            correlation_id=async_record.correlation_id,
            response=response,
        )
    except ProcessError as e:
        # For known errors. Using exception types to drive error handling behaviour is probably not a good
        # pattern to use, but this bit stops us from throwing an exception and that might be useful for your usecase.
        now = system_clock.now()
        db.create_audit_record(
            process_id=process.id,
            context_id=latest_process_context.id,
            error=f"Response handling failed: {_format_exception(e)}",
            status=AuditRecord.Status.ERROR,
            step_name=async_record.step_name,
            created_at=now,
        )
        process.mark_as_errored(now)
        db.update_process(process, now)
        return None
    except Exception as e:
        # For unexpected errors.
        now = system_clock.now()
        db.create_audit_record(
            process_id=process.id,
            context_id=latest_process_context.id,
            error=f"Response handling failed: {_format_exception(e)}",
            status=AuditRecord.Status.ERROR,
            step_name=async_record.step_name,
            created_at=system_clock.now(),
        )
        process.mark_as_errored(now)
        db.update_process(process, now)
        raise

    if not new_context:
        return None

    now = system_clock.now()

    process_context = db.create_context(
        process_id=process_id,
        context=new_context,
        as_at=now,
    )
    db.create_audit_record(
        process_id=process_id,
        context_id=process_context.id,
        step_name=step.get_name(),
        status=AuditRecord.Status.ASYNC_RESPONSE_RECEIVED,
        created_at=now,
    )

    return run_process(process=process, db=db, system_clock=system_clock)


def _maybe_handle_unsolicited_message(
    idempotency_keys: Sequence[str],
    response: Any,
    db: Type[DBHelper],
    system_clock: SystemClock,
) -> None:
    for idempotency_key in idempotency_keys:
        active_processes = db.get_active_processes_for_idempotency_key(
            idempotency_key=idempotency_key, as_at=system_clock.now()
        )
        if not active_processes:
            return None

        for process in active_processes:
            try:
                _handle_unsolicited_message_for_process(
                    process=process, response=response, db=db, system_clock=system_clock
                )
            except ProcessError:
                # Do not skip processes if one process fails. # TODO: audit trail?
                continue

    return None


def _handle_unsolicited_message_for_process(
    *, process: Process, response: Any, db: Type[DBHelper], system_clock: SystemClock
) -> ProcessContext | None:
    definition = _get_process_definition_by_name(process_name=process.name)
    unsolicited_steps = definition.unsolicited_steps

    if not unsolicited_steps:
        return None

    latest_process_context = db.get_latest_context(process.id)
    if not latest_process_context:
        raise ProcessError("Latest context could not be found.")

    context = definition.get_context(latest_process_context)

    should_run_process: bool = False

    for step_class in unsolicited_steps:
        step = step_class(
            process_id=process.id,
            db=db,
            system_clock=system_clock,
        )
        new_context = step.handle_event(context=copy.deepcopy(context), event=response)
        if new_context is not None:
            should_run_process = True

            now = system_clock.now()
            process_context = db.create_context(
                process_id=process.id, context=new_context, as_at=now
            )
            db.create_audit_record(
                process_id=process.id,
                context_id=process_context.id,
                status=AuditRecord.Status.UNSOLICITED_STEP_PROCESSED,
                step_name=step.get_name(),
                created_at=now,
            )
            context = copy.deepcopy(new_context)

    if should_run_process:
        return run_process(process=process, db=db, system_clock=system_clock)

    return None


def perform_queued_task(
    *,
    process_id: int | str,
    step_name: str,
    db: Type[DBHelper],
    system_clock: SystemClock,
) -> ProcessContext | None:
    process, latest_process_context, latest_context = _get_process_and_latest_context(  # type: ignore
        process_id=process_id, db=db
    )
    step_class = _get_step_class(process_name=process.name, step_name=step_name)
    if not issubclass(step_class, TaskQueueStep):
        raise ValidationError("This function may only be called with TaskQueueSteps")

    # This will allow the target step to be run.
    context = step_class.set_as_triggered(latest_context)

    now = system_clock.now()

    process_context = db.create_context(process_id=process.id, context=context, as_at=now)
    db.create_audit_record(
        process_id=process.id,
        context_id=process_context.id,
        step_name=step_class.get_name(),
        status=AuditRecord.Status.TARGET_STEP_TRIGGERED,
        created_at=now,
    )
    process_runner = _get_process_runner(process=process, db=db, system_clock=system_clock)
    return process_runner.run(latest_context_record=process_context)


def run_process(
    *, process: Process, db: Type[DBHelper], system_clock: SystemClock
) -> ProcessContext | None:
    """
    Run an existing process from its latest state.
    """
    process_runner = _get_process_runner(process=process, db=db, system_clock=system_clock)
    return process_runner.run()


# Helpers


def _get_process_and_latest_context(
    *, process_id: str | int, db: Type[DBHelper]
) -> Tuple[Process, ProcessContext, C]:
    """
    :raises ProcessError: if it cannot find the process or a context.
    """
    process = db.get_process(process_id)
    if not process:
        raise ProcessError(f"Could not find a process with key {process_id}")

    latest_process_context = db.get_latest_context(process_id)
    if not latest_process_context:
        raise ProcessError("No context found for process.")

    process_definition = _get_process_definition_by_name(process.name)
    latest_context = process_definition.get_context(latest_process_context)

    return process, latest_process_context, latest_context


def _get_process_definition_by_name(process_name: str) -> Type[ProcessDefinition]:
    return PROCESS_NAMES_TO_DEFINITIONS[process_name]


def _get_step_class(process_name: str, step_name: str) -> Type[Step]:
    return STEPS_FOR_PROCESSES[process_name][step_name]


def _get_process_runner(
    process: Process, db: Type[DBHelper], system_clock: SystemClock
) -> _ProcessRunner:
    process_runner: _ProcessRunner = _ProcessRunner(
        process=process,
        db_helper=db,
        system_clock=system_clock,
    )
    return process_runner


def _format_exception(e: Exception) -> str:
    class_name = e.__class__.__name__
    return f"{class_name}: {str(e)}"
