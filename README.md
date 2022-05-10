# Wolfi

#### Context-driven processes

Wolfi is a library that helps you construct step-wise processes with automatic audit and context logging.

Named after Octopus wolfi, the world's smallest octopus, the aim is to have the smallest everything:
    * user APIs 
    * boilerplate
    * time taken to understand the concepts
    * time taken writing tests for user processes

### Why you should use wolfi

* Quick to set up processes
* Easy to read
* DB- and framework-agnostic
* Implementation details are completely hidden from the user
* Quick set-up for asynchronous and unsolicited requests

### Why you should not use wolfi

* Your process should be _mostly_ linear. If you have a lot of branched logic, this library will probably not be appropriate. Simple either/or should be fine.
* Not the most space-efficient. Every version of the process's context is saved. This results in lots of entries of key-value stores per process.
* As of Jan 2021, this is a very new project, and you have to be prepared to work on both your process and the library itself. Both minor and major tweaks are expected as the library is pushed to its limits with more usecases.

## Requirements

Python 3.0+


## Examples

Go to `tests/example_processes/` to see some examples of user implementations of varying complexity.

## Main concepts

A **process** is something that completes when its constituent steps have all run and are successful.

Each **process** has a **globally unique name**, a primary key and an idempotency key such that there may not be more than one active process per idempotency key and name.

The library’s process runner will take a **process definition** and run specified **steps** in order. Each process has a starting **context**, which is passed through to each step on the way.

When something changes, a new **context** and an **audit record** will be generated and saved to the DB. Every time the context changes, we save a new version. It is possible to restart a process from a previous version of the context.

The **context** is a free-form array that stores all fields required for the entire process to run, plus any data that the user is interested in. The engine itself will add and use keys that track all the steps (“step B is done”, “step C has been queued” and so on).

A process definition may also have zero or more unsolicited steps. These listen out for events that should occur while the process is active, but are untouched by the process runner.

### Process

A set of actions that are required to achieve a specific high level goal. For example, onboard a new customer.

### Steps

Each process must contain one or more steps. This is a discrete action that the process has to perform. By default,
the order of the steps is maintained and preserved. This is important if an asynchronous request has been made and the
process has to wait for the response to be processed before continuing.

The library supports several types of steps.

#### Synchronous

This is a step that does all of its required actions in one block. The absence of exceptions means success, and it 
doesn't have to wait for any external events before succeeding.

#### Asynchronous

For steps that require a response that comes back at an indeterminate amount of time.

Sometimes we need to send a request or a file to a third party and wait for a response to come back at an undetermined
time. Here, we have to tell the process to wait until something prods it again, e.g. when a message comes back.

Each asynchronous action requires a correlation ID that the response will use to trace it back to the process. This ID
may be one that is already in use by the application (e.g. a request ID), or it may be an idempotency key that is
generated specifically for this purpose.

#### Task queue

Steps that are scheduled to be performed some time in the future.

Occasionally, we may need to delay a task until after a specific time or make use of some rate-limiting feature and can
achieve that by putting a step onto a task queue.

In this library, this is represented by a separate step. Whatever is called by the worker that pulls the task from the
queue should be another synchronous or asynchronous step.

#### Simultaneously scheduled task queue steps

There is also a constructor available for several task queue steps to be scheduled simultaneously.

#### Unsolicited step

A message or and event that comes without being directly prompted by the process.

Some processes may need to listen out for a message or event in the application that the process itself did not 
request and therefore cannot have a correlation ID. However, the  process is interested and should be updated
if it happens during its active window.

### Context

This is an array of data for the process. It includes tracking for the steps completed as well as user-defined data
that fills in as steps progress.

### Audit record

This should do what it says on the tin - it's a record of discrete actions that have occurred during the timeline of
a process. This may be recorded in the database or by a logging service.

## Getting started

### Setting up the very first process in a project

Because Wolfi is DB-agnostic, you must implement `DBHelper` class for whatever DB-accessing tool you have (e.g. Django ORM),
as well as all the DB schema objects (e.g. if you use Django, you must implement the model classes):

1. Process
2. ProcessContext
3. AuditRecord (though you may choose to use a logging service instead)

### All processes

As the user, you must use subclasses of the following:

1. ProcessDefinition
2. Context
3. Steps (`SimpleStep`/ `AsyncStep`/`TaskQueueStep`)

You must also register the process definitions and their steps.

Use the `initiate_process` function to kick off a new process.

If you have any asynchronous steps, place `handle_response_received` at the end of message/event processing with the 
`Response` object of choice (for now, this can be anything the user chooses).

If you have task queue steps, call `perform_step_for_process` from the task queue.

### Entry points

The library provides all entry points into all processes.

#### Initiation

When a process is initiated, it has to instantiate the context class, fill it up with the required values to begin, and populate it with all the tracking information. The process can then start running. All of this is handled by the library so users only need to write simple wrappers around it.

```py
def initiate_process(
    *,
    group_id: str,
    process_definition: Type[ProcessDefinition],
    db: Type[DBHelper],
    context_kwargs: dict,
    system_clock: SystemClock,
    user_id: int | str | None = None,
) -> Context | None:
    ...
```

#### Handling a response or unsolicited message

A library function is provided for correlating responses back to the processes that need them.

The same handle is used for unsolicited requests. The difference is that because unsolicited responses won’t have a correlation ID, they have to be correlated back to a process’s idempotency key (here called the group_id).

```py
def handle_response_received(
    *,
    correlation_id: str,
    group_ids: Sequence[str],
    response: Any,
    db: Type[DBHelper],
    system_clock: SystemClock,
) -> Context | None:
    ...
```

### Triggering a task from a task queue

Only one function ever needs to be called from the task queue. The actual action is not called from the task queue. What happens is that the task queue step is being marked as triggered and then the process is run as per usual. Now that the step has been marked as triggered from the task queue, it immediately performs the intended action. The result is the same as queueing the action, but there is no need to create a separate celery task per action.

```py
def perform_queued_task(
    *,
    process_id: int | str,
    step_name: str,
    db: Type[DBHelper],
    system_clock: SystemClock,
) -> Context | None:
    ...
```

### Helpers

#### DB class

Wolfi is DB-agnostic. Instead, the engine specifies an interface to the DB that the user must implement (as a class), but only once. This implementation is passed into the entry points.

### System clock

This is another simple interface that is injected into the entry points. This is so that the engine can call `now()` at any point in its runtime to record an accurate time. This also makes it easy to mock out in tests.


### Contributing

In is trial period, Wolfi is going to be embedded into Kraken so we can speed up development cycles.

There are lots of things that could be improved. Tests, documentation, clarity of implementation code, the inevitable bugs.
I would encourage you to write processes and tests with it straight away and see what things fail unexpectedly, and make
pull requests as you normally would if you spot any bugs.

Suggestions:

- The registry could be greatly improved. We currently rely on developers to remember to register process definitions.
It would be nice to remove this.
  
- More robust tests for idempotency

- Support obvious cases of concurrency