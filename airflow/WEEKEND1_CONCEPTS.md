# Weekend 1: Airflow Foundations — Concepts

Notes from building and breaking `hello_dag`, in my own words.

## DAG (Directed Acyclic Graph)
A DAG is a collection of tasks with a defined order of execution. **Directed**
means each task points to the next one it depends on. **Acyclic** means the
graph can't loop back on itself — there's always a clear start and end, never
a task depending on itself, even indirectly.

## Task
A task is a single unit of work inside a DAG — in my case, a Python function
wrapped with `@task`. Tasks run as separate processes, each with its own log,
retry count, and success/failure state. They're connected in order (`>>`),
but each one executes and is tracked independently.

## Operator
The piece I hadn't learned yet. An operator is a *template* for a kind of
work — `PythonOperator` runs a Python function, `BashOperator` runs a shell
command, etc. A task is a specific instance of an operator. The `@task`
decorator I've been using is TaskFlow syntax that wraps a `PythonOperator`
automatically — it's hiding the operator underneath. My `extract_usgs` and
`load_raw` tasks will likely use `@task`/`PythonOperator`, but `dbt_run` and
`dbt_test` are planned as `BashOperator` tasks, since they just shell out to
the `dbt` CLI.

## Scheduler
A running process (one of the 8 containers) that continuously loops and
decides two things: (1) is a new DAG run due, based on the schedule, and
(2) for DAG runs already in progress, which tasks now have their upstream
dependencies satisfied and can be queued next. It's why `say_interval`
started the instant `say_hello` succeeded — nobody manually triggered it;
the scheduler noticed and queued it.

## Data interval
The period of *source data* a run is responsible for — separate from the
wall-clock time it actually executes. With `schedule='@daily'`, the run
Airflow labels "Sept 22" doesn't execute on Sept 22 — it runs right after
midnight on Sept 23, once the 22nd has fully closed, with
`data_interval_start` = Sept 22 00:00 and `data_interval_end` = Sept 23
00:00. This is why `extract_usgs` pulls data for the run's data interval,
not for "today" — those are different dates, and mixing them up is a classic
off-by-one-day bug.

Manually triggering a run doesn't produce a zero-length interval either.
Airflow looks at the schedule and assigns the most recently *completed*
period before the trigger moment — e.g. triggering at 3pm still gets
assigned the most recent full day, not "now to now."

## Retries
Set per task (in `default_args`), not per DAG run. `retries=2` means 2
*additional* attempts after the first failure — 3 total tries — spaced by
`retry_delay`. If a task exhausts its retries, that specific DAG run stalls
(downstream tasks go `upstream_failed` and don't run at all), but it does
**not** cancel future scheduled runs. Tomorrow's run still fires on schedule;
only that one day's run sits failed until manually cleared and fixed.

## XCom (cross-communication)
Small values passed between tasks, stored in Airflow's own metadata database
(the same Postgres used for run history). If a `@task` function returns a
value and a downstream task takes it as an argument, Airflow stores it as an
XCom automatically. This is why the plan explicitly avoids passing large
data through XCom — `extract_usgs` writes raw JSON to disk and passes only
the **file path** downstream, not the payload itself, so the metadata
database (which the scheduler depends on) never gets bloated.

## Catchup vs. Backfill
**Catchup** is automatic: with `catchup=True`, on first deploy the scheduler
walks forward one schedule period at a time from `start_date` — check the
last DAG run row in the database, add one period, check if that period has
already closed, create the row if so, repeat — until it reaches a period
that hasn't finished yet (i.e., "now"). It's a loop, not a gap-scan.
`catchup=False` skips this and starts from the present.

**Backfill** is a deliberate, manual action — explicitly telling Airflow to
run a DAG for a specific past date range, regardless of `catchup` or whether
those runs already exist. This is different from "Clear," which re-runs an
already-existing (usually failed) run without creating new ones for dates
that were never run at all.

## What I proved hands-on
- Built and ran a 3-task DAG (`hello_dag`) from a worked example.
- Added a 4th task myself.
- Broke a task on purpose (raised an exception), watched it retry 3 times
  (1 initial attempt + 2 retries) roughly 10s apart, end in `failed`, and
  saw the downstream task marked `upstream_failed` without ever running.
- Confirmed upstream tasks that already succeeded were untouched by the
  failure — no rollback in Airflow, which is exactly why idempotent design
  (like a `MERGE` instead of an `INSERT` in `load_raw`) matters.
- Recovered by clearing just the failed task (with "Downstream" checked),
  and confirmed via timestamps that only the cleared tasks re-ran — the
  already-successful ones kept their original run time.
- Rewrote the DAG skeleton from a blank file, unaided, to check retention.

**Stop point reached:** a DAG that runs, fails on purpose, retries, and
recovers.
