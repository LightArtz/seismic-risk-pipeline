"""
hello_dag: a minimal 3-task DAG to learn Airflow's basic shapes.

Tasks: say_hello -> say_data_interval -> say_bye
This chain is artificial (no real dependency between the tasks' outputs) --
it exists only to show how tasks link together and run in order.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from datetime import timedelta
# Airflow 3 uses airflow.sdk for the @dag/@task decorators (this replaces
# airflow.decorators, which still works but is the 2.x-style import path).


@dag(
    dag_id="hello_dag",
    schedule="@daily",       # run once per day
    start_date=pendulum.datetime(2026, 9, 20, tz="UTC"),
    catchup=False,            # don't backfill past runs on first deploy
    tags=["learning"],
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
)
def hello_dag():

    @task
    def say_hello():
        print("Hello from Airflow!")

    @task
    def say_data_interval(**context):
        # This is the templated, correct way to get "today" for this run --
        # never datetime.now() or datetime.today() inside a task, because
        # that breaks reruns and backfills (the plan's "no datetime.now()" rule).
        interval_start = context["data_interval_start"]
        interval_end = context["data_interval_end"]
        print(f"This run covers data interval: {interval_start} to {interval_end}")

    @task
    def say_bye():
        print("Goodbye from Airflow!")
        # raise TypeError("This is a raise.")

    @task
    def say_see_you():
        print("See you soon :D")

    # This line defines the order: hello runs, then say_data_interval, then bye.
    say_hello() >> say_data_interval() >> say_bye() >> say_see_you()


hello_dag()