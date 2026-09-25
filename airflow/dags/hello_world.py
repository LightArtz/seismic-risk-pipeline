from airflow.sdk import dag, task
import pendulum
from datetime import timedelta

@dag(
    dag_id='hello_world',
    schedule='@daily',
    start_date=pendulum.datetime(2026, 9, 20, tz = 'UTC'),
    catchup=False,
    tags=["learning"],
    default_args={'retries': 2, 'retry_delay': timedelta(seconds=10)},
)

def hello_world():
    @task
    def say_hello():
        print("Hello World!")

    @task
    def say_interval(**context):
        interval_start = context["data_interval_start"]
        interval_end = context["data_interval_end"]
        print(f"Interval date -> Start: {interval_start}, end: {interval_end}")

    @task
    def say_bye():
        print("Bye World!")

    say_hello() >> say_interval() >> say_bye()

hello_world()