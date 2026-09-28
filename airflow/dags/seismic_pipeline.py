import requests, json, pendulum
import os, pandas as pd

from airflow.sdk import dag, task, CronDataIntervalTimetable
from datetime import timedelta
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.bash import BashOperator

def notify_failure(context):
    dag_id = context.get('dag_run').dag_id
    task_id = context.get('task_instance').task_id
    exception = context.get('exception')

    print(f"DAG Failed: {dag_id}")
    print(f"Task Failed: {task_id}")
    print(f"Error: {exception}")

def parse_feature(feature: dict) -> dict:
    return {
        "id": feature["id"],
        "mag": feature["properties"]["mag"],
        "place": feature["properties"]["place"],
        "time": feature["properties"]["time"],
        "longitude": feature["geometry"]["coordinates"][0],
        "latitude": feature["geometry"]["coordinates"][1],
        "depth_km": feature["geometry"]["coordinates"][2],
    }

@dag(
    dag_id="seismic_pipeline",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone='UTC'),
    start_date=pendulum.datetime(2026, 9, 21, tz='UTC'),
    catchup=False,
    default_args={
        'retries': 3, 
        'retry_delay': timedelta(seconds=10),
        'on_failure_callback': notify_failure,
    }
)

def seismic_pipeline():
    @task
    def extract_usgs(**context):
        url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
        
        starttime = context["data_interval_start"]
        endtime = context["data_interval_end"]
        
        params = {
            "format": "geojson",
            "starttime": f"{starttime.strftime('%Y-%m-%d')}",
            "endtime": f"{endtime.strftime('%Y-%m-%d')}",
            "minmagnitude": 4.5,
        }

        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )

        session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        resp = session.get(url, params=params, timeout=5)
        print(f"Requesting: {resp.url}")
        resp.raise_for_status()
        data = resp.json()

        if len(data['features']) >= 20000:
            raise ValueError('The total data is over 20000.')

        filename = f"/opt/airflow/data/raw_earthquakes_{starttime.strftime('%y-%m-%d')}.json"

        with open(filename, "w") as f:
            json.dump({"type": "FeatureCollection", "features": data['features']}, f)

        return filename

    @task
    def load_raw(filename, **context):
        # Input
        with open(f"{filename}") as f:
            data = json.load(f)

        # Flatten
        flat_rows = []

        for d in data['features']:
            flat_rows.append(parse_feature(d))

        df = pd.DataFrame(flat_rows)    
        df.columns = [c.upper() for c in df.columns]

        local_csv = f"earthquake_raw_{context['data_interval_start'].strftime('%y-%m-%d')}.csv"
        df.to_csv(local_csv, index=False)

        # Stage
        conn = SnowflakeHook(snowflake_conn_id="snowflake-seismic_pipeline").get_conn()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS RAW.EARTHQUAKES_RAW (
                ID STRING,
                MAG FLOAT,
                PLACE STRING,
                TIME NUMBER,
                LONGITUDE FLOAT,
                LATITUDE FLOAT,
                DEPTH_KM FLOAT
            )
        """)

        cur.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS RAW.EARTHQUAKES_RAW_DAILY (
                ID STRING,
                MAG FLOAT,
                PLACE STRING,
                TIME NUMBER,
                LONGITUDE FLOAT,
                LATITUDE FLOAT,
                DEPTH_KM FLOAT
            )
        """)

        local_path = os.path.abspath(local_csv).replace("\\", "/")
        put_sql = f"PUT 'file://{local_path}' @RAW.%EARTHQUAKES_RAW_DAILY OVERWRITE = TRUE"
        cur.execute(put_sql)

        copy_sql = """
            COPY INTO RAW.EARTHQUAKES_RAW_DAILY
            FROM @RAW.%EARTHQUAKES_RAW_DAILY
            FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            ON_ERROR = 'ABORT_STATEMENT'
        """
        cur.execute(copy_sql)
        for row in cur.fetchall():
            print(row)

        # Merge
        merge_sql = """
            MERGE INTO RAW.EARTHQUAKES_RAW AS TARGET
                USING RAW.EARTHQUAKES_RAW_DAILY AS SOURCE
                ON (TARGET.ID = SOURCE.ID)

                WHEN MATCHED
                    AND (TARGET.MAG <> SOURCE.MAG
                    OR TARGET.PLACE <> SOURCE.PLACE
                    OR TARGET.TIME <> SOURCE.TIME
                    OR TARGET.LONGITUDE <> SOURCE.LONGITUDE
                    OR TARGET.LATITUDE <> SOURCE.LATITUDE
                    OR TARGET.DEPTH_KM <> SOURCE.DEPTH_KM)
                THEN UPDATE
                    SET TARGET.MAG = SOURCE.MAG,
                    TARGET.PLACE = SOURCE.PLACE,
                    TARGET.TIME = SOURCE.TIME,
                    TARGET.LONGITUDE = SOURCE.LONGITUDE,
                    TARGET.LATITUDE = SOURCE.LATITUDE,
                    TARGET.DEPTH_KM = SOURCE.DEPTH_KM

                WHEN NOT MATCHED
                THEN INSERT (
                    TARGET.ID,
                    TARGET.MAG,
                    TARGET.PLACE,
                    TARGET.TIME,
                    TARGET.LONGITUDE,
                    TARGET.LATITUDE,
                    TARGET.DEPTH_KM
                )
                VALUES (
                    SOURCE.ID,
                    SOURCE.MAG,
                    SOURCE.PLACE,
                    SOURCE.TIME,
                    SOURCE.LONGITUDE,
                    SOURCE.LATITUDE,
                    SOURCE.DEPTH_KM
                )   
        """
        cur.execute(merge_sql)
        cur.close()
        conn.close()

    @task
    def verify_load(filename):
        # Input
        with open(f"{filename}") as f:
            data = json.load(f)

        # Flatten
        extracted_ids = [d['id'] for d in data['features']]

        if not extracted_ids:
            return
        
        conn = SnowflakeHook(snowflake_conn_id="snowflake-seismic_pipeline").get_conn()
        cur = conn.cursor()

        placeholders = ",".join(["%s"] * len(extracted_ids))
        
        sql = f"SELECT ID FROM RAW.EARTHQUAKES_RAW WHERE ID IN ({placeholders})"

        cur.execute(sql, tuple(extracted_ids))

        rows = cur.fetchall() # [('id1', ), ('id2', )]
        found_ids = [row[0] for row in rows]

        cur.close()
        conn.close()

        diff_ids = set(extracted_ids) - set(found_ids)

        if diff_ids:
            raise ValueError(f"The ids are not complete. There are {len(extracted_ids)} daily earthquakes, but {len(found_ids)} found in the table. The missing ids: {diff_ids}")

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_profile",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_profile",
    )
    
    filename = extract_usgs()
    merge_result = load_raw(filename)
    check = verify_load(filename)
    merge_result >> check >> dbt_run >> dbt_test

seismic_pipeline()