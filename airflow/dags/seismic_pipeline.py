import requests, json, time, pendulum, json
import os, pandas as pd, snowflake.connector

from airflow.sdk import dag, task, CronDataIntervalTimetable
from datetime import timedelta
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

@dag(
    dag_id="seismic_pipeline",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone='UTC'),
    start_date=pendulum.datetime(2026, 9, 21, tz='UTC'),
    catchup=True,
    default_args={'retries': 3, 'retry_delay': timedelta(seconds=10)}
)

def seismic_pipeline():
    @task
    def extract_usgs(**context):
        url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

        print(f"context start/end: {context['data_interval_start']} / {context['data_interval_end']}")
        print(f"dag_run start/end: {context['dag_run'].data_interval_start} / {context['dag_run'].data_interval_end}")
        print(f"logical_date: {context.get('logical_date')}")
        
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
            earthquake = {
                "id": d["id"],
                "mag": d["properties"]["mag"],
                "place": d["properties"]["place"],
                "time": d["properties"]["time"],
                "longitude": d["geometry"]["coordinates"][0],
                "latitude": d["geometry"]["coordinates"][1],
                "depth_km": d["geometry"]["coordinates"][2]
            }

            flat_rows.append(earthquake)    

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
    def verify_load():
        conn = SnowflakeHook(snowflake_conn_id="snowflake-seismic_pipeline").get_conn()
        cur = conn.cursor()

        # Input
        with open(f"{filename}") as f:
            data = json.load(f)

        # Flatten
        extracted_ids = []

        for d in data['features']:
            earthquake = {
                "id": d["id"],
            }
            extracted_ids.append(earthquake)

        cur.execute(f"SELECT ID FROM {} WHERE ID NOT IN (SELECT ID FROM RAW.EARTHQUAKES_RAW)", )
        # I want to build if ID exist, then show the ID diff and raise. If it returns nothing, then succeed. But I don't know if doing it full SQL is the best practice.
    
    filename = load_raw(extract_usgs())
    load_raw(filename) >> verify_load(filename)

seismic_pipeline()