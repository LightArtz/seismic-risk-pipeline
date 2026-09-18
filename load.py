import json

with open('raw_earthquakes.json') as f:
    data = json.load(f)

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



import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

df = pd.DataFrame(flat_rows)
df.columns = [c.upper() for c in df.columns]

local_csv = "earthquakes_raw.csv"
df.to_csv(local_csv, index=False)  # header=True (default) — COPY INTO will skip it explicitly below

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema="RAW",
)
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS EARTHQUAKES_RAW (
        ID STRING,
        MAG FLOAT,
        PLACE STRING,
        TIME NUMBER,
        LONGITUDE FLOAT,
        LATITUDE FLOAT,
        DEPTH_KM FLOAT
    )
""")

# Windows needs: forward slashes, and single-quotes around the whole URI
# whenever the path has spaces — yours does ("...seismic snowflake dbt...").
local_path = os.path.abspath(local_csv).replace("\\", "/")
put_sql = f"PUT 'file://{local_path}' @%EARTHQUAKES_RAW OVERWRITE = TRUE"
cur.execute(put_sql)

copy_sql = """
    COPY INTO EARTHQUAKES_RAW
    FROM @%EARTHQUAKES_RAW
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'ABORT_STATEMENT'
"""
cur.execute(copy_sql)
for row in cur.fetchall():
    print(row)

cur.close()
conn.close()