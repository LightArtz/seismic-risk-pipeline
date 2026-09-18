import requests, json, time

url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

all_features = []

temp_feature = []

for year in range(2015, 2025):  # 2015 through 2024
    params = {
        "format": "geojson",
        "starttime": f"{year}-01-01",
        "endtime": f"{year+1}-01-01",
        "minmagnitude": 4.5,
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    all_features.extend(data["features"])
    print(f"{year}: {len(data['features'])} events")
    time.sleep(0.5)  # small courtesy delay on a free public service

    '''
    # only for testing
    if year == 2015:
        temp_feature.extend(data["features"])
    '''

print(f"Total: {len(all_features)} earthquakes")

with open("raw_earthquakes.json", "w") as f:
    json.dump({"type": "FeatureCollection", "features": all_features}, f)

'''
# only for testing
with open("temp_raw_earthquakes.json", "w") as f:
    json.dump({"type": "FeatureCollection", "features": temp_feature}, f)
'''
