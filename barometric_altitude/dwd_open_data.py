import re
import requests
import logging
import io
import zipfile
import datetime as dt
from operator import itemgetter
import arrow
import pygeodesy.ellipsoidalVincenty as eV
from barometric_altitude.timeit import timeit
import pandas as pd

station_re = re.compile(
    "^(?P<station_id>[0-9]{5}) (?P<from>[0-9]{8}) (?P<until>[0-9]{8})\s+"
    "(?P<elevation>-?[0-9]{1,4})\s+(?P<lat>[45][0-9]\.[0-9]{4})\s+"
    "(?P<lon>[1]?[0-9]\.[0-9]{4})\s+(?P<station_name>[A-ZÄ-Ü].*\S)\s+"
    "(?P<state>[A-Z].*\S)"
)
pressure_hourly_file_re = re.compile(
    "(?P<file_name>stundenwerte_P0_(?P<station_id>[0-9]{5})_(?:akt|(?:[0-9]{8}_[0-9]{8}_hist)).zip)</a>"
)
temperature_hourly_file_re = re.compile(
    "(?P<file_name>stundenwerte_TU_(?P<station_id>[0-9]{5})_(?:akt|(?:[0-9]{8}_[0-9]{8}_hist)).zip)</a>"
)


@timeit
def unpack_zipped_data(my_file, file_name_prefix):
    my_zipfile = zipfile.ZipFile(my_file, "r")
    for file_name in my_zipfile.namelist():
        if file_name.startswith(file_name_prefix):
            break
    with my_zipfile.open(file_name) as f:
        df = pd.read_csv(f, delimiter=";", skipinitialspace=True)
        df.drop(columns=["STATIONS_ID", "eor"], inplace=True)
        return df


@timeit
def unpack_zipped_data_from_url(url, file_name_prefix):
    response = requests.get(url)
    if not response.status_code == 200:
        logging.warning("no data downloaded.")
        return None
    return unpack_zipped_data(io.BytesIO(response.content), file_name_prefix)


@timeit
def get_hourly_stations(date, lat: float, lon: float):
    selected_date = arrow.get(date)
    yesterday = arrow.utcnow().floor("day").shift(days=-1)
    stations = []
    if arrow.get(date) < yesterday.shift(days=-500):
        category = "historical"
    else:
        category = "recent"
    url = (
        "https://opendata.dwd.de/climate_environment/CDC/observations_germany"
        f"/climate/hourly/pressure/{category}/"
    )
    temperature_url = (
        "https://opendata.dwd.de/climate_environment/CDC/observations_germany"
        f"/climate/hourly/air_temperature/{category}/"
    )
    stations_file = "P0_Stundenwerte_Beschreibung_Stationen.txt"
    stations_response = requests.get(url + stations_file)
    if not stations_response.status_code == 200:
        logging.warning("no valid response from server")
        return []
    pressure_files_response = requests.get(url)
    if not pressure_files_response.status_code == 200:
        logging.warning("no valid response from server")
        return []
    pressure_file_names = {
        _x["station_id"]: _x["file_name"]
        for _x in pressure_hourly_file_re.finditer(pressure_files_response.text)
    }
    temperature_files_response = requests.get(temperature_url)
    if not temperature_files_response.status_code == 200:
        logging.warning("no valid response from server")
        return []
    temperature_file_names = {
        _x["station_id"]: _x["file_name"]
        for _x in temperature_hourly_file_re.finditer(
            temperature_files_response.text
        )
    }
    selected_location = eV.LatLon(lat, lon)
    for _line in stations_response.text.splitlines():
        _match = station_re.match(_line)
        if _match is not None:
            _station_data = _match.groupdict()
            _from = arrow.get(_station_data["from"])
            if _from > selected_date:
                continue
            _until = arrow.get(_station_data["until"])
            if _until < selected_date.floor("day"):
                continue
            _station_id = _station_data["station_id"]
            if _station_id not in pressure_file_names:
                continue
            _station_data["pressure_file_name"] = (
                url + pressure_file_names[_station_id]
            )
            if _station_id not in temperature_file_names:
                continue
            _station_data["temperature_file_name"] = (
                temperature_url + temperature_file_names[_station_id]
            )
            _station_location = eV.LatLon(
                _station_data["lat"], _station_data["lon"]
            )
            _distance = selected_location.distanceTo(_station_location)
            _station_data["distance"] = round(_distance)
            stations.append(_station_data)
    sorted_stations = sorted(stations, key=itemgetter("distance"))
    return {"category": category, "stations": sorted_stations}


@timeit
def get_nearest_hourly_data(
    date,
    lat: float,
    lon: float,
    as_dataframe=False,
    bounds: dt.timedelta = None,
):
    hourly_stations = get_hourly_stations(date, lat, lon)
    if len(hourly_stations) == 0:
        logging.warning("no suitable stations found.")
        return None
    nearest_station = hourly_stations["stations"][0]
    pressure_data = unpack_zipped_data_from_url(
        nearest_station["pressure_file_name"], "produkt_P0_stunde_"
    )
    temperature_data = unpack_zipped_data_from_url(
        nearest_station["temperature_file_name"], "produkt_TU_stunde_"
    )
    combined_data = pd.merge(pressure_data, temperature_data, on="MESS_DATUM")
    combined_data.MESS_DATUM = pd.to_datetime(
        combined_data.MESS_DATUM, format="%Y%m%d%H"
    ) - dt.timedelta(minutes=10)
    combined_data["utc"] = (
        (combined_data.MESS_DATUM - dt.datetime(1970, 1, 1))
        .dt.total_seconds()
        .astype(int)
    )
    combined_data.rename(
        columns={
            "P": "pressure",
            "P0": "p0",
            "TT_TU": "temperature",
            "RF_TU": "humidity",
        },
        inplace=True,
    )
    combined_data.set_index("MESS_DATUM", inplace=True)
    combined_data.drop(columns=["QN_8", "QN_9"], inplace=True)
    if bounds is not None:
        _date = pd.to_datetime(date)
        _start = _date - bounds
        _stop = _date + bounds
        mask = (combined_data.index >= _start) & (combined_data.index < _stop)
        combined_data = combined_data.loc[mask]
    if as_dataframe:
        data = combined_data
    else:
        data = combined_data.to_dict("records")
    return {
        "category": hourly_stations["category"],
        "station": nearest_station,
        "data": data,
    }


if __name__ == "__main__":
    hourly_stations = get_hourly_stations(
        date="20210804T1849", lat=52.52, lon=7.30
    )
    print(f"hourly ({hourly_stations['category']}):")
    for _station in hourly_stations["stations"][:6]:
        print(
            f"{_station['distance']/1e3:.1f}km distance to "
            f"{_station['station_name']}, {_station['pressure_file_name']}, "
            f"{_station['temperature_file_name']}"
        )
    data = get_nearest_hourly_data(
        date="20210804T1849", lat=52.52, lon=7.30, as_dataframe=False
    )
    print(f"downloaded nearest hourly data for {data['station']}.")
    print(f"first entry: {data['data'][0]}")
    print(f"last entry: {data['data'][-1]}")
    data = get_nearest_hourly_data(
        date="20210804T1849",
        lat=52.52,
        lon=7.30,
        as_dataframe=False,
        bounds=dt.timedelta(minutes=30),
    )
    print(f"downloaded nearest hourly data for {data['station']}.")
    print(f"target entry: {data['data']}")
