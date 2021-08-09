import re
import requests
import logging
from operator import itemgetter
import arrow
import pygeodesy.ellipsoidalVincenty as eV

station_re = re.compile(
    "^(?P<station_id>[0-9]{5}) (?P<from>[0-9]{8}) (?P<until>[0-9]{8})\s+"
    "(?P<elevation>-?[0-9]{1,4})\s+(?P<lat>[45][0-9]\.[0-9]{4})\s+"
    "(?P<lon>[1]?[0-9]\.[0-9]{4})\s+(?P<station_name>[A-ZÄ-Ü].*\S)\s+"
    "(?P<state>[A-Z].*\S)"
)
hourly_file_re = re.compile(
    "(?P<file_name>stundenwerte_P0_(?P<station_id>[0-9]{5})_(?:akt|(?:[0-9]{8}_[0-9]{8}_hist)).zip)</a>"
)
ten_minutes_file_re = re.compile(
    "(?P<file_name>10minutenwerte_TU_(?P<station_id>[0-9]{5})_(?:now|akt|(?:[0-9]{8}_[0-9]{8}_hist)).zip)</a>"
)


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
    stations_file = "P0_Stundenwerte_Beschreibung_Stationen.txt"
    stations_response = requests.get(url + stations_file)
    if not stations_response.status_code == 200:
        logging.warning("no valid response from server")
        return []
    files_response = requests.get(url)
    if not files_response.status_code == 200:
        logging.warning("no valid response from server")
        return []
    file_names = {
        _x["station_id"]: _x["file_name"]
        for _x in hourly_file_re.finditer(files_response.text)
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
            if _station_id not in file_names:
                continue
            _station_data["file_name"] = url + file_names[_station_id]
            _station_location = eV.LatLon(
                _station_data["lat"], _station_data["lon"]
            )
            _distance = selected_location.distanceTo(_station_location)
            _station_data["distance"] = round(_distance)
            stations.append(_station_data)
    sorted_stations = sorted(stations, key=itemgetter("distance"))
    return {"category": category, "stations": sorted_stations}


def get_10_minutes_stations(date, lat: float, lon: float):
    selected_date = arrow.get(date)
    today = arrow.utcnow().floor("day")
    yesterday = today.shift(days=-1)
    stations = []
    if arrow.get(date) < yesterday.shift(days=-500):
        category = "historical"
        stations_file = "zehn_min_tu_Beschreibung_Stationen.txt"
    elif arrow.get(date) < today:
        category = "recent"
        stations_file = "zehn_min_tu_Beschreibung_Stationen.txt"
    else:
        category = "now"
        stations_file = "zehn_now_tu_Beschreibung_Stationen.txt"
    url = (
        "https://opendata.dwd.de/climate_environment/CDC/observations_germany"
        f"/climate/10_minutes/air_temperature/{category}/"
    )
    stations_response = requests.get(url + stations_file)
    if not stations_response.status_code == 200:
        logging.warning("no valid response from server")
        return []
    files_response = requests.get(url)
    if not files_response.status_code == 200:
        logging.warning("no valid response from server")
        return []
    file_names = {
        _x["station_id"]: _x["file_name"]
        for _x in ten_minutes_file_re.finditer(files_response.text)
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
            if _station_id not in file_names:
                continue
            _station_data["file_name"] = url + file_names[_station_id]
            _station_location = eV.LatLon(
                _station_data["lat"], _station_data["lon"]
            )
            _distance = selected_location.distanceTo(_station_location)
            _station_data["distance"] = round(_distance)
            #            _station_data["file_url"] =
            stations.append(_station_data)
    sorted_stations = sorted(stations, key=itemgetter("distance"))
    return {"category": category, "stations": sorted_stations}


if __name__ == "__main__":
    hourly_stations = get_hourly_stations(
        date="20210804T1849", lat=52.52, lon=7.30
    )
    print(f"hourly ({hourly_stations['category']}):")
    for _station in hourly_stations["stations"][:6]:
        print(
            f"{_station['distance']/1e3:.1f}km distance to "
            f"{_station['station_name']}, {_station['file_name']}"
        )
    ten_minutes_stations = get_10_minutes_stations(
        date="20210808T1349", lat=52.52, lon=7.30
    )
    print(f"10_minutes ({ten_minutes_stations['category']}):")
    for _station in ten_minutes_stations["stations"][:6]:
        print(
            f"{_station['distance']/1e3:.1f}km distance to "
            f"{_station['station_name']}, {_station['file_name']}"
        )
