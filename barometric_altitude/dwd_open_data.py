import re
import requests
from requests_cache import CachedSession
import logging
import io
import zipfile
import datetime as dt
from operator import itemgetter
import scipy.constants
import arrow
import pandas as pd
from pydantic import constr
import pygeodesy.ellipsoidalVincenty as eV
from barometric_altitude.timeit import timeit
import barometric_altitude as ba

# logging.basicConfig(level="INFO")


class HourlyCatalog:
    def __init__(self):
        self.url = (
            "https://opendata.dwd.de/climate_environment/CDC/"
            "observations_germany/climate/hourly/"
        )
        self.station_re = re.compile(
            "(?P<station_id>[0-9]{5}) (?P<from>[0-9]{8}) (?P<until>[0-9]{8})"
            "\\s+(?P<elevation>-?[0-9]{1,4})\\s+(?P<lat>[45][0-9]\\.[0-9]{4})"
            "\\s+(?P<lon>[1]?[0-9]\\.[0-9]{4})\\s+(?P<station_name>[A-ZÄ-Ü].*"
            "\\S)\\s+(?P<state>[A-Z].*\\S)"
        )
        self.pressure_re = re.compile(
            "(?P<file_name>stundenwerte_P0_(?P<station_id>[0-9]{5})_"
            "(?:akt|(?:[0-9]{8}_[0-9]{8}_hist)).zip)</a>"
        )
        self.temperature_re = re.compile(
            "(?P<file_name>stundenwerte_TU_(?P<station_id>[0-9]{5})_"
            "(?:akt|(?:[0-9]{8}_[0-9]{8}_hist)).zip)</a>"
        )
        self.updated = None
        self.stations = None
        self.pressure = {"recent": None, "historical": None}
        self.temperature = {"recent": None, "historical": None}

    @timeit
    def download_catalog(self):
        _url = f"{self.url}pressure/recent/"
        stations_response = requests.get(
            _url + "P0_Stundenwerte_Beschreibung_Stationen.txt"
        )
        if not stations_response.status_code == 200:
            logging.warning("no valid response from server")
            return None
        self.stations = [
            _x.groupdict()
            for _x in self.station_re.finditer(stations_response.text)
        ]
        for category in ("historical", "recent"):
            _url = f"{self.url}pressure/{category}/"
            pressure_response = requests.get(_url)
            if not pressure_response.status_code == 200:
                logging.warning("no valid response from server")
                return None
            self.pressure[category] = {
                _x["station_id"]: _url + _x["file_name"]
                for _x in self.pressure_re.finditer(pressure_response.text)
            }
            _url = f"{self.url}air_temperature/{category}/"
            temperature_response = requests.get(_url)
            if not temperature_response.status_code == 200:
                logging.warning("no valid response from server")
                return None
            self.temperature[category] = {
                _x["station_id"]: _url + _x["file_name"]
                for _x in self.temperature_re.finditer(
                    temperature_response.text
                )
            }
        self.updated = arrow.utcnow()

    @timeit
    def check_catalog(self):
        if self.updated is None:
            self.download_catalog()
        elif self.updated < arrow.utcnow().shift(hours=-8):
            self.download_catalog()
        return self.updated is not None

    @timeit
    def get_catalog(self, date):
        if not self.check_catalog():
            return None
        selected_date = arrow.get(date)
        yesterday = arrow.utcnow().floor("day").shift(days=-1)
        if selected_date < yesterday.shift(days=-500):
            category = "historical"
        else:
            category = "recent"
        _pressures = self.pressure[category]
        _temperatures = self.temperature[category]
        available_stations = [
            {
                **_station,
                "pressure_file_name": _pressures[_station["station_id"]],
                "temperature_file_name": _temperatures[_station["station_id"]],
            }
            for _station in self.stations
            if _station["station_id"] in _pressures
            and _station["station_id"] in _temperatures
            and arrow.get(_station["from"]) <= selected_date
            and arrow.get(_station["until"]) >= selected_date.floor("day")
        ]
        return {"stations": available_stations, "category": category}


class TenMinutesCatalog:
    def __init__(self):
        self.url = (
            "https://opendata.dwd.de/climate_environment/CDC/"
            "observations_germany/climate/10_minutes/air_temperature/"
        )
        self.station_re = re.compile(
            "(?P<station_id>[0-9]{5}) (?P<from>[0-9]{8}) (?P<until>[0-9]{8})"
            "\\s+(?P<elevation>-?[0-9]{1,4})\\s+(?P<lat>[45][0-9]\\.[0-9]{4})"
            "\\s+(?P<lon>[1]?[0-9]\\.[0-9]{4})\\s+(?P<station_name>[A-ZÄ-Ü].*"
            "\\S)\\s+(?P<state>[A-Z].*\\S)"
        )
        self.temperature_re = re.compile(
            "(?P<file_name>10minutenwerte_TU_(?P<station_id>[0-9]{5})_"
            "(?:now|akt|(?:(?P<from>[0-9]{8})_(?P<until>[0-9]{8})_hist)).zip)"
            "</a>"
        )
        self.metadata_re = re.compile(
            "(?P<file_name>Meta_Daten_zehn_min_tu_(?P<station_id>[0-9]{5})"
            ".zip)</a>"
        )
        self.updated = None
        self.stations = {"recent": None, "historical": None, "now": None}
        self.temperature = {
            "recent": None,
            "historical": None,
            "now": None,
        }

    @timeit
    def download_catalog(self):
        _url = f"{self.url}meta_data/"
        metadata_response = requests.get(_url)
        if not metadata_response.status_code == 200:
            logging.warning("no valid response from server")
            return None
        self.metadata = {
            _x["station_id"]: _url + _x["file_name"]
            for _x in self.metadata_re.finditer(metadata_response.text)
        }
        catalog_files = {
            "now": "zehn_now_tu_Beschreibung_Stationen.txt",
            "recent": "zehn_min_tu_Beschreibung_Stationen.txt",
            "historical": "zehn_min_tu_Beschreibung_Stationen.txt",
        }
        for _category, _file in catalog_files.items():
            _url = f"{self.url}/{_category}/{_file}"
            _response = requests.get(_url)
            if not _response.status_code == 200:
                logging.warning("no valid response from server")
                return None
            self.stations[_category] = [
                {
                    **_x.groupdict(),
                    "metadata_file_name": self.metadata[_x["station_id"]],
                }
                for _x in self.station_re.finditer(_response.text)
            ]
            _url = f"{self.url}{_category}/"
            temperature_response = requests.get(_url)
            if not temperature_response.status_code == 200:
                logging.warning("no valid response from server")
                return None
            if _category == "historical":
                self.temperature[_category] = [
                    _x
                    for _x in self.temperature_re.finditer(
                        temperature_response.text
                    )
                ]
            else:
                self.temperature[_category] = {
                    _x["station_id"]: _url + _x["file_name"]
                    for _x in self.temperature_re.finditer(
                        temperature_response.text
                    )
                }
        self.updated = arrow.utcnow()

    @timeit
    def check_catalog(self):
        if self.updated is None:
            self.download_catalog()
        elif self.updated < arrow.utcnow().shift(hours=-8):
            self.download_catalog()
        return self.updated is not None

    @timeit
    def get_catalog(self, date):
        if not self.check_catalog():
            return None
        selected_date = arrow.get(date)
        today = arrow.utcnow().floor("day")
        yesterday = today.shift(days=-1)
        if selected_date < yesterday.shift(days=-500):
            category = "historical"
            _url = f"{self.url}{category}/"
            _temperatures = {
                _x["station_id"]: _url + _x["file_name"]
                for _x in self.temperature[category]
                if arrow.get(_x["from"]) <= selected_date
                and arrow.get(_x["until"]) >= selected_date.floor("day")
            }
        elif selected_date >= today:
            category = "now"
            _temperatures = self.temperature[category]
        else:
            category = "recent"
            _temperatures = self.temperature[category]
        available_stations = [
            {
                **_station,
                "temperature_file_name": _temperatures[_station["station_id"]],
            }
            for _station in self.stations[category]
            if _station["station_id"] in _temperatures
            and arrow.get(_station["from"]) <= selected_date
            and arrow.get(_station["until"]) >= selected_date.floor("day")
        ]
        return {"stations": available_stations, "category": category}


_hourly_catalog = HourlyCatalog()
_ten_minutes_catalog = TenMinutesCatalog()
_session = requests.Session()
_session = CachedSession("dwd_data", expire_after=dt.timedelta(hours=8))


@timeit
def unpack_zipped_data(my_file, file_name_prefix: str):
    my_zipfile = zipfile.ZipFile(my_file, "r")
    response = {}
    for file_name in my_zipfile.namelist():
        if not file_name.endswith(".txt"):
            continue
        elif file_name.startswith(file_name_prefix):
            with my_zipfile.open(file_name) as f:
                df = pd.read_csv(f, delimiter=";", skipinitialspace=True)
            df.drop(columns=["STATIONS_ID", "eor"], inplace=True)
            response["data"] = df
        elif file_name.startswith("Metadaten_Geographie_"):
            with my_zipfile.open(file_name) as f:
                df = pd.read_csv(
                    f,
                    delimiter=";",
                    skipinitialspace=True,
                    parse_dates=["von_datum", "bis_datum"],
                    encoding="latin",
                )
            df.rename(
                columns={
                    "Stations_id": "station_id",
                    "Stationsname": "station_name",
                    "Stationshoehe": "elevation",
                    "von_datum": "from",
                    "bis_datum": "until",
                    "Geogr.Breite": "lat",
                    "Geogr.Laenge": "lon",
                },
                inplace=True,
            )
            df["until"].replace(
                {pd.NaT: dt.datetime.now().replace(microsecond=0)}, inplace=True
            )
            response["elevation_history"] = df
        elif file_name.startswith("Metadaten_Geraete_Luftdruck_"):
            with my_zipfile.open(file_name) as f:
                df = pd.read_csv(
                    f,
                    delimiter=";",
                    skipinitialspace=True,
                    parse_dates=["Von_Datum", "Bis_Datum"],
                    encoding="latin",
                    usecols=[
                        "Stations_ID",
                        "Stationsname",
                        "Stationshoehe [m]",
                        "Geraetetyp Name",
                        "Von_Datum",
                        "Bis_Datum",
                        "Geo. Breite [Grad]",
                        "Geo. Laenge [Grad]",
                    ],
                )
            df.rename(
                columns={
                    "Stations_ID": "station_id",
                    "Stationsname": "station_name",
                    "Stationshoehe [m]": "elevation",
                    "Geraetetyp Name": "device_name",
                    "Von_Datum": "from",
                    "Bis_Datum": "until",
                    "Geo. Breite [Grad]": "lat",
                    "Geo. Laenge [Grad]": "lon",
                },
                inplace=True,
            )
            df.dropna(subset=["elevation", "device_name"], inplace=True)
            response["device_history"] = df
    return response


@timeit
def unpack_zipped_data_from_url(url: str, file_name_prefix: str):
    response = _session.get(url)
    if not response.status_code == 200:
        logging.warning("no data downloaded.")
        return None
    return unpack_zipped_data(io.BytesIO(response.content), file_name_prefix)


@timeit
def get_hourly_stations(date, lat: float = None, lon: float = None):
    stations = []
    catalog = _hourly_catalog.get_catalog(date)
    if catalog is None:
        return []
    response = {"category": catalog["category"]}
    if None in (lat, lon):
        response["stations"] = catalog["stations"]
    else:
        selected_location = eV.LatLon(lat, lon)
        for _station in catalog["stations"]:
            _station_location = eV.LatLon(_station["lat"], _station["lon"])
            _distance = selected_location.distanceTo(_station_location)
            _station["distance"] = round(_distance)
            stations.append(_station)
        sorted_stations = sorted(stations, key=itemgetter("distance"))
        response["stations"] = sorted_stations
    return response


@timeit
def get_hourly_data(
    station: dict,
    category: constr(regex=r"^(historical|recent)$"),
    date,
    as_dataframe: bool = False,
    bounds_minutes: float = None,
):
    pressure_data = unpack_zipped_data_from_url(
        station["pressure_file_name"], "produkt_"
    )
    temperature_data = unpack_zipped_data_from_url(
        station["temperature_file_name"], "produkt_"
    )
    combined_data = pd.merge(
        pressure_data["data"], temperature_data["data"], on="MESS_DATUM"
    )
    combined_data.MESS_DATUM = pd.to_datetime(
        combined_data.MESS_DATUM, format="%Y%m%d%H"
    ) - dt.timedelta(minutes=10)
    combined_data["utc"] = (
        (combined_data.MESS_DATUM - dt.datetime(1970, 1, 1))
        .dt.total_seconds()
        .astype(int)
    )
    if isinstance(date, int):
        _date = dt.datetime.utcfromtimestamp(date)
    else:
        _date = pd.to_datetime(date)
    device_history = pressure_data.get("device_history")
    if device_history is not None:
        _mask = (device_history["until"] + pd.Timedelta(days=1) > _date) & (
            device_history["from"] <= _date
        )
        device_history = device_history.loc[_mask]
        if len(device_history) > 1:
            # prefer electronic over conventional devices if both exist
            device_history = device_history[
                device_history["device_name"] != "Stationsbarometer"
            ]
    elevation_history = pressure_data["elevation_history"]
    _mask = (elevation_history["until"] + pd.Timedelta(days=1) > _date) & (
        elevation_history["from"] <= _date
    )
    elevation_history = elevation_history.loc[_mask]
    if device_history is None or len(device_history) == 0:
        _device = elevation_history.iloc[0]
    else:
        _device = device_history.iloc[0]
    combined_data.rename(
        columns={
            "P": "pressure",
            "P0": "station_pressure",
            "TT_TU": "temperature",
            "RF_TU": "humidity",
        },
        inplace=True,
    )
    combined_data.set_index("MESS_DATUM", inplace=True)
    combined_data.drop(columns=["QN_8", "QN_9"], inplace=True)
    # remove rows with invalid/empty data points
    combined_data = combined_data[combined_data["station_pressure"] != -999]
    if bounds_minutes is not None:
        # limit output to the given bounds
        _bounds = dt.timedelta(minutes=bounds_minutes)
        _start = _date - _bounds
        _stop = _date + _bounds
        _mask = (combined_data.index >= _start) & (combined_data.index < _stop)
    else:
        # limit output to data from the same device
        _mask = (combined_data.index >= _device["from"]) & (
            combined_data.index < _device["until"] + pd.Timedelta(days=1)
        )
    combined_data = combined_data.loc[_mask]
    _device = _device.to_dict()
    _device["from"] = _device["from"].strftime("%Y%m%d")  #
    _device["until"] = _device["until"].strftime("%Y%m%d")
    station.update(_device)
    if combined_data.pressure.max() == -999:
        combined_data.pressure = float("NaN")
    else:
        geopotential_elevation = (
            station["elevation"]
            * ba.get_lat_gravity(station["lat"])
            / scipy.constants.g
        )
        combined_data["qfe"] = ba.qfe_from_qff(
            qff=combined_data["pressure"],
            h=geopotential_elevation,
            t_celsius=combined_data["temperature"],
            rh_percent=combined_data["humidity"],
        )
        combined_data.qfe = combined_data.qfe.round(2)
    if as_dataframe:
        data = combined_data
    else:
        data = combined_data.to_dict("records")
    return {
        "station": station,
        "category": category,
        "data": data,
    }


@timeit
def get_nearest_hourly_data(
    date,
    lat: float,
    lon: float,
    as_dataframe: bool = False,
    bounds_minutes: float = None,
):
    """
    Lookup pressure, temperature (and humidity) data from the DWD Open Data
    Server (https://opendata.dwd.de). The data may be available from 1949
    until yesterday depending on the availability of the selected station.
    :param date: The date of interest may be entered as string like
    "20210804T1849" or as seconds since epoch like 1628102940.
    :param lat: Latitude of target location within Germany.
    :param lon: Longitude of target location within Germany.
    :param as_dataframe: Return results as pandas.DataFrame? Defaults to False.
    :param bounds_minutes: Provide range around target date in minutes.
    Defaults to None.
    :return: dict
    """
    hourly_stations = get_hourly_stations(date, lat, lon)
    if len(hourly_stations) == 0:
        logging.warning("no suitable stations found.")
        return None
    station = hourly_stations["stations"][0]
    category = hourly_stations["category"]
    return get_hourly_data(
        station, category, date, as_dataframe, bounds_minutes
    )


@timeit
def get_ten_minutes_stations(date, lat: float = None, lon: float = None):
    stations = []
    catalog = _ten_minutes_catalog.get_catalog(date)
    if catalog is None:
        return []
    response = {"category": catalog["category"]}
    if None in (lat, lon):
        response["stations"] = catalog["stations"]
    else:
        selected_location = eV.LatLon(lat, lon)
        for _station in catalog["stations"]:
            _station_location = eV.LatLon(_station["lat"], _station["lon"])
            _distance = selected_location.distanceTo(_station_location)
            _station["distance"] = round(_distance)
            stations.append(_station)
        sorted_stations = sorted(stations, key=itemgetter("distance"))
        response["stations"] = sorted_stations
    return response


@timeit
def get_ten_minutes_data(
    station: dict,
    category: constr(regex=r"^(historical|recent|now)$"),
    date,
    as_dataframe: bool = False,
    bounds_minutes: float = None,
):
    metadata = unpack_zipped_data_from_url(
        station["metadata_file_name"], "produkt_"
    )
    temperature_data = unpack_zipped_data_from_url(
        station["temperature_file_name"], "produkt_"
    )
    combined_data = temperature_data["data"]
    combined_data.MESS_DATUM = pd.to_datetime(
        combined_data.MESS_DATUM, format="%Y%m%d%H%M"
    )
    combined_data["utc"] = (
        (combined_data.MESS_DATUM - dt.datetime(1970, 1, 1))
        .dt.total_seconds()
        .astype(int)
    )
    if isinstance(date, int):
        _date = dt.datetime.utcfromtimestamp(date)
    else:
        _date = pd.to_datetime(date)
    device_history = metadata.get("device_history")
    if device_history is not None:
        _mask = (device_history["until"] + pd.Timedelta(days=1) > _date) & (
            device_history["from"] <= _date
        )
        device_history = device_history.loc[_mask]
        if len(device_history) > 1:
            # prefer electronic over conventional devices if both exist
            device_history = device_history[
                device_history["device_name"] != "Stationsbarometer"
            ]
    elevation_history = metadata["elevation_history"]
    _mask = (elevation_history["until"] + pd.Timedelta(days=1) > _date) & (
        elevation_history["from"] <= _date
    )
    elevation_history = elevation_history.loc[_mask]
    if device_history is None or len(device_history) == 0:
        _device = elevation_history.iloc[0]
    else:
        _device = device_history.iloc[0]
    combined_data.rename(
        columns={
            "QN": "quality",
            "PP_10": "station_pressure",
            "TT_10": "temperature",
            "RF_10": "humidity",
        },
        inplace=True,
    )
    combined_data.set_index("MESS_DATUM", inplace=True)
    combined_data.drop(columns=["TM5_10", "TD_10"], inplace=True)
    # remove rows with invalid/empty data points
    combined_data = combined_data[combined_data["station_pressure"] != -999]
    if bounds_minutes is not None:
        # limit output to the given bounds
        _bounds = dt.timedelta(minutes=bounds_minutes)
        _start = _date - _bounds
        _stop = _date + _bounds
        _mask = (combined_data.index >= _start) & (combined_data.index < _stop)
    else:
        # limit output to data from the same device
        _mask = (combined_data.index >= _device["from"]) & (
            combined_data.index < _device["until"] + pd.Timedelta(days=1)
        )
    combined_data = combined_data.loc[_mask]
    _device = _device.to_dict()
    _device["from"] = _device["from"].strftime("%Y%m%d")  #
    _device["until"] = _device["until"].strftime("%Y%m%d")
    station.update(_device)
    if as_dataframe:
        data = combined_data
    else:
        data = combined_data.to_dict("records")
    return {
        "station": station,
        "category": category,
        "data": data,
    }


@timeit
def get_nearest_ten_minutes_data(
    date,
    lat: float,
    lon: float,
    as_dataframe: bool = False,
    bounds_minutes: float = None,
):
    """
    Lookup pressure, temperature (and humidity) data from the DWD Open Data
    Server (https://opendata.dwd.de). The data may be available from 1990
    until today depending on the availability of the selected station.
    :param date: The date of interest may be entered as string like
    "20210804T1849" or as seconds since epoch like 1628102940.
    :param lat: Latitude of target location within Germany.
    :param lon: Longitude of target location within Germany.
    :param as_dataframe: Return results as pandas.DataFrame? Defaults to False.
    :param bounds_minutes: Provide range around target date in minutes.
    Defaults to None.
    :return: dict
    """
    ten_minutes_stations = get_ten_minutes_stations(date, lat, lon)
    if len(ten_minutes_stations) == 0:
        logging.warning("no suitable stations found.")
        return None
    station = ten_minutes_stations["stations"][0]
    category = ten_minutes_stations["category"]
    return get_ten_minutes_data(
        station, category, date, as_dataframe, bounds_minutes
    )


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
        bounds_minutes=30,
    )
    print(f"downloaded nearest hourly data for {data['station']}.")
    print(f"target entry: {data['data']}")
    data = get_nearest_hourly_data(
        date=1628102940,
        lat=52.52,
        lon=7.30,
        as_dataframe=False,
        bounds_minutes=30,
    )
    print(f"downloaded nearest hourly data for {data['station']}.")
    print(f"target entry: {data['data']}")
