import numpy as np
import scipy.constants


M = 0.0289644  # molar mass of air in kg/mol
a = 0.0065  # K/m
R = scipy.constants.R  # molar gas constant
C_h = 0.12
R_L = R / M
g = scipy.constants.g


# based on eq 3a from https://library.wmo.int/doc_num.php?explnum_id=7450
def calculate_saturation_vapour_pressure(t_celsius: float) -> float:
    e_w = 6.112 * np.exp(17.62 * t_celsius / (243.12 + t_celsius))
    return e_w  # in units of hPa


# based on eq 7.3 from https://www.dwd.de/DE/leistungen/pbfb_verlag_leitfaeden/
# pdf_einzelbaende/leitfaden6_pdf.pdf?__blob=publicationFile&v=3
def qff_from_qfe(
    qfe: float, h: float, t_celsius: float, rh_percent: float
) -> float:
    T = 273.15 + t_celsius
    e_w = calculate_saturation_vapour_pressure(t_celsius)
    # Ignoring the pressure dependency of the vapour pressure for simplicity.
    e = rh_percent / 100 * e_w
    qff = qfe * np.exp(h * g / R_L / (T + e * C_h + a * h / 2))
    return qff  # in units of hPa


def qnh_from_qfe(qfe: float, h: float) -> float:
    return qff_from_qfe(qfe, h, t_celsius=15, rh_percent=0)


def qfe_from_qff(
    qff: float, h: float, t_celsius: float, rh_percent: float
) -> float:
    T = 273.15 + t_celsius
    e_w = calculate_saturation_vapour_pressure(t_celsius)
    # Ignoring the pressure dependency of the vapour pressure for simplicity.
    e = rh_percent / 100 * e_w
    qfe = qff / np.exp(h * g / R_L / (T + e * C_h + a * h / 2))
    return qfe  # in units of hPa


# calculate barometric altitude based on the following formula:
# https://www.weather.gov/media/epz/wxcalc/pressureAltitude.pdf
def calculate_pressure_altitude(pressure: float, p0: float = 101_325) -> float:
    altitude = 0.3048 * 145_366.45 * (1 - pow(pressure / p0, 0.190_284))
    return altitude


# https://en.wikipedia.org/wiki/Theoretical_gravity
def get_lat_gravity(latitude: float) -> float:
    sin_lat_sq = np.sin(latitude * np.pi / 180.0) ** 2
    g = (
        9.7803253359
        * (1 + 0.00193185265241 * sin_lat_sq)
        / np.sqrt(1 - 0.00669437999013 * sin_lat_sq)
    )
    return g


# based on https://www.amsys-sensor.com/downloads/notes/ms5611-precise-
# altitude-measurement-with-a-pressure-sensor-module-amsys-509e.pdf and
# http://dx.doi.org/10.1109/WPNC.2010.5650745
def get_barometric_altitude(
    p: float,
    p0: float = 101325.0,
    h0: float = 0.0,
    T0: float = 288.15,
    latitude: float = None,
) -> float:
    if latitude is not None:
        _g = get_lat_gravity(latitude)
    else:
        _g = g
    altitude = h0 + T0 / a * (1 - (p / p0) ** (R_L * a / _g))
    return altitude


def get_altitude_pressure(
    altitude: float,
    p0: float = 101325.0,
    T0: float = 288.15,
    h0: float = 0.0,
    latitude: float = None,
) -> float:
    if latitude is not None:
        _g = get_lat_gravity(latitude)
    else:
        _g = g
    dh = altitude - h0
    p = p0 * (1 - a * dh / T0) ** (_g / (R_L * a))
    return p
