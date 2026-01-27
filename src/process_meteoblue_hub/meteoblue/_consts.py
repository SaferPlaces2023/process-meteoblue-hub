import numpy as np
import xarray as xr

_DATASET_NAME = 'Meteoblue'

_BASE_URL = 'https://my.meteoblue.com/packages'

_SERVICE_BASIC_5MIN = 'basic-5min'
_SERVICE_BASIC_1H = 'basic-1h'

_API_URL = lambda service: f'{_BASE_URL}/{service}'


class _SERVICES:
    """
    Class to hold the constants for the Meteoblue services.
    """
    BASIC_5MIN = {
        'name': _SERVICE_BASIC_5MIN,
        'api_url': _API_URL(_SERVICE_BASIC_5MIN),
        'response_data_key': 'data_xmin',
        'time_delta_default': 5,
        'time_delta_multiple': 5,
        'max_forecast_days': 7
    }
    BASIC_1H = {
        'name': _SERVICE_BASIC_1H,
        'api_url': _API_URL(_SERVICE_BASIC_1H),
        'response_data_key': 'data_1h',
        'time_delta_default': 60,
        'time_delta_multiple': 60,
        'max_forecast_days': 7
    }


_SERVICES_LIST = [_SERVICE_BASIC_5MIN, _SERVICE_BASIC_1H]
_SERVICES_DICT = {
    _SERVICE_BASIC_5MIN: _SERVICES.BASIC_5MIN,
    _SERVICE_BASIC_1H: _SERVICES.BASIC_1H
}


class _VARIABLES:
    """
    Class to hold the constants for the Meteoblue variables.
    """
    SNOWFRACTION = "SNOWFRACTION"
    WINDSPEED = "WINDSPEED"
    TEMPERATURE = "TEMPERATURE"
    PRECIPITATION_PROBABILITY = "PRECIPITATION_PROBABILITY"
    CONVECTIVE_PRECIPITATION = "CONVECTIVE_PRECIPITATION"
    RAINSPOT = "RAINSPOT"
    PICTOCODE = "PICTOCODE"
    FELTTEMPERATURE = "FELTTEMPERATURE"
    PRECIPITATION = "PRECIPITATION"
    ISDAYLIGHT = "ISDAYLIGHT"
    UVINDEX = "UVINDEX"
    RELATIVEHUMIDITY = "RELATIVEHUMIDITY"
    SEALEVELPRESSURE = "SEALEVELPRESSURE"
    WINDDIRECTION = "WINDDIRECTION"


_VARIABLES_LIST = [attr for attr in dir(_VARIABLES) if not attr.startswith('_')]

_VARIABLE_CODE = lambda variable: variable.lower().replace(' ', '_')
_VARIABLES_DICT = {_VARIABLE_CODE(variable): _VARIABLES.__dict__[variable] for variable in _VARIABLES_LIST}


class _GRID:
    """
    Class to hold grid resolution constants.
    """
    DEFAULT_RESOLUTION = 1000  # meters
    MIN_RESOLUTION = 100  # meters
    RESOLUTION_MULTIPLE = 100  # meters


class _API_PARAMS:
    """
    Class to hold default API parameters.
    """
    FORMAT = 'json'
    SEMAPHORE_LIMIT = 10


