# -----------------------------------------------------------------------------
# License:
# Copyright (c) 2025 Gecosistema S.r.l.
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
#
# Name:        main.py
# Purpose:
#
# Author:      Tommaso Redaelli
#
# Created:     27/01/2026
# -----------------------------------------------------------------------------
import click
import pprint
import traceback
import json

from .cli.module_log import Logger
from .utils.status_exception import StatusException
from .utils.module_prologo import prologo, epilogo

from .meteoblue import _MeteoblueIngestor, _MeteoblueRetriever


# REGION: [ METEOBLUE INGESTOR ] =====================================================================================

class _ARG_NAMES_METEOBLUE_INGESTOR():
    VARIABLE = {
        'aliases': ['--variable', '--var'],
        'help': "Variable(s) to ingest. Comma-separated list. Currently supported: 'precipitation'.",
        'default': None,
        'example': '--variable precipitation',
    }
    SERVICE = {
        'aliases': ['--service', '--svc'],
        'help': "Meteoblue service to use: 'basic-5min' or 'basic-1h'.",
        'default': None,
        'example': '--service basic-5min',
    }
    LOCATION_NAME = {
        'aliases': ['--location_name', '--location', '--loc'],
        'help': "Location identifier for the ingested data (required).",
        'default': None,
        'example': '--location_name Milan',
    }
    LAT_RANGE = {
        'aliases': ['--lat_range', '--lat'],
        'help': "Latitude range as [min,max]. Example: --lat_range 45.0,46.0",
        'default': None,
        'example': '--lat_range 45.0,46.0',
    }
    LONG_RANGE = {
        'aliases': ['--long_range', '--lon'],
        'help': "Longitude range as [min,max]. Example: --long_range 7.0,8.0",
        'default': None,
        'example': '--long_range 7.0,8.0',
    }
    GRID_RES = {
        'aliases': ['--grid_res', '--res'],
        'help': "Grid resolution in meters (minimum 100, multiple of 100).",
        'default': 1000,
        'example': '--grid_res 1000',
    }
    TIME_DELTA = {
        'aliases': ['--time_delta', '--td'],
        'help': "Time interval in minutes for output data (must match service resolution).",
        'default': None,
        'example': '--time_delta 60',
    }
    OUT_DIR = {
        'aliases': ['--out_dir', '--output_dir', '--od'],
        'help': "Output directory for the ingested data. If not provided, the output will be returned as a dictionary.",
        'default': None,
        'example': '--out_dir /path/to/output',
    }
    BUCKET_DESTINATION = {
        'aliases': ['--bucket_destination', '--bucket', '--s3'],
        'help': "Destination bucket for the output data.",
        'default': None,
        'example': '--bucket_destination s3://my-bucket/path/to/prefix',
    }

@click.command()
@click.option(
    *_ARG_NAMES_METEOBLUE_INGESTOR.VARIABLE['aliases'],
    type=str,
    default=_ARG_NAMES_METEOBLUE_INGESTOR.VARIABLE['default'],
    help=_ARG_NAMES_METEOBLUE_INGESTOR.VARIABLE['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_INGESTOR.SERVICE['aliases'],
    type=click.Choice(['basic-5min', 'basic-1h'], case_sensitive=True),
    default=_ARG_NAMES_METEOBLUE_INGESTOR.SERVICE['default'],
    help=_ARG_NAMES_METEOBLUE_INGESTOR.SERVICE['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_INGESTOR.LOCATION_NAME['aliases'],
    type=str,
    required=True,
    help=_ARG_NAMES_METEOBLUE_INGESTOR.LOCATION_NAME['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_INGESTOR.LAT_RANGE['aliases'],
    type=str,
    default=_ARG_NAMES_METEOBLUE_INGESTOR.LAT_RANGE['default'],
    help=_ARG_NAMES_METEOBLUE_INGESTOR.LAT_RANGE['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_INGESTOR.LONG_RANGE['aliases'],
    type=str,
    default=_ARG_NAMES_METEOBLUE_INGESTOR.LONG_RANGE['default'],
    help=_ARG_NAMES_METEOBLUE_INGESTOR.LONG_RANGE['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_INGESTOR.GRID_RES['aliases'],
    type=int,
    default=_ARG_NAMES_METEOBLUE_INGESTOR.GRID_RES['default'],
    help=_ARG_NAMES_METEOBLUE_INGESTOR.GRID_RES['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_INGESTOR.TIME_DELTA['aliases'],
    type=int,
    default=_ARG_NAMES_METEOBLUE_INGESTOR.TIME_DELTA['default'],
    help=_ARG_NAMES_METEOBLUE_INGESTOR.TIME_DELTA['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_INGESTOR.OUT_DIR['aliases'],
    type=str,
    default=_ARG_NAMES_METEOBLUE_INGESTOR.OUT_DIR['default'],
    help=_ARG_NAMES_METEOBLUE_INGESTOR.OUT_DIR['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_INGESTOR.BUCKET_DESTINATION['aliases'],
    type=str,
    default=_ARG_NAMES_METEOBLUE_INGESTOR.BUCKET_DESTINATION['default'],
    help=_ARG_NAMES_METEOBLUE_INGESTOR.BUCKET_DESTINATION['help'],
)
# -----------------------------------------------------------------------------
# Common options to all Gecosistema CLI applications
# -----------------------------------------------------------------------------
@click.option(
    '--backend', 
    type=click.STRING, required=False, default=None,
    help="The backend to use for sending back progress status updates to the backend server."
)
@click.option(
    '--jid',
    type=click.STRING, required=False, default=None,
    help="The job ID to use for sending back progress status updates to the backend server. If not provided, it will be generated automatically."
)
@click.option(
    '--version',
    is_flag=True, required=False, default=False,
    help="Show the version of the package."
)
@click.option(
    '--debug',
    is_flag=True, required=False, default=False,
    help="Debug mode."
)
@click.option(
    '--verbose',
    is_flag=True, required=False, default=False,
    help="Print some words more about what is doing."
)
def cli_run_meteoblue_ingestor(**kwargs):
    """
    CLI entry point for Meteoblue Ingestor
    """
    output = run_meteoblue_ingestor(**kwargs)
    
    Logger.debug(pprint.pformat(output))
    
    return output

def run_meteoblue_ingestor(
    # --- Specific options ---
    variable = None,
    service = None,
    location_name = None,
    lat_range = None,
    long_range = None,
    grid_res = 1000,
    time_delta = None,
    out_dir = None,
    bucket_destination = None,
    # --- Common options ---
    backend = None,
    jid = None,
    version = False,
    debug = False,
    verbose = False
):
    """
    Main function for Meteoblue Ingestor
    """

    try:
        # DOC: -- Init logger + cli settings + handle version and debug -------
        t0, jid = prologo(backend, jid, version, verbose, debug)

        # DOC: -- Parse lat_range and long_range from string to list ----------
        if lat_range and isinstance(lat_range, str):
            lat_range = [float(x.strip()) for x in lat_range.split(',')]
        if long_range and isinstance(long_range, str):
            long_range = [float(x.strip()) for x in long_range.split(',')]

        # DOC: -- Run the Meteoblue ingestor process --------------------------
        MeteoblueIngestor = _MeteoblueIngestor()
        results = MeteoblueIngestor.run(
            variable=variable,
            service=service,
            location_name=location_name,
            lat_range=lat_range,
            long_range=long_range,
            grid_res=grid_res,
            time_delta=time_delta,
            out_dir=out_dir,
            bucket_destination=bucket_destination,
            debug=debug
        )

        # DOC: -- Close the process with epilogo ------------------------------
        epilogo(t0, backend, jid)

        return results

    except StatusException as e:
        Logger.error(f'StatusException: {e}')
        results = {
            'status': e.status,
            'message': str(e),
            ** ({'traceback': traceback.format_exc()} if debug else {})
        }
        epilogo(t0, backend, jid)
        return results

    except Exception as e:
        error_msg = f'Error: {traceback.format_exc() if debug else str(e)}'
        Logger.error(error_msg)
        results = {
            'status': StatusException.ERROR,
            'message': error_msg,
            ** ({'traceback': traceback.format_exc()} if debug else {})
        }
        epilogo(t0, backend, jid)
        return results

# ENDREGION

# REGION: [ METEOBLUE RETRIEVER ] ====================================================================================

class _ARG_NAMES_METEOBLUE_RETRIEVER():
    VARIABLE = {
        'aliases': ['--variable', '--var'],
        'help': "Variable to retrieve. Currently supported: 'precipitation'.",
        'default': None,
        'example': '--variable precipitation',
    }
    LOCATION_NAME = {
        'aliases': ['--location_name', '--location', '--loc'],
        'help': "Location identifier for the data (required).",
        'default': None,
        'example': '--location_name Milan',
    }
    LAT_RANGE = {
        'aliases': ['--lat_range', '--lat'],
        'help': "Latitude range as [min,max]. Example: --lat_range 45.0,46.0",
        'default': None,
        'example': '--lat_range 45.0,46.0',
    }
    LONG_RANGE = {
        'aliases': ['--long_range', '--lon'],
        'help': "Longitude range as [min,max]. Example: --long_range 7.0,8.0",
        'default': None,
        'example': '--long_range 7.0,8.0',
    }
    TIME_RANGE = {
        'aliases': ['--time_range', '--time'],
        'help': "Time range as [start,end] in ISO format. Example: --time_range 2026-01-27T00:00:00,2026-01-28T00:00:00",
        'default': None,
        'example': '--time_range 2026-01-27T00:00:00,2026-01-28T00:00:00',
    }
    OUT_FORMAT = {
        'aliases': ['--out_format', '--format'],
        'help': "Output format (default: tif).",
        'default': 'tif',
        'example': '--out_format tif',
    }
    OUT = {
        'aliases': ['--out', '--output'],
        'help': "Output file path. If not provided, the output will be stored in a temporary directory.",
        'default': None,
        'example': '--out /path/to/output.tif',
    }
    BUCKET_SOURCE = {
        'aliases': ['--bucket_source', '--source', '--s3_source'],
        'help': "Source bucket URI where NetCDF files are stored.",
        'default': None,
        'example': '--bucket_source s3://my-bucket/ingestor/',
    }
    BUCKET_DESTINATION = {
        'aliases': ['--bucket_destination', '--bucket', '--s3'],
        'help': "Destination bucket URI where output will be stored.",
        'default': None,
        'example': '--bucket_destination s3://my-bucket/retriever/',
    }

@click.command()
@click.option(
    *_ARG_NAMES_METEOBLUE_RETRIEVER.VARIABLE['aliases'],
    type=str,
    default=_ARG_NAMES_METEOBLUE_RETRIEVER.VARIABLE['default'],
    help=_ARG_NAMES_METEOBLUE_RETRIEVER.VARIABLE['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_RETRIEVER.LOCATION_NAME['aliases'],
    type=str,
    required=True,
    help=_ARG_NAMES_METEOBLUE_RETRIEVER.LOCATION_NAME['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_RETRIEVER.LAT_RANGE['aliases'],
    type=str,
    default=_ARG_NAMES_METEOBLUE_RETRIEVER.LAT_RANGE['default'],
    help=_ARG_NAMES_METEOBLUE_RETRIEVER.LAT_RANGE['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_RETRIEVER.LONG_RANGE['aliases'],
    type=str,
    default=_ARG_NAMES_METEOBLUE_RETRIEVER.LONG_RANGE['default'],
    help=_ARG_NAMES_METEOBLUE_RETRIEVER.LONG_RANGE['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_RETRIEVER.TIME_RANGE['aliases'],
    type=str,
    default=_ARG_NAMES_METEOBLUE_RETRIEVER.TIME_RANGE['default'],
    help=_ARG_NAMES_METEOBLUE_RETRIEVER.TIME_RANGE['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_RETRIEVER.OUT_FORMAT['aliases'],
    type=click.Choice(['tif'], case_sensitive=True),
    default=_ARG_NAMES_METEOBLUE_RETRIEVER.OUT_FORMAT['default'],
    help=_ARG_NAMES_METEOBLUE_RETRIEVER.OUT_FORMAT['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_RETRIEVER.OUT['aliases'],
    type=str,
    default=_ARG_NAMES_METEOBLUE_RETRIEVER.OUT['default'],
    help=_ARG_NAMES_METEOBLUE_RETRIEVER.OUT['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_RETRIEVER.BUCKET_SOURCE['aliases'],
    type=str,
    default=_ARG_NAMES_METEOBLUE_RETRIEVER.BUCKET_SOURCE['default'],
    help=_ARG_NAMES_METEOBLUE_RETRIEVER.BUCKET_SOURCE['help'],
)
@click.option(
    *_ARG_NAMES_METEOBLUE_RETRIEVER.BUCKET_DESTINATION['aliases'],
    type=str,
    default=_ARG_NAMES_METEOBLUE_RETRIEVER.BUCKET_DESTINATION['default'],
    help=_ARG_NAMES_METEOBLUE_RETRIEVER.BUCKET_DESTINATION['help'],
)
# -----------------------------------------------------------------------------
# Common options to all Gecosistema CLI applications
# -----------------------------------------------------------------------------
@click.option(
    '--backend', 
    type=click.STRING, required=False, default=None,
    help="The backend to use for sending back progress status updates to the backend server."
)
@click.option(
    '--jid',
    type=click.STRING, required=False, default=None,
    help="The job ID to use for sending back progress status updates to the backend server. If not provided, it will be generated automatically."
)
@click.option(
    '--version',
    is_flag=True, required=False, default=False,
    help="Show the version of the package."
)
@click.option(
    '--debug',
    is_flag=True, required=False, default=False,
    help="Debug mode."
)
@click.option(
    '--verbose',
    is_flag=True, required=False, default=False,
    help="Print some words more about what is doing."
)
def cli_run_meteoblue_retriever(**kwargs):
    """
    CLI entry point for Meteoblue Retriever
    """
    output = run_meteoblue_retriever(**kwargs)
    
    Logger.debug(pprint.pformat(output))
    
    return output

def run_meteoblue_retriever(
    # --- Specific options ---
    variable = None,
    location_name = None,
    lat_range = None,
    long_range = None,
    time_range = None,
    out_format = 'tif',
    out = None,
    bucket_source = None,
    bucket_destination = None,
    # --- Common options ---
    backend = None,
    jid = None,
    version = False,
    debug = False,
    verbose = False
):
    """
    Main function for Meteoblue Retriever
    """

    try:
        # DOC: -- Init logger + cli settings + handle version and debug -------
        t0, jid = prologo(backend, jid, version, verbose, debug)

        # DOC: -- Parse lat_range, long_range, and time_range from string to list
        if lat_range and isinstance(lat_range, str):
            lat_range = [float(x.strip()) for x in lat_range.split(',')]
        if long_range and isinstance(long_range, str):
            long_range = [float(x.strip()) for x in long_range.split(',')]
        if time_range and isinstance(time_range, str):
            time_range = [x.strip() for x in time_range.split(',')]

        # DOC: -- Run the Meteoblue retriever process -------------------------
        MeteoblueRetriever = _MeteoblueRetriever()
        results = MeteoblueRetriever.run(
            variable=variable,
            location_name=location_name,
            lat_range=lat_range,
            long_range=long_range,
            time_range=time_range,
            out_format=out_format,
            out=out,
            bucket_source=bucket_source,
            bucket_destination=bucket_destination,
            debug=debug
        )

        # DOC: -- Close the process with epilogo ------------------------------
        epilogo(t0, backend, jid)

        return results

    except StatusException as e:
        Logger.error(f'StatusException: {e}')
        results = {
            'status': e.status,
            'message': str(e),
            ** ({'traceback': traceback.format_exc()} if debug else {})
        }
        epilogo(t0, backend, jid)
        return results

    except Exception as e:
        error_msg = f'Error: {traceback.format_exc() if debug else str(e)}'
        Logger.error(error_msg)
        results = {
            'status': StatusException.ERROR,
            'message': error_msg,
            ** ({'traceback': traceback.format_exc()} if debug else {})
        }
        epilogo(t0, backend, jid)
        return results

# ENDREGION
