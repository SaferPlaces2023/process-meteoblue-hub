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
# Name:        meteoblue_retriever_processor.py
# Purpose:
#
# Author:      Tommaso Redaelli
#
# Created:     27/01/2026
# -----------------------------------------------------------------------------

import os
import json
import uuid

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from ..cli.module_log import Logger, set_log_debug
from ..utils import filesystem
from ..utils.status_exception import StatusException

from .meteoblue_retriever import _MeteoblueRetriever

# -----------------------------------------------------------------------------

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'meteoblue-retriever-process',
    'title': {
        'en': 'Meteoblue Retriever Process',
    },
    'description': {
        'en': 'Retrieve data from Meteoblue NetCDF files and create GeoTIFF outputs'
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['meteoblue', 'retriever', 'geotiff'],
    'inputs': {
        'token': {
            'title': 'Secret token',
            'description': 'Authentication token',
            'schema': {
                'type': 'string'
            }
        },
        'variable': {
            'title': 'Variable',
            'description': 'The variable to retrieve. Possible values: "precipitation".',
            'schema': {
                'type': 'string',
                'enum': ['precipitation']
            }
        },
        'location_name': {
            'title': 'Location name',
            'description': 'Location identifier for the data (required).',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1
        },
        'lat_range': {
            'title': 'Latitude range',
            'description': 'The latitude range in format [lat_min, lat_max]. Values must be in EPSG:4326. If not provided, all latitudes will be returned.',
            'schema': {
                'type': 'array'
            }
        },
        'long_range': {
            'title': 'Longitude range',
            'description': 'The longitude range in format [long_min, long_max]. Values must be in EPSG:4326. If not provided, all longitudes will be returned.',
            'schema': {
                'type': 'array'
            }
        },
        'time_range': {
            'title': 'Time range',
            'description': 'The time range in format [time_start, time_end]. Both must be in ISO format. If not provided, all times will be returned.',
            'schema': {
                'type': 'array'
            }
        },
        'out_format': {
            'title': 'Output format',
            'description': 'Output format (default: tif).',
            'schema': {
                'type': 'string',
                'enum': ['tif'],
                'default': 'tif'
            }
        },
        'out': {
            'title': 'Output file path',
            'description': 'The output file path. If not provided, the data will be stored in a temporary directory.',
            'schema': {
                'type': 'string'
            }
        },
        'bucket_source': {
            'title': 'Bucket source',
            'description': 'S3 bucket source URI where NetCDF files are stored.',
            'schema': {
                'type': 'string'
            }
        },
        'bucket_destination': {
            'title': 'Bucket destination',
            'description': 'S3 bucket destination URI where output will be stored.',
            'schema': {
                'type': 'string'
            }
        },
        'debug': {
            'title': 'Debug',
            'description': 'Enable debug mode',
            'schema': {
                'type': 'boolean',
                'default': False
            }
        },
    },
    'outputs': {
        'status': {
            'title': 'Status',
            'description': 'Status of the process execution [OK or ERROR]',
            'schema': {
                'type': 'string'
            }
        },
        'collected_data_info': {
            'title': 'Collected data info',
            'description': 'Information about retrieved data files',
            'schema': {
                'type': 'array'
            }
        },
        'message': {
            'title': 'Message',
            'description': 'Error message if status is ERROR',
            'schema': {
                'type': 'string'
            }
        }
    },
    'example': {
        "inputs": {
            "debug": True,
            "variable": "precipitation",
            "location_name": "Piemonte",
            "lat_range": [45.0, 45.1],
            "long_range": [7.0, 7.1],
            "time_range": ["2026-01-27T00:00:00", "2026-01-28T00:00:00"],
            "bucket_source": "s3://saferplaces.co/packages/process-meteoblue-hub/ingestor/",
            "bucket_destination": "s3://saferplaces.co/packages/process-meteoblue-hub/retriever/",
            "token": "123ABC456XYZ",
        }
    }
}

# -----------------------------------------------------------------------------

class MeteoblueRetrieverProcessor(BaseProcessor):
    """
    Meteoblue Retriever Processor for PyGeoAPI.
    """

    name = 'MeteoblueRetrieverProcessor'

    def __init__(self, processor_def):
        """
        Initialize the Meteoblue Retriever Processor.
        """
        super().__init__(processor_def, PROCESS_METADATA)


    def argument_validation(self, data):
        """
        Validate the arguments passed to the processor.
        
        Args:
            data: Input data dictionary
        """
        token = data.get('token', None)
        debug = data.get('debug', False)

        if token is None or token != os.getenv("INT_API_TOKEN", "token"):
            raise StatusException(StatusException.DENIED, 'ACCESS DENIED: wrong token')
            
        if not isinstance(debug, bool):
            raise StatusException(StatusException.INVALID, 'debug must be a boolean')
        
        if debug:
            set_log_debug()

        # Validate location_name is provided
        if 'location_name' not in data or data['location_name'] is None:
            raise StatusException(StatusException.INVALID, 'location_name is required')

    
    def execute(self, data):
        """
        Execute the Meteoblue retriever process.
        
        Args:
            data: Input data dictionary
            
        Returns:
            tuple: (mimetype, outputs)
        """
        mimetype = 'application/json'
        outputs = {}

        # Create retriever with unique temporary folder for async execution
        MeteoblueRetriever = _MeteoblueRetriever()
        MeteoblueRetriever._set_tmp_data_folder(
            os.path.join(MeteoblueRetriever._tmp_data_folder, str(uuid.uuid4()))
        )

        try:
            # Validate process parameters
            self.argument_validation(data)
            Logger.debug(f'Validated process parameters')

            # Execute retriever
            outputs = MeteoblueRetriever.run(**data)
            
        except StatusException as err:
            outputs = {
                'status': err.status,
                'message': str(err)
            }
        except Exception as err:
            outputs = {
                'status': StatusException.ERROR,
                'message': str(err)
            }
            raise ProcessorExecuteError(str(err))
        
        finally:
            # Cleanup temporary folder
            filesystem.rmdir(MeteoblueRetriever._tmp_data_folder)
            Logger.debug(f'Removed temporary data folder: {MeteoblueRetriever._tmp_data_folder}')
        
        return mimetype, outputs


    def __repr__(self):
        return f'<MeteoblueRetrieverProcessor> {self.name}'
