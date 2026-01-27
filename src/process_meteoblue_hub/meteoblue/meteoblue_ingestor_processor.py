# =================================================================
#
# Authors: Tommaso Redaelli <tommaso.redaelli@gecosistema.com>
#
# Copyright (c) 2026 Gecosistema S.r.l.
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
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
# =================================================================

import os
import uuid

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from ..cli.module_log import Logger, set_log_debug
from ..utils import filesystem
from ..utils.status_exception import StatusException

from .meteoblue_ingestor import _MeteoblueIngestor

# -----------------------------------------------------------------------------


#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'meteoblue-ingestor-process',
    'title': {
        'en': 'Meteoblue Ingestor Process',
    },
    'description': {
        'en': 'Collect forecast data from Meteoblue API'
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['meteoblue', 'weather', 'forecast', 'ingestor'],
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
            'description': 'The variable(s) to retrieve. Can be a single variable or a list of variables.',
            'schema': {
                'type': 'string or array',
                'enum': [
                    'precipitation',
                    'snowfraction',
                    'windspeed',
                    'temperature',
                    'precipitation_probability',
                    'convective_precipitation',
                    'rainspot',
                    'pictocode',
                    'felttemperature',
                    'isdaylight',
                    'uvindex',
                    'relativehumidity',
                    'sealevelpressure',
                    'winddirection'
                ]
            }
        },
        'service': {
            'title': 'Meteoblue service',
            'description': 'The Meteoblue forecast service to use. Default is "basic-5min".',
            'schema': {
                'type': 'string',
                'enum': ['basic-5min', 'basic-1h']
            }
        },
        'lat_range': {
            'title': 'Latitude range',
            'description': 'The latitude range in format [lat_min, lat_max]. Values must be in EPSG:4326 CRS.',
            'schema': {
                'type': 'array',
                'items': {'type': 'number'},
                'minItems': 2,
                'maxItems': 2
            }
        },
        'long_range': {
            'title': 'Longitude range',
            'description': 'The longitude range in format [long_min, long_max]. Values must be in EPSG:4326 CRS.',
            'schema': {
                'type': 'array',
                'items': {'type': 'number'},
                'minItems': 2,
                'maxItems': 2
            }
        },
        'grid_res': {
            'title': 'Grid resolution',
            'description': 'The grid resolution in meters. Must be a multiple of 100. Default is 1000 meters.',
            'schema': {
                'type': 'integer',
                'minimum': 100
            }
        },
        'time_delta': {
            'title': 'Time delta',
            'description': 'The time interval in minutes for output data. Must be a multiple of the service time delta (5 for basic-5min, 60 for basic-1h).',
            'schema': {
                'type': 'integer',
                'minimum': 5
            }
        },
        'out_dir': {
            'title': 'Output directory',
            'description': 'The output directory where the data will be stored. If not provided, data will be stored in a temporary directory.',
            'schema': {
                'type': 'string'
            }
        },
        'bucket_destination': {
            'title': 'S3 bucket destination',
            'description': 'The S3 bucket destination where the data will be uploaded (e.g., s3://bucket-name/path).',
            'schema': {
                'type': 'string'
            }
        },
        'debug': {
            'title': 'Debug mode',
            'description': 'Enable debug mode for detailed logging.',
            'schema': {
                'type': 'boolean'
            }
        }
    },
    'outputs': {
        'status': {
            'title': 'Status',
            'description': 'Status of the process execution [OK, PARTIAL, SKIPPED, DENIED, INVALID, ERROR]',
            'schema': {
                'type': 'string',
                'enum': ['OK', 'PARTIAL', 'SKIPPED', 'DENIED', 'INVALID', 'ERROR']
            }
        },
        'collected_data_info': {
            'title': 'Collected data information',
            'description': 'Reference to the collected data. Each entry contains the variable, date and file reference.',
            'type': 'array',
            'schema': {
                'type': 'object',
                'properties': {
                    'variable': {
                        'type': 'string'
                    },
                    'date': {
                        'type': 'string'
                    },
                    'ref': {
                        'type': 'string'
                    }
                }
            }
        },
        'message': {
            'title': 'Message',
            'description': 'Additional information or error message',
            'schema': {
                'type': 'string'
            }
        }
    },
    'example': {
        "inputs": {
            "token": "ABC123XYZ666",
            "debug": True,
            "variable": "precipitation",
            "service": "basic-5min",
            "lat_range": [44.0, 44.5],
            "long_range": [12.2, 12.8],
            "grid_res": 1000,
            "time_delta": 5,
            "bucket_destination": "s3://my-bucket/meteoblue-data"
        }
    }
}

# -----------------------------------------------------------------------------


class MeteoblueIngestorProcessor(BaseProcessor):
    """
    Meteoblue Ingestor Processor.
    """
    name = 'MeteoblueIngestorProcessor'

    def __init__(self, processor_def):
        """
        Initialize the Meteoblue Ingestor Process.
        """
        super().__init__(processor_def, PROCESS_METADATA)


    def argument_validation(self, data):
        """
        Validate the arguments passed to the processor.
        """
        token = data.get('token', None)
        debug = data.get('debug', False)

        if token is None or token != os.getenv("INT_API_TOKEN", "token"):
            raise StatusException(StatusException.DENIED, 'ACCESS DENIED: wrong token')
            
        if not isinstance(debug, bool):
            raise StatusException(StatusException.INVALID, 'debug must be a boolean')
        
        if debug:
            set_log_debug()


    def execute(self, data):
        """
        Execute the Meteoblue ingestor process.
        
        Args:
            data: Input data dictionary
            
        Returns:
            tuple: (mimetype, outputs)
        """
        mimetype = 'application/json'
        outputs = {}

        # Create unique temporary folder for this execution
        meteoblue_ingestor = _MeteoblueIngestor()
        meteoblue_ingestor._set_tmp_data_folder(
            os.path.join(meteoblue_ingestor._tmp_data_folder, str(uuid.uuid4()))
        )

        try:
            # Validate process parameters
            self.argument_validation(data)
            Logger.debug('Validated process parameters')

            # Run the Meteoblue ingestor
            outputs = meteoblue_ingestor.run(**data)
            
        except StatusException as err:
            outputs = {
                'status': err.status,
                'message': str(err.message)
            }
        except Exception as err:
            outputs = {
                'status': StatusException.ERROR,
                'error': str(err)
            }
            raise ProcessorExecuteError(str(err))
        
        finally:
            # Cleanup temporary folder
            filesystem.rmdir(meteoblue_ingestor._tmp_data_folder)
            Logger.debug(f'Removed temporary data folder: {meteoblue_ingestor._tmp_data_folder}')
        
        return mimetype, outputs


    def __repr__(self):
        return f'<MeteoblueIngestorProcessor> {self.name}'
