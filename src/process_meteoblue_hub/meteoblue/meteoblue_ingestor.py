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
# Name:        meteoblue_ingestor.py
# Purpose:
#
# Author:      Tommaso Redaelli
#
# Created:     26/01/2026
# -----------------------------------------------------------------------------

import os
import json
import uuid
import traceback
import datetime
import urllib3
import asyncio
import aiohttp

import numpy as np
import pandas as pd
import xarray as xr

from . import _consts
from ..cli.module_log import Logger
from ..utils import filesystem, module_s3
from ..utils.status_exception import StatusException


urllib3.disable_warnings()


class _MeteoblueIngestor():
    """
    Class to ingest data from Meteoblue API.
    """

    name = f'{_consts._DATASET_NAME}__Ingestor'

    _tmp_data_folder = os.path.join(os.getcwd(), name)

    def __init__(self):
        """
        Initialize the Meteoblue Ingestor.
        """
        if not os.path.exists(self._tmp_data_folder):
            os.makedirs(self._tmp_data_folder)


    def _set_tmp_data_folder(self, tmp_data_folder):
        """
        Set the temporary data folder.
        
        Args:
            tmp_data_folder: Path to the temporary data folder
        """
        self._tmp_data_folder = tmp_data_folder


    def get_api_key(self):
        """
        Retrieve the Meteoblue API key from environment variables.
        
        Returns:
            str: API key
        """
        return os.getenv('METEOBLUE_API_KEY')


    def argument_validation(self, **kwargs):
        """
        Validate the arguments passed to the ingestor.
        
        Args:
            **kwargs: Arguments to validate
                - variable: Variable(s) to ingest
                - service: Meteoblue service (basic-5min or basic-1h)
                - location_name: Location identifier (required)
                - lat_range: Latitude range [min, max]
                - long_range: Longitude range [min, max]
                - grid_res: Grid resolution in meters
                - time_delta: Time interval in minutes for output data
                - bucket_destination: S3 bucket destination
                - out_dir: Output directory
                
        Returns:
            dict: Validated arguments
        """
        Logger.debug(f"Validating arguments: {kwargs}")

        variable = kwargs.get('variable', None)
        service = kwargs.get('service') or _consts._SERVICE_BASIC_5MIN
        location_name = kwargs.get('location_name', None)
        lat_range = kwargs.get('lat_range', None)
        long_range = kwargs.get('long_range', None)
        grid_res = kwargs.get('grid_res') or _consts._GRID.DEFAULT_RESOLUTION
        time_delta = kwargs.get('time_delta', None)
        bucket_destination = kwargs.get('bucket_destination', None)
        out_dir = kwargs.get('out_dir', None)

        # Validate variable
        if variable is None:
            variable = [_consts._VARIABLES.PRECIPITATION]
            Logger.debug(f'No variable specified, using default: {variable}')
        if not isinstance(variable, (str, list)):
            raise StatusException(StatusException.INVALID, 'variable must be a string or a list of strings')
        if isinstance(variable, str):
            variable = [variable]
        if not all(isinstance(v, str) for v in variable):
            raise StatusException(StatusException.INVALID, 'All variables must be strings')
        if not all(v in _consts._VARIABLES_DICT.values() or v.lower() in _consts._VARIABLES_DICT.keys() for v in variable):
            raise StatusException(StatusException.INVALID, f'Invalid variable "{variable}". Must be one of {list(_consts._VARIABLES_DICT.values())}')
        
        # Normalize variable names to lowercase keys
        variable = [v.lower() if v in _consts._VARIABLES_DICT.values() else v for v in variable]

        # Validate service
        if not isinstance(service, str):
            raise StatusException(StatusException.INVALID, 'service must be a string')
        if service not in _consts._SERVICES_LIST:
            raise StatusException(StatusException.INVALID, f'Invalid service "{service}". Must be one of {_consts._SERVICES_LIST}')

        if location_name is None:
            raise StatusException(StatusException.INVALID, 'location_name is required')
        if not isinstance(location_name, str):
            raise StatusException(StatusException.INVALID, 'location_name must be a string')

        # Validate lat_range
        if lat_range is not None:
            if not isinstance(lat_range, (list, tuple)) or len(lat_range) != 2:
                raise StatusException(StatusException.INVALID, 'lat_range must be a list or tuple with 2 elements [min, max]')
            if not all(isinstance(v, (int, float)) for v in lat_range):
                raise StatusException(StatusException.INVALID, 'lat_range values must be numbers')
            if not (-90 <= lat_range[0] < lat_range[1] <= 90):
                raise StatusException(StatusException.INVALID, 'lat_range must be in valid EPSG:4326 range [-90, 90] with min < max')

        # Validate long_range
        if long_range is not None:
            if not isinstance(long_range, (list, tuple)) or len(long_range) != 2:
                raise StatusException(StatusException.INVALID, 'long_range must be a list or tuple with 2 elements [min, max]')
            if not all(isinstance(v, (int, float)) for v in long_range):
                raise StatusException(StatusException.INVALID, 'long_range values must be numbers')
            if not (-180 <= long_range[0] < long_range[1] <= 180):
                raise StatusException(StatusException.INVALID, 'long_range must be in valid EPSG:4326 range [-180, 180] with min < max')

        # Validate grid_res
        if not isinstance(grid_res, int):
            raise StatusException(StatusException.INVALID, 'grid_res must be an integer')
        if grid_res < _consts._GRID.MIN_RESOLUTION:
            raise StatusException(StatusException.INVALID, f'grid_res must be >= {_consts._GRID.MIN_RESOLUTION} meters')
        if grid_res % _consts._GRID.RESOLUTION_MULTIPLE != 0:
            raise StatusException(StatusException.INVALID, f'grid_res must be a multiple of {_consts._GRID.RESOLUTION_MULTIPLE} meters')

        # Validate time_delta
        service_config = _consts._SERVICES_DICT[service]
        service_time_delta_default = service_config['time_delta_default']
        
        if time_delta is None:
            time_delta = service_time_delta_default
            Logger.debug(f'No time_delta specified, using service default: {time_delta} minutes')
        else:
            if not isinstance(time_delta, int):
                raise StatusException(StatusException.INVALID, 'time_delta must be an integer')
            if time_delta < service_time_delta_default:
                raise StatusException(StatusException.INVALID, f'time_delta must be >= {service_time_delta_default} minutes for service {service}')
            if time_delta % service_time_delta_default != 0:
                raise StatusException(StatusException.INVALID, f'time_delta must be a multiple of {service_time_delta_default} minutes for service {service}')
        # Validate bucket_destination
        if bucket_destination is not None:
            if not isinstance(bucket_destination, str):
                raise StatusException(StatusException.INVALID, 'bucket_destination must be a string')
            if not bucket_destination.startswith('s3://'):
                raise StatusException(StatusException.INVALID, 'bucket_destination must start with "s3://"')

        # Validate out_dir
        if out_dir is not None:
            if not isinstance(out_dir, str):
                raise StatusException(StatusException.INVALID, 'out_dir must be a string')
            os.makedirs(out_dir, exist_ok=True)
        else:
            out_dir = self._tmp_data_folder
            os.makedirs(out_dir, exist_ok=True)

        return {
            'variable': variable,
            'service': service,
            'location_name': location_name,
            'lat_range': lat_range,
            'long_range': long_range,
            'grid_res': grid_res,
            'time_delta': time_delta,
            'bucket_destination': bucket_destination,
            'out_dir': out_dir
        }


    def generate_grid_points(self, bbox, grid_res):
        """
        Generate grid points for the given bounding box and resolution.
        
        Args:
            bbox: Bounding box [lon_min, lat_min, lon_max, lat_max]
            grid_res: Grid resolution in meters
            
        Returns:
            list: List of (lon, lat) coordinate tuples
        """
        lon_min, lat_min, lon_max, lat_max = bbox
        
        # Calculate number of points based on grid resolution
        # 1e-5 is approximate conversion factor from meters to degrees at equator
        num_lon = int((lon_max - lon_min) // (grid_res * 1e-5))
        num_lat = int((lat_max - lat_min) // (grid_res * 1e-5))
        
        # Generate coordinate lists
        lon_list = np.round(np.linspace(lon_min, lon_max, num_lon), 3)
        lat_list = np.round(np.linspace(lat_min, lat_max, num_lat), 3)
        
        # Create coordinate pairs (lon, lat)
        coords = [(lon, lat) for lat in lat_list for lon in lon_list]
        
        Logger.debug(f'Generated {len(coords)} grid points ({num_lon}x{num_lat})')
        
        return coords


    def prepare_api_requests(self, grid_coords, service):
        """
        Prepare API request parameters for all grid points.
        
        Args:
            grid_coords: List of (lon, lat) coordinates
            service: Meteoblue service name
            
        Returns:
            list: List of request parameter dictionaries
        """
        base_params = {
            'format': _consts._API_PARAMS.FORMAT,
            'apikey': self.get_api_key(),
        }
        
        requests_params = [
            base_params | {'lon': lon, 'lat': lat}
            for (lon, lat) in grid_coords
        ]
        
        Logger.debug(f'Prepared {len(requests_params)} API requests')
        
        return requests_params


    async def fetch_and_process(self, session, base_url, params, data_key, variable):
        """
        Fetch data from Meteoblue API and process into DataArray.
        
        Args:
            session: aiohttp ClientSession
            base_url: Base API URL
            params: Request parameters
            data_key: Key for response data
            variable: Variable name
            
        Returns:
            xr.DataArray: Processed data array
        """
        semaphore = asyncio.Semaphore(_consts._API_PARAMS.SEMAPHORE_LIMIT)
        async with semaphore:
            try:
                async with session.get(base_url, params=params) as response:
                    status_code = response.status
                    if status_code == 200:
                        out = await response.json()
                        # Extract variable data from response
                        variable_data = out[data_key][variable]
                        time_data = out[data_key]['time']
                        
                        da = xr.DataArray(
                            data=[[variable_data]],
                            dims=["lat", "lon", "time"],
                            coords=dict(
                                lat=[params['lat']],
                                lon=[params['lon']],
                                time=[datetime.datetime.fromisoformat(dt) for dt in time_data]
                            )
                        )
                        return da
                    else:
                        error_msg = await response.text()
                        Logger.error(f"API request failed with status {status_code}: {error_msg}")
                        return None
            except Exception as ex:
                Logger.error(f"Error fetching data for lat={params['lat']}, lon={params['lon']}: {ex}")
                return None


    async def run_requests(self, base_url, requests_params, data_key, variable):
        """
        Run asynchronous requests to Meteoblue API.
        
        Args:
            base_url: Base API URL
            requests_params: List of request parameters
            data_key: Key for response data
            variable: Variable name
            
        Returns:
            list: List of DataArrays
        """
        responses = []
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.fetch_and_process(session, base_url, params, data_key, variable)
                for params in requests_params
            ]
            responses.extend(await asyncio.gather(*tasks))
        
        # Filter out None values from failed requests
        responses = [r for r in responses if r is not None]
        Logger.debug(f'Successfully retrieved {len(responses)} responses')
        
        return responses


    def download_meteoblue_data(self, service, variable, grid_coords):
        """
        Download data from Meteoblue API for given grid coordinates.
        
        Args:
            service: Meteoblue service name
            variable: Variable name
            grid_coords: List of grid coordinates
            
        Returns:
            xr.Dataset: Downloaded dataset
        """
        Logger.info(f'Downloading {variable} data from Meteoblue API for {len(grid_coords)} grid points')
        
        # Get service configuration
        service_config = _consts._SERVICES_DICT[service]
        api_url = service_config['api_url']
        data_key = service_config['response_data_key']
        
        # Prepare API requests
        requests_params = self.prepare_api_requests(grid_coords, service)
        
        # Execute async requests
        coverages = asyncio.run(
            self.run_requests(api_url, requests_params, data_key, variable)
        )
        
        if not coverages:
            raise StatusException(
                StatusException.ERROR,
                'No data retrieved from Meteoblue API'
            )
        
        # Combine DataArrays by coordinates into Dataset
        dataset = xr.combine_by_coords(coverages).to_dataset(name=variable)
        
        Logger.info(f'Successfully downloaded dataset with shape: {dataset[variable].shape}')
        
        return dataset


    def process_variable_data(self, dataset, variable):
        """
        Process variable data (e.g., compute cumulative sum).
        
        Args:
            dataset: xarray Dataset
            variable: Variable name
            
        Returns:
            xr.Dataset: Processed dataset
        """        
        # Convert to float32 to reduce file size
        for var in dataset.data_vars:
            dataset[var] = dataset[var].astype(np.float32)
        
        Logger.debug(f'Processed variable data for: {variable}')
        
        return dataset


    def get_single_date_dataset(self, dataset):
        """
        Split dataset into individual date datasets.
        
        Args:
            dataset: xarray Dataset with time dimension
            
        Returns:
            list: List of (date, dataset) tuples
        """
        dates = sorted(list(set(dataset.time.dt.date.values)))
        date_datasets = []
        
        for date in dates:
            subset = dataset.sel(time=dataset.time.dt.date == date)
            date_datasets.append((date, subset))
        
        Logger.debug(f'Split dataset into {len(date_datasets)} date-based datasets')
        
        return date_datasets


    def get_dataset_name(self, location_name, variable, date):
        """
        Generate dataset filename for a specific date.
        
        Args:
            location_name: Location identifier
            variable: Variable name
            date: Date object
            
        Returns:
            str: Dataset filename
        """
        dataset_name = f"{_consts._DATASET_NAME}__{location_name}__{variable}__{date.isoformat()}.nc"
        Logger.debug(f'Generated dataset name: {dataset_name}')
        return dataset_name


    def save_date_datasets(self, date_datasets, location_name, variable, out_dir, bucket_destination):
        """
        Save date datasets to NetCDF files and optionally upload to S3.
        
        Args:
            date_datasets: List of (date, dataset) tuples
            location_name: Location identifier
            variable: Variable name
            out_dir: Output directory
            bucket_destination: S3 bucket destination (optional)
            
        Returns:
            list: List of dataset reference dictionaries
        """
        date_dataset_refs = []
        
        for dt, ds in date_datasets:
            fn = self.get_dataset_name(location_name, variable, dt)
            fp = os.path.join(out_dir, fn)
            
            # Save to NetCDF
            self.save_to_netcdf(ds, fp)
            
            date_dataset_ref = {
                'variable': variable,
                'date': dt,
                'ref': {'filepath': fp}
            }
            
            # Upload to S3 if destination provided
            if bucket_destination:
                uri = os.path.join(bucket_destination, fn)
                self.upload_to_s3(fp, uri)
                date_dataset_ref['ref']['uri'] = uri
            
            date_dataset_refs.append(date_dataset_ref)
        
        Logger.info(f'Saved {len(date_dataset_refs)} date datasets for variable: {variable}')
        
        return date_dataset_refs


    def save_to_netcdf(self, dataset, filepath):
        """
        Save dataset to NetCDF file.
        
        Args:
            dataset: xarray Dataset
            filepath: Output file path
        """
        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            # Save to NetCDF with netcdf4 engine
            dataset.to_netcdf(filepath, engine='netcdf4')
            
            Logger.info(f'Saved dataset to: {filepath}')
            
        except Exception as ex:
            Logger.error(f'Error saving dataset to {filepath}: {ex}')
            raise StatusException(
                StatusException.ERROR,
                f'Failed to save dataset to NetCDF: {ex}'
            )


    def upload_to_s3(self, local_path, s3_uri):
        """
        Upload file to S3 bucket.
        
        Args:
            local_path: Local file path
            s3_uri: S3 destination URI
        """
        try:
            module_s3.s3_upload(local_path, s3_uri, remove_src=False)
            Logger.info(f'Uploaded to S3: {s3_uri}')
        except Exception as ex:
            Logger.error(f'Error uploading to S3: {ex}')
            raise StatusException(
                StatusException.ERROR,
                f'Failed to upload to S3: {ex}'
            )


    def run(
        self,
        variable: str = None,
        service: str = None,
        location_name: str = None,
        lat_range: list = None,
        long_range: list = None,
        grid_res: int = None,
        time_delta: int = None,
        out_dir: str = None,
        bucket_destination: str = None,
        **kwargs
    ):
        """
        Main ingest method to collect data from Meteoblue API.
        
        Args:
            variable: Variable(s) to ingest
            service: Meteoblue service (basic-5min or basic-1h)
            location_name: Location identifier (required)
            lat_range: Latitude range [min, max]
            long_range: Longitude range [min, max]
            grid_res: Grid resolution in meters
            time_delta: Time interval in minutes for output data
            out_dir: Output directory
            bucket_destination: S3 bucket destination
            **kwargs: Additional parameters (debug, etc.)
            
        Returns:
            dict: Ingest results with status and collected data info
        """
        debug = kwargs.get('debug', False)

        try:
            # Validate arguments
            validated_args = self.argument_validation(
                variable=variable,
                service=service,
                location_name=location_name,
                lat_range=lat_range,
                long_range=long_range,
                grid_res=grid_res,
                time_delta=time_delta,
                out_dir=out_dir,
                bucket_destination=bucket_destination
            )
            
            variable = validated_args['variable']
            service = validated_args['service']
            location_name = validated_args['location_name']
            lat_range = validated_args['lat_range']
            long_range = validated_args['long_range']
            grid_res = validated_args['grid_res']
            time_delta = validated_args['time_delta']
            out_dir = validated_args['out_dir']
            bucket_destination = validated_args['bucket_destination']

            # Generate grid points
            if lat_range is None or long_range is None:
                raise StatusException(
                    StatusException.INVALID,
                    'lat_range and long_range are required for Meteoblue ingestor'
                )
            
            bbox = [long_range[0], lat_range[0], long_range[1], lat_range[1]]
            grid_coords = self.generate_grid_points(bbox, grid_res)

            # Download and process each variable
            variables_date_datasets_refs = []
            for var in variable:
                Logger.info(f'Processing variable: {var}')
                
                # Download data from Meteoblue API
                dataset = self.download_meteoblue_data(service, var, grid_coords)
                
                # Process variable data (cumsum, float32 conversion)
                dataset = self.process_variable_data(dataset, var)
                
                # Resample if time_delta is different from service default
                service_config = _consts._SERVICES_DICT[service]
                if time_delta != service_config['time_delta_default']:
                    Logger.info(f'Resampling data to {time_delta} minute intervals')
                    dataset = dataset.resample(time=f'{time_delta}min', skipna=True).sum()
                
                # Split dataset into individual date datasets
                date_datasets = self.get_single_date_dataset(dataset)
                
                # Save date datasets to output directory and upload to S3
                variable_date_datasets_refs = self.save_date_datasets(
                    date_datasets, location_name, var, out_dir, bucket_destination
                )
                
                # Collect all variables+date datasets references
                variables_date_datasets_refs.extend(variable_date_datasets_refs)

            # Prepare output
            outputs = {
                'status': 'OK',
                'collected_data_info': [
                    {
                        'variable': vddr['variable'],
                        'date': vddr['date'].isoformat(),
                        'ref': vddr['ref']['uri'] if bucket_destination else vddr['ref']['filepath']
                    }
                    for vddr in variables_date_datasets_refs
                ]
            }

            Logger.info(f'Successfully ingested {len(variables_date_datasets_refs)} datasets')
            
            return outputs

        except StatusException as e:
            Logger.error(f'StatusException during Meteoblue ingestor run: {e}')
            raise
        except Exception as e:
            error_msg = f'Error during Meteoblue ingestor run: {traceback.format_exc() if debug else e}'
            Logger.error(error_msg)
            raise StatusException(StatusException.ERROR, error_msg)
        
        finally:
            # Cleanup temporary data folder
            try:
                filesystem.garbage_folders(self._tmp_data_folder)
                Logger.debug(f'Cleaned up temporary data folder: {self._tmp_data_folder}')
            except Exception as ex:
                Logger.warning(f'Error cleaning up temporary folder: {ex}')


    def __repr__(self):
        return f'<MeteoblueIngestor> {self.name}'
