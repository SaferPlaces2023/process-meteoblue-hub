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
# Name:        meteoblue_retriever.py
# Purpose:
#
# Author:      Tommaso Redaelli
#
# Created:     27/01/2026
# -----------------------------------------------------------------------------

import os
import json
import uuid
import datetime
import traceback
import urllib3

import numpy as np
import pandas as pd
import xarray as xr
import rioxarray

from . import _consts
from .meteoblue_ingestor import _MeteoblueIngestor
from ..cli.module_log import Logger
from ..utils import filesystem, module_s3
from ..utils.status_exception import StatusException


urllib3.disable_warnings()


class _MeteoblueRetriever():
    """
    Class to retrieve data from Meteoblue NetCDF files and create GeoTIFF outputs.
    """

    name = f'{_consts._DATASET_NAME}__Retriever'
    
    _tmp_data_folder = os.path.join(os.getcwd(), name)

    def __init__(self):
        """
        Initialize the Meteoblue Retriever.
        """
        if not os.path.exists(self._tmp_data_folder):
            os.makedirs(self._tmp_data_folder)


    def _set_tmp_data_folder(self, tmp_data_folder):
        """
        Set the temporary data folder.
        
        Args:
            tmp_data_folder: Path to the temporary data folder
        """
        if not os.path.exists(tmp_data_folder):
            os.makedirs(tmp_data_folder, exist_ok=True)
        self._tmp_data_folder = tmp_data_folder
        Logger.debug(f'Set temporary data folder to: {self._tmp_data_folder}')


    def argument_validation(self, **kwargs):
        """
        Validate the arguments passed to the retriever.
        
        Args:
            **kwargs: Arguments to validate
                - variable: Variable(s) to retrieve
                - location_name: Location identifier (required)
                - lat_range: Latitude range [min, max]
                - long_range: Longitude range [min, max]
                - time_range: Time range [start, end]
                - out_format: Output format (default: 'tif')
                - bucket_source: S3 bucket source
                - bucket_destination: S3 bucket destination
                - out: Output file path
                
        Returns:
            dict: Validated arguments
        """
        Logger.debug(f"Validating arguments: {kwargs}")

        variable = kwargs.get('variable', None)
        location_name = kwargs.get('location_name', None)
        lat_range = kwargs.get('lat_range', None)
        long_range = kwargs.get('long_range', None)
        time_range = kwargs.get('time_range', None)
        time_start = time_range[0] if isinstance(time_range, (list, tuple)) else time_range
        time_end = time_range[1] if isinstance(time_range, (list, tuple)) and len(time_range) > 1 else None
        out_format = kwargs.get('out_format', None)
        bucket_source = kwargs.get('bucket_source', None)
        bucket_destination = kwargs.get('bucket_destination', None)
        out = kwargs.get('out', None)

        # Validate variable
        if variable is None:
            variable = list(_consts._VARIABLES_DICT.keys())
            Logger.debug(f'No variable specified, collect all variables: {variable}')
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

        # Validate location_name
        if location_name is None:
            raise StatusException(StatusException.INVALID, 'location_name is required')
        if not isinstance(location_name, str):
            raise StatusException(StatusException.INVALID, 'location_name must be a string')
        if not location_name.strip():
            raise StatusException(StatusException.INVALID, 'location_name cannot be empty')
        # Sanitize location_name
        location_name = location_name.strip().replace(' ', '_').replace('/', '_').replace('\\', '_')

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

        # Validate time_range
        if time_start is None:
            raise StatusException(StatusException.INVALID, 'time_range is required')
        if not isinstance(time_start, str):
            raise StatusException(StatusException.INVALID, 'time_start must be a string')
        try:
            time_start = datetime.datetime.fromisoformat(time_start)
        except ValueError:
            raise StatusException(StatusException.INVALID, 'time_start must be a valid datetime ISO format string')
        
        if time_end is not None:
            if not isinstance(time_end, str):
                raise StatusException(StatusException.INVALID, 'time_end must be a string')
            try:
                time_end = datetime.datetime.fromisoformat(time_end)
            except ValueError:
                raise StatusException(StatusException.INVALID, 'time_end must be a valid datetime ISO format string')
            if time_start > time_end:
                raise StatusException(StatusException.INVALID, 'time_start must be less than or equal to time_end')
        else:
            time_end = time_start + datetime.timedelta(days=1)
            Logger.debug(f'No time_end specified, using time_start + 1 day: {time_end.isoformat()}')

        # Validate out_format
        if out_format is not None:
            if not isinstance(out_format, str):
                raise StatusException(StatusException.INVALID, 'out_format must be a string')
            if out_format not in ['tif']:
                raise StatusException(StatusException.INVALID, 'out_format must be "tif"')
        else:
            out_format = 'tif'

        # Validate bucket_source
        if bucket_source is not None:
            if not isinstance(bucket_source, str):
                raise StatusException(StatusException.INVALID, 'bucket_source must be a string')
            if not bucket_source.startswith('s3://'):
                raise StatusException(StatusException.INVALID, 'bucket_source must start with "s3://"')

        # Validate bucket_destination
        if bucket_destination is not None:
            if not isinstance(bucket_destination, str):
                raise StatusException(StatusException.INVALID, 'bucket_destination must be a string')
            if not bucket_destination.startswith('s3://'):
                raise StatusException(StatusException.INVALID, 'bucket_destination must start with "s3://"')
        
        # If bucket_source is not provided, use bucket_destination
        if bucket_source is None:
            bucket_source = bucket_destination

        # Validate out
        if out is not None:
            if not isinstance(out, str):
                raise StatusException(StatusException.INVALID, 'out must be a string')
            if not out.endswith('.tif'):
                raise StatusException(StatusException.INVALID, 'out must end with ".tif"')
            dirname = os.path.dirname(out)
            if dirname and not os.path.exists(dirname):
                os.makedirs(dirname, exist_ok=True)

        return {
            'variable': variable,
            'location_name': location_name,
            'lat_range': lat_range,
            'long_range': long_range,
            'time_start': time_start,
            'time_end': time_end,
            'out_format': out_format,
            'bucket_source': bucket_source,
            'bucket_destination': bucket_destination,
            'out': out
        }


    def check_date_dataset_availability(self, location_name, variable, requested_dates, bucket_source):
        """
        Check if date datasets are available in the source bucket.
        
        Args:
            location_name: Location identifier
            variable: Variable name
            requested_dates: List of requested dates
            bucket_source: S3 bucket source
            
        Returns:
            list: List of available URIs or None if not all are available
        """
        # Build expected URIs for requested dates
        requested_source_uris = [
            f'{bucket_source}/{_consts._DATASET_NAME}__{location_name}__{variable}__{d}.nc'
            for d in requested_dates
        ]
        
        # List available files in bucket with matching prefix
        bucket_source_filekeys = module_s3.s3_list(
            bucket_source,
            filename_prefix=f'{_consts._DATASET_NAME}__{location_name}__{variable}__'
        )
        bucket_source_uris = [
            f'{bucket_source}/{filesystem.justfname(f)}'
            for f in bucket_source_filekeys
        ]
        
        # Check if all requested URIs are available
        available_uris = [ru for ru in requested_source_uris if ru in bucket_source_uris]
        
        if len(available_uris) != len(requested_dates):
            Logger.debug(f'Not all datasets available: {len(available_uris)}/{len(requested_dates)}')
            return None
        
        Logger.debug(f'All {len(available_uris)} datasets are available')
        return available_uris


    def retrieve_meteoblue_data(self, location_name, variable, lat_range, long_range, time_start, time_end, bucket_source):
        """
        Retrieve Meteoblue data from NetCDF files.
        
        Args:
            location_name: Location identifier
            variable: Variable(s) to retrieve
            lat_range: Latitude range [min, max]
            long_range: Longitude range [min, max]
            time_start: Start time
            time_end: End time
            bucket_source: S3 bucket source
            
        Returns:
            dict: Dictionary with variable names as keys and datasets as values
        """
        # Generate list of requested dates
        requested_dates = pd.date_range(time_start, time_end, freq='1d').to_series().apply(
            lambda d: d.date().isoformat()
        ).to_list()
        
        Logger.info(f'Retrieving data for {len(requested_dates)} dates: {requested_dates[0]} to {requested_dates[-1]}')
        
        variable_datasets = dict()
        
        for var in variable:
            Logger.debug(f'Processing variable: {var}')
            
            # Check if datasets are available in bucket
            data_source_uris = self.check_date_dataset_availability(
                location_name, var, requested_dates, bucket_source
            ) if bucket_source is not None else None
            
            # If not available in bucket, we need the datasets locally or fail
            if data_source_uris is None:
                meteoblue_ingestor = _MeteoblueIngestor()
                meteoblue_ingestor_out = meteoblue_ingestor.run(
                    variable = variable,
                    location_name = location_name,
                    lat_range = lat_range,
                    long_range = long_range,
                    out_dir = self._tmp_data_folder,
                    bucket_destination = bucket_source
                )
                if meteoblue_ingestor_out.get('status', 'ERROR') != 'OK':
                    raise StatusException(StatusException.ERROR, f'Error during ICON2I ingestor run: {meteoblue_ingestor_out["message"]}')    
                data_source_uris = [cdi['ref'] for cdi in meteoblue_ingestor_out['collected_data_info'] if cdi['variable'] == var]
            
            # Download files from S3 if needed
            retrieved_files = []
            for dsu in data_source_uris:
                if dsu.startswith('s3://'):
                    rf = os.path.join(self._tmp_data_folder, os.path.basename(dsu))
                    module_s3.s3_download(dsu, rf)
                    retrieved_files.append(rf)
                    Logger.debug(f'Downloaded: {os.path.basename(rf)}')
                else:
                    retrieved_files.append(dsu)
            
            # Load and concatenate datasets
            datasets = [xr.open_dataset(rf) for rf in retrieved_files]
            dataset = xr.concat(datasets, dim='time')
            
            # Round coordinates for consistent querying
            dataset = dataset.assign_coords(
                lat=np.round(dataset.lat.values, 6),
                lon=np.round(dataset.lon.values, 6),
            )
            dataset = dataset.sortby(['time', 'lat', 'lon'])
            
            # Filter by spatial and temporal ranges
            dataset = self.dataset_query(dataset, lat_range, long_range, [time_start, time_end])
            
            variable_datasets[var] = dataset
            Logger.info(f'Retrieved dataset for {var} with shape: {dataset[var].shape}')
        
        return variable_datasets


    def dataset_query(self, dataset, lat_range, long_range, time_range):
        """
        Filter dataset by spatial and temporal ranges.
        
        Args:
            dataset: xarray Dataset
            lat_range: Latitude range or single value
            long_range: Longitude range or single value
            time_range: Time range or single value
            
        Returns:
            xr.Dataset: Filtered dataset
        """
        query_dataset = dataset.copy()
        
        # Filter by latitude
        if isinstance(lat_range, (list, tuple)) and len(lat_range) == 2:
            query_dataset = query_dataset.sel(lat=slice(lat_range[0], lat_range[1]))
        elif isinstance(lat_range, (float, int)):
            query_dataset = query_dataset.sel(lat=lat_range, method="nearest")
        
        # Filter by longitude
        if isinstance(long_range, (list, tuple)) and len(long_range) == 2:
            query_dataset = query_dataset.sel(lon=slice(long_range[0], long_range[1]))
        elif isinstance(long_range, (float, int)):
            query_dataset = query_dataset.sel(lon=long_range, method="nearest")
        
        # Filter by time
        if isinstance(time_range, (list, tuple)) and len(time_range) == 2:
            query_dataset = query_dataset.sel(time=slice(time_range[0], time_range[1]))
        elif isinstance(time_range, (str, datetime.datetime)):
            query_dataset = query_dataset.sel(time=time_range, method="nearest")
        
        Logger.debug(f'Dataset filtered to shape: {query_dataset.dims}')
        
        return query_dataset


    def create_timestamp_raster(self, location_name, variable, dataset, out):
        """
        Create a multi-band GeoTIFF raster with temporal bands.
        
        Args:
            location_name: Location identifier
            variable: Variable name
            dataset: xarray Dataset
            out: Output file path (optional)
            
        Returns:
            str: Path to created raster file
        """
        # Extract timestamps and format them in UTC0 without timezone
        timestamps = []
        for ts in dataset.time.values:
            dt = pd.to_datetime(ts)
            # Convert to UTC if timezone-aware
            if dt.tz is not None:
                dt = dt.tz_convert('UTC')
            # Remove timezone info and format as ISO string
            dt_naive = dt.replace(tzinfo=None)
            timestamps.append(dt_naive.isoformat(timespec='seconds'))
        
        Logger.debug(f'Creating raster with {len(timestamps)} temporal bands')
        
        # Generate output filename if not provided
        if out is None:
            multiband_raster_filename = f'{_consts._DATASET_NAME}__{location_name}__{variable}__{timestamps[0]}.tif'
            multiband_raster_filepath = os.path.join(self._tmp_data_folder, multiband_raster_filename)
        else:
            multiband_raster_filepath = out
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(multiband_raster_filepath) if os.path.dirname(multiband_raster_filepath) else '.', exist_ok=True)
        
        # Extract data array for the variable
        data_array = dataset[variable]
        
        # Sort latitude in descending order (GeoTIFF convention: north to south)
        data_array = data_array.sortby('lat', ascending=False)
        
        # Transpose to (time, lat, lon) if needed
        if data_array.dims != ('time', 'lat', 'lon'):
            data_array = data_array.transpose('time', 'lat', 'lon')
        
        # Rename lat/lon to y/x for rioxarray compatibility
        data_array = data_array.rename({'lat': 'y', 'lon': 'x'})
        
        # Set CRS
        data_array = data_array.rio.write_crs('EPSG:4326')
        
        # Set nodata value
        data_array = data_array.rio.write_nodata(-9999.0)
        
        # Replace NaN with nodata value
        data_array = data_array.fillna(-9999.0)
        
        # Save as Cloud-Optimized GeoTIFF
        data_array.rio.to_raster(
            multiband_raster_filepath,
            driver='COG',
            compress='LZW',
            predictor=2,
            tags={'band_names': timestamps}
        )
        
        Logger.info(f'Created timestamp raster: {multiband_raster_filepath}')
        Logger.debug(f'Raster dimensions: {data_array.shape} (time, y, x)')
        
        return multiband_raster_filepath


    def run(
        self,
        variable: str = None,
        location_name: str = None,
        lat_range: list = None,
        long_range: list = None,
        time_range: list = None,
        out_format: str = None,
        bucket_source: str = None,
        bucket_destination: str = None,
        out: str = None,
        **kwargs
    ):
        """
        Main retrieval method to extract data from Meteoblue NetCDF files.
        
        Args:
            variable: Variable(s) to retrieve
            location_name: Location identifier (required)
            lat_range: Latitude range [min, max]
            long_range: Longitude range [min, max]
            time_range: Time range [start, end]
            out_format: Output format (default: 'tif')
            bucket_source: S3 bucket source
            bucket_destination: S3 bucket destination
            out: Output file path
            **kwargs: Additional parameters (debug, etc.)
            
        Returns:
            dict: Retrieval results with status and collected data info
        """
        debug = kwargs.get('debug', False)

        try:
            # Validate arguments
            validated_args = self.argument_validation(
                variable=variable,
                location_name=location_name,
                lat_range=lat_range,
                long_range=long_range,
                time_range=time_range,
                out_format=out_format,
                bucket_source=bucket_source,
                bucket_destination=bucket_destination,
                out=out
            )
            
            variable = validated_args['variable']
            location_name = validated_args['location_name']
            lat_range = validated_args['lat_range']
            long_range = validated_args['long_range']
            time_start = validated_args['time_start']
            time_end = validated_args['time_end']
            out_format = validated_args['out_format']
            bucket_source = validated_args['bucket_source']
            bucket_destination = validated_args['bucket_destination']
            out = validated_args['out']

            # Retrieve Meteoblue data from NetCDF files
            variable_datasets = self.retrieve_meteoblue_data(
                location_name=location_name,
                variable=variable,
                lat_range=lat_range,
                long_range=long_range,
                time_start=time_start,
                time_end=time_end,
                bucket_source=bucket_source
            )

            # Create timestamp rasters for each variable
            variables_timestamp_rasters_refs = dict()
            
            for var, dataset in variable_datasets.items():
                Logger.debug(f'Creating timestamp raster for variable: {var}')
                
                # Create timestamp raster
                timestamp_raster = self.create_timestamp_raster(
                    location_name=location_name,
                    variable=var,
                    dataset=dataset,
                    out=out
                )
                
                variables_timestamp_rasters_refs[var] = timestamp_raster
                
                # Upload to S3 if bucket_destination is provided
                if bucket_destination is not None:
                    bucket_uri = f"{bucket_destination}/{filesystem.justfname(timestamp_raster)}"
                    upload_status = module_s3.s3_upload(timestamp_raster, bucket_uri, remove_src=False)
                    if not upload_status:
                        raise StatusException(
                            StatusException.ERROR,
                            f"Failed to upload data to bucket {bucket_destination}"
                        )
                    Logger.info(f"Uploaded to S3: {bucket_uri}")
                    variables_timestamp_rasters_refs[var] = bucket_uri

            # Prepare outputs
            if bucket_destination is not None or out is not None:
                outputs = {
                    'status': 'OK',
                    'collected_data_info': [
                        {
                            'variable': var,
                            'ref': ref,
                        }
                        for var, ref in variables_timestamp_rasters_refs.items()
                    ]
                }
            else:
                # If no output destination, return the raster path
                outputs = timestamp_raster if len(variables_timestamp_rasters_refs) == 1 else variables_timestamp_rasters_refs
            
            Logger.info(f'Successfully retrieved {len(variable)} variable(s)')
            
            return outputs

        except StatusException as e:
            Logger.error(f'StatusException during Meteoblue retriever run: {e}')
            raise
        except Exception as e:
            error_msg = f'Error during Meteoblue retriever run: {traceback.format_exc() if debug else str(e)}'
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
        return f'<MeteoblueRetriever> {self.name}'
