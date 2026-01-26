# Implementation Logic - process-meteoblue-hub

<!-- TODO(agent): Comprehensive implementation roadmap based on process-icon2i-hub architecture -->

## 1. Project Structure and Configuration

### 1.1 Package Setup
- [ ] Create `pyproject.toml` with build system configuration
  - Define `[build-system]` with setuptools backend
  - Set package metadata: name, version, description, authors, license
  - Configure `[project.scripts]` for CLI entry points:
    - `meteoblue-ingestor` → main.py function
    - `meteoblue-retriever` → main.py function
  - Define dependencies: `python-dotenv`, `requests`, `filelock`, `click`, `boto3`, `xarray`, `rioxarray`, `netcdf4`, `geopandas`
  - Add optional dependencies group `[project.optional-dependencies]` for PyGeoAPI integration
  - Configure `[tool.setuptools]` package discovery in `src/` directory

### 1.2 Core Directory Structure
- [ ] Create package structure under `src/process_meteoblue_hub/`
  ```
  src/process_meteoblue_hub/
  ├── __init__.py              # Package exports and optional PyGeoAPI imports
  ├── main.py                  # CLI entry points with Click decorators
  ├── meteoblue/               # Core logic module
  │   ├── __init__.py
  │   ├── _consts.py          # API URLs, variables, data processing functions
  │   ├── meteoblue_ingestor.py
  │   ├── meteoblue_retriever.py
  │   ├── meteoblue_ingestor_processor.py   # PyGeoAPI processor (optional)
  │   └── meteoblue_retriever_processor.py  # PyGeoAPI processor (optional)
  ├── cli/                     # CLI utilities
  │   ├── __init__.py
  │   ├── module_log.py       # Logger setup
  │   ├── module_logo.py      # ASCII logo display
  │   └── module_version.py   # Version management
  └── utils/                   # Shared utilities
      ├── __init__.py
      ├── filesystem.py        # File operations
      ├── module_prologo.py    # Execution prolog/epilog
      ├── module_s3.py         # S3 operations (upload/download)
      ├── module_status.py     # Status tracking
      ├── status_exception.py  # Custom exception class
      └── strings.py           # String parsing utilities
  ```

---

## 2. Constants and Configuration (`meteoblue/_consts.py`)

### 2.1 API Configuration
- [ ] Define `_DATASET_NAME` constant (e.g., `'METEOBLUE_WEATHER_DATA'`)
- [ ] Define `_BASE_URL` for MeteoBlue API endpoint
- [ ] Define `_AVAILABLE_DATA_URL` endpoint for listing available forecast runs
- [ ] Define `_RETRIEVE_DATA_URL` lambda function to construct data retrieval URLs

### 2.2 Variable Definitions
- [ ] Create `_VARIABLES` class with MeteoBlue variable constants
  - Map MeteoBlue API variable names (e.g., temperature, precipitation, wind speed, etc.)
  - Use descriptive uppercase constants (e.g., `TEMPERATURE_2M`, `TOTAL_PRECIPITATION`)
- [ ] Generate `_VARIABLES_LIST` from class attributes (exclude private members)
- [ ] Create `_VARIABLE_CODE` lambda to convert variable names to snake_case codes
- [ ] Build `_VARIABLES_DICT` mapping codes to variable names

### 2.3 Data Processing Configuration
- [ ] Define `_DATA_CUBE_PROCESSING` dictionary for variable-specific transformations
  - Example: cumulative precipitation to incremental values via `np.diff()`
- [ ] Create `_DERIVED_VARIABLES` class for computed variables (e.g., wind speed from U/V components)
- [ ] Build `_DERIVED_VARIABLES_DICT` mapping
- [ ] Implement computation functions (e.g., `compute_wind_speed`, `compute_wind_direction`)
- [ ] Create `_DERIVED_VARIABLES_COMPUTE` dictionary mapping codes to functions

---

## 3. Core Logic - Ingestor (`meteoblue/meteoblue_ingestor.py`)

### 3.1 Class Structure
- [ ] Create `_MeteoBlueIngestor` class with:
  - `name` class attribute
  - `_tmp_data_folder` attribute for temporary file storage
  - `__init__()` method to create temp directory

### 3.2 Forecast Run Management
- [ ] Implement `get_available_forecast_runs()` method
  - Request available data from MeteoBlue API
  - Parse response into pandas DataFrame
  - Convert forecast run strings to datetime objects
  - Return structured DataFrame with forecast run metadata

- [ ] Implement `ping_available_runs(forecast_datetime_runs)` method
  - Check if requested forecast runs are available
  - Return boolean validation result

### 3.3 Argument Validation
- [ ] Implement `argument_validation(**kwargs)` method with validation for:
  - **variable**: string or list of variable codes, validate against `_VARIABLES_DICT` and `_DERIVED_VARIABLES_DICT`
  - **forecast_run**: ISO format datetime string(s), validate format and availability
  - **bucket_destination**: S3 URI format validation (`s3://...`)
  - **out_dir**: output directory path, create if not exists
  - Return validated and normalized parameters dict

### 3.4 Data Download
- [ ] Implement `get_meteoblue_data_filenames(forecast_datetime_runs)` method
  - Query available data for specific forecast runs
  - Return list of data filenames to download

- [ ] Implement `download_meteoblue_data(forecast_datetime_runs)` method
  - Iterate through filenames
  - Stream download data files to `_tmp_data_folder`
  - Handle HTTP errors and retries
  - Return list of local file paths

### 3.5 Data Processing
- [ ] Implement `process_grib_to_netcdf(grib_files, variables)` method
  - Read GRIB files using `pygrib` or MeteoBlue-specific format
  - Extract specified variables
  - Convert to xarray Dataset
  - Apply `_DATA_CUBE_PROCESSING` transformations if defined
  - Save as NetCDF format

- [ ] Implement `compute_derived_variables(variable_files)` method
  - Load NetCDF files for base variables
  - Apply computation functions from `_DERIVED_VARIABLES_COMPUTE`
  - Return derived variable datasets

### 3.6 Output Management
- [ ] Implement `save_outputs(datasets, out_dir)` method
  - Write NetCDF files to local directory
  - Return file paths

- [ ] Implement `upload_to_s3(local_paths, bucket_destination)` method
  - Use `module_s3` utilities
  - Upload files to S3 with proper key structure
  - Return S3 URIs

### 3.7 Main Execution
- [ ] Implement `execute(**kwargs)` method orchestrating:
  1. Argument validation
  2. Forecast run discovery
  3. Data download
  4. GRIB to NetCDF conversion
  5. Derived variable computation
  6. Local save and/or S3 upload
  7. Return result dictionary with status and file paths

---

## 4. Core Logic - Retriever (`meteoblue/meteoblue_retriever.py`)

### 4.1 Class Structure
- [ ] Create `_MeteoBlueRetriever` class inheriting from or importing `_MeteoBlueIngestor`
- [ ] Add `_set_tmp_data_folder(tmp_data_folder)` method for custom temp directory

### 4.2 Argument Validation
- [ ] Implement `argument_validation(**kwargs)` with additional parameters:
  - **variable**: validate variable codes
  - **lat_range**: `[lat_min, lat_max]` validation (range -90 to 90, proper ordering)
  - **long_range**: `[lon_min, lon_max]` validation (range -180 to 180, proper ordering)
  - **time_range**: `[time_start, time_end]` validation
    - Parse ISO format strings to datetime
    - Ensure time_start < time_end
    - Validate time window constraints (e.g., last 48 hours for real-time data)
  - **out_format**: validate output format (e.g., `'tif'`, `'netcdf'`)
  - **bucket_source**: S3 URI for retrieving preprocessed data
  - **bucket_destination**: S3 URI for storing output
  - **out**: output file path validation

### 4.3 Data Source Management
- [ ] Implement logic to choose data source:
  - If `bucket_source` provided: retrieve preprocessed NetCDF from S3
  - Else: download raw data via `_MeteoBlueIngestor.download_meteoblue_data()`

### 4.4 Spatial and Temporal Subsetting
- [ ] Implement `subset_data(dataset, lat_range, long_range, time_range)` method
  - Use xarray `.sel()` or `.isel()` for slicing
  - Handle coordinate system conversions if needed
  - Return subsetted xarray Dataset

### 4.5 Format Conversion
- [ ] Implement `convert_to_geotiff(dataset, variable, out_path)` method
  - Extract variable data array
  - Set spatial reference (EPSG:4326 or appropriate CRS)
  - Use `rioxarray` to write GeoTIFF with proper geotransform
  - Return output file path

- [ ] Implement additional format converters as needed (NetCDF, JSON, etc.)

### 4.6 Output Management
- [ ] Implement `save_output(data, out_path, out_format)` method
  - Route to appropriate converter based on `out_format`
  - Handle multi-band or time-series outputs

- [ ] Implement `upload_output_to_s3(local_path, bucket_destination)` method
  - Upload converted file to S3
  - Return S3 URI

### 4.7 Main Execution
- [ ] Implement `execute(**kwargs)` method orchestrating:
  1. Argument validation
  2. Data source selection (S3 vs API)
  3. Data loading or download
  4. Spatial/temporal subsetting
  5. Format conversion
  6. Local save and/or S3 upload
  7. Return result dictionary with status and file paths

---

## 5. CLI Implementation (`main.py`)

### 5.1 CLI Structure
- [ ] Import dependencies: `click`, `pprint`, `traceback`, `json`
- [ ] Import core classes: `_MeteoBlueIngestor`, `_MeteoBlueRetriever`
- [ ] Import utilities: `Logger`, `StatusException`, `prologo`, `epilogo`

### 5.2 Argument Definition Classes
- [ ] Create `_ARG_NAMES_METEOBLUE_INGESTOR` class with dictionaries for each parameter:
  - `aliases`: list of CLI flag variations (e.g., `['--variable', '--var']`)
  - `help`: description text
  - `default`: default value
  - `example`: usage example

- [ ] Create `_ARG_NAMES_METEOBLUE_RETRIEVER` class similarly

### 5.3 Ingestor CLI Command
- [ ] Implement `cli_run_meteoblue_ingestor()` function:
  - Decorate with `@click.command()`
  - Add `@click.option()` decorators for each ingestor parameter
  - Add common Gecosistema CLI options: `--backend`, `--jid`, `--token`, `--request_uuid`
  - Implement execution flow:
    1. Call `prologo()` for initialization
    2. Validate token if required
    3. Parse and validate arguments
    4. Instantiate `_MeteoBlueIngestor`
    5. Call `.execute(**kwargs)`
    6. Handle `StatusException` and generic exceptions
    7. Log results with `Logger`
    8. Call `epilogo()` for cleanup
    9. Return result or exit code

### 5.4 Retriever CLI Command
- [ ] Implement `cli_run_meteoblue_retriever()` function:
  - Similar structure to ingestor CLI
  - Add retriever-specific options (lat_range, long_range, time_range, out_format)
  - Use `_MeteoBlueRetriever` class

### 5.5 Programmatic Entry Points
- [ ] Implement `run_meteoblue_ingestor(**kwargs)` wrapper function
  - Call ingestor logic without CLI parsing
  - Return structured result dict

- [ ] Implement `run_meteoblue_retriever(**kwargs)` wrapper function
  - Similar to ingestor wrapper

---

## 6. PyGeoAPI Processor Integration (Optional)

### 6.1 Retriever Processor (`meteoblue/meteoblue_retriever_processor.py`)
- [ ] Import `pygeoapi.process.base.BaseProcessor`
- [ ] Create `MeteoBlueRetrieverProcessor(BaseProcessor)` class

- [ ] Define `PROCESS_METADATA` dict with:
  - `version`, `id`, `title`, `description`
  - `jobControlOptions`: `['sync-execute', 'async-execute']`
  - `keywords`: list of relevant tags
  - `inputs`: schema for each parameter (variable, lat_range, time_range, etc.)
  - `outputs`: schema for result structure
  - `example`: sample request payload

- [ ] Implement `__init__(processor_def)` method
  - Call `super().__init__()`
  - Initialize logger and retriever instance

- [ ] Implement `execute(data)` method
  - Parse input parameters from `data` dict
  - Set debug mode if requested
  - Call `_MeteoBlueRetriever.execute(**data)`
  - Return result dict conforming to PyGeoAPI response schema

- [ ] Implement `__repr__()` for object representation

### 6.2 Ingestor Processor (`meteoblue/meteoblue_ingestor_processor.py`)
- [ ] Create `MeteoBlueIngestorProcessor(BaseProcessor)` class
- [ ] Define `PROCESS_METADATA` specific to ingestor workflow
- [ ] Implement methods similar to retriever processor

### 6.3 Conditional Import
- [ ] In `__init__.py`, conditionally import processor classes:
  ```python
  import importlib.util
  if importlib.util.find_spec('pygeoapi') is not None:
      from .meteoblue import MeteoBlueRetrieverProcessor, MeteoBlueIngestorProcessor
  ```

---

## 7. Utility Modules

### 7.1 Logger (`cli/module_log.py`)
- [ ] Implement `Logger` class or configure logging
- [ ] Support debug mode with `set_log_debug()`
- [ ] Methods: `.debug()`, `.info()`, `.warning()`, `.error()`

### 7.2 Status Exception (`utils/status_exception.py`)
- [ ] Create `StatusException(Exception)` class
- [ ] Define status constants: `OK`, `PARTIAL`, `SKIPPED`, `DENIED`, `INVALID`, `ERROR`
- [ ] Store `status` and `message` attributes

### 7.3 S3 Module (`utils/module_s3.py`)
- [ ] Implement S3 helper functions using `boto3`:
  - `s3_upload(local_path, s3_uri)`: upload file to S3
  - `s3_download(s3_uri, local_path)`: download file from S3
  - `s3_exists(s3_uri)`: check if S3 object exists
  - `s3_list(s3_prefix)`: list objects under prefix
- [ ] Handle `NoCredentialsError` and `ClientError` exceptions
- [ ] Parse S3 URIs with `urlparse()`

### 7.4 Filesystem (`utils/filesystem.py`)
- [ ] Implement utility functions:
  - `justext(path)`: extract file extension
  - `justfname(path)`: extract filename without extension
  - `justpath(path)`: extract directory path
  - `forceext(path, ext)`: change file extension
- [ ] Create temporary directory management functions

### 7.5 String Parsing (`utils/strings.py`)
- [ ] Implement `parse_event(event, function)` for Lambda event parsing
  - Extract function parameters from event dict
  - Map event keys to function argument names
  - Convert string booleans to bool type
  - Return `**kwargs` dict

### 7.6 Prolog/Epilog (`utils/module_prologo.py`)
- [ ] Implement `prologo()` function:
  - Display ASCII logo
  - Set environment variables (JID, backend URL)
  - Initialize status tracking
  - Create temporary directories

- [ ] Implement `epilogo()` function:
  - Cleanup temporary files
  - Send final status to backend if configured
  - Log execution summary

### 7.7 Status Module (`utils/module_status.py`)
- [ ] Implement status tracking functions:
  - `send_status(backend_url, jid, status, progress, message)`
  - Handle HTTP requests to backend server
  - Retry logic for failed status updates

---

## 8. AWS Lambda Integration

### 8.1 Lambda Handler (`lambda/lambda_function.py`)
- [ ] Import `parse_event` and main execution function
- [ ] Implement `lambda_handler(event, context)`:
  - Parse event using `parse_event()`
  - Call `run_meteoblue_ingestor()` or `run_meteoblue_retriever()` based on event
  - Return response dict with `statusCode` and `body`
- [ ] Add `if __name__ == "__main__"` block for local testing

### 8.2 Docker Configuration (`Dockerfile`)
- [ ] Create Dockerfile for Lambda container:
  - Base image: AWS Lambda Python runtime
  - Copy source code and requirements
  - Install dependencies
  - Set CMD to Lambda handler

### 8.3 Lambda Entry Script (`lambda-entrypoint.sh`)
- [ ] Create shell script for Lambda initialization
- [ ] Set environment variables
- [ ] Invoke Lambda runtime interface

---

## 9. Testing and Documentation

### 9.1 Unit Tests (`tests/`)
- [ ] Create `test_ingestor.py` with unit tests for ingestor methods
- [ ] Create `test_retriever.py` with unit tests for retriever methods
- [ ] Create `test_utils.py` for utility function tests
- [ ] Use `pytest` framework with fixtures for common test data
- [ ] Mock external API calls with `unittest.mock` or `responses` library

### 9.2 Integration Tests
- [ ] Create integration test for full ingestor workflow
- [ ] Create integration test for full retriever workflow
- [ ] Test S3 operations with MinIO or localstack

### 9.3 README Documentation
- [ ] Document installation: `pip install process-meteoblue-hub`
- [ ] Document CLI usage examples:
  ```bash
  meteoblue-ingestor --variable temperature --forecast_run latest
  meteoblue-retriever --variable precipitation --time_range "2025-01-01T00:00:00,2025-01-02T00:00:00"
  ```
- [ ] Document PyGeoAPI configuration example
- [ ] Document API credentials setup (environment variables or .env file)

### 9.4 API Documentation
- [ ] Document MeteoBlue API endpoints and authentication
- [ ] Document variable codes and their meanings
- [ ] Document data formats and coordinate systems

---

## 10. Dependencies and Environment

### 10.1 Python Dependencies
- [ ] Core: `python-dotenv`, `requests`, `click`, `filelock`
- [ ] Data processing: `numpy`, `pandas`, `xarray`, `rioxarray`, `netcdf4`
- [ ] Geospatial: `geopandas`, `shapely`, `gdal` (via optional dependency)
- [ ] AWS: `boto3`
- [ ] GRIB support: `pygrib` or `cfgrib`
- [ ] Optional: `pygeoapi`, `gdal2numpy`, `numba`

### 10.2 Environment Variables
- [ ] Document required environment variables:
  - `METEOBLUE_API_KEY`: API authentication key
  - `METEOBLUE_API_URL`: API base URL (if not using default)
  - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`: S3 credentials
  - `AWS_REGION`: S3 region

### 10.3 Development Environment
- [ ] Create `.env.example` template file
- [ ] Create `requirements.txt` for pip installs
- [ ] Create separate requirements files for Python 3.11, 3.12, 3.13 if needed
- [ ] Configure `.gitignore` to exclude virtual environments, temp files, credentials

---

## 11. Packaging and Distribution

### 11.1 Build Configuration
- [ ] Verify `pyproject.toml` completeness
- [ ] Ensure `setuptools.build_meta` backend configured
- [ ] Test local installation: `pip install -e .`
- [ ] Test optional dependencies: `pip install -e .[pygeoapi]`

### 11.2 Version Management
- [ ] Implement versioning strategy (semantic versioning)
- [ ] Store version in `pyproject.toml` and `cli/module_version.py`
- [ ] Create release workflow

### 11.3 Distribution
- [ ] Build package: `python -m build`
- [ ] Test installation from built wheel
- [ ] Configure PyPI credentials if public release planned
- [ ] Publish to PyPI or private package repository

---

## 12. Deployment Considerations

### 12.1 AWS Lambda
- [ ] Configure Lambda function with sufficient memory and timeout
- [ ] Set up Lambda layers for large dependencies (GDAL, NumPy)
- [ ] Configure Lambda environment variables for API keys
- [ ] Set up EventBridge schedules for automated ingestion

### 12.2 PyGeoAPI Integration
- [ ] Register process in PyGeoAPI configuration YAML
- [ ] Configure process endpoints
- [ ] Test process execution via PyGeoAPI API

### 12.3 Monitoring
- [ ] Implement CloudWatch logging for Lambda executions
- [ ] Set up error alerting
- [ ] Track execution metrics (duration, data volume, success rate)

---

## Implementation Priority

**Phase 1 - Foundation (Weeks 1-2)**
- Items 1.1, 1.2, 2.1, 2.2, 7.4 (basic structure and constants)

**Phase 2 - Core Ingestor (Weeks 3-4)**
- Items 3.1-3.7 (full ingestor implementation)

**Phase 3 - Core Retriever (Weeks 5-6)**
- Items 4.1-4.7 (full retriever implementation)

**Phase 4 - CLI Integration (Week 7)**
- Items 5.1-5.5 (command-line interface)

**Phase 5 - Utilities and Testing (Weeks 8-9)**
- Items 7.1-7.7, 9.1-9.2 (utilities and tests)

**Phase 6 - Advanced Features (Weeks 10-11)**
- Items 2.3, 6.1-6.3, 8.1-8.3 (derived variables, PyGeoAPI, Lambda)

**Phase 7 - Documentation and Deployment (Week 12)**
- Items 9.3-9.4, 10.1-10.3, 11.1-11.3, 12.1-12.3 (docs and deployment)

---

## Notes and Considerations

### MeteoBlue API Specifics
<!-- ASSUMPTION(agent): MeteoBlue API structure may differ from ICON-2I -->
- [ ] Research MeteoBlue API authentication mechanism (API key, OAuth, etc.)
- [ ] Identify MeteoBlue data format (GRIB, NetCDF, JSON, etc.)
- [ ] Determine available forecast variables and their naming conventions
- [ ] Check forecast run schedules and data availability windows
- [ ] Verify coordinate reference system used by MeteoBlue

### Data Format Handling
<!-- TODO(agent): Adapt GRIB processing to MeteoBlue format -->
- If MeteoBlue uses GRIB: reuse `pygrib` approach from ICON-2I
- If MeteoBlue uses NetCDF: simplify processing with direct xarray loading
- If MeteoBlue uses JSON/CSV: implement custom parsers

### Licensing and Attribution
- [ ] Verify MeteoBlue API terms of service
- [ ] Add attribution requirements to documentation
- [ ] Ensure license compatibility (MIT license assumed)

---

## Context Files Integration

### Alignment with `context/index.json`
<!-- TODO(agent): Create workflow definitions in context registry -->
- Register `meteoblue-ingest` workflow with ordered steps
- Register `meteoblue-retrieve` workflow with ordered steps
- Define input/output contracts for each workflow

### Structured Knowledge Files
<!-- TODO(agent): Create atomic context files -->
- `context/meteoblue-variables.json`: variable definitions and metadata
- `context/meteoblue-api-schema.json`: API endpoint documentation
- `context/meteoblue-processing-steps.json`: data transformation logic
- Each file must include `id`, `version`, `description`, `status` fields
