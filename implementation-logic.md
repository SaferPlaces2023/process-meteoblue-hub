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
- [ ] Define `_DATASET_NAME` constant: `'METEOBLUE'`
- [ ] Define MeteoBlue service configurations dictionary:
  ```python
  _METEOBLUE_SERVICES = {
      'basic-5min': {
          'api_url': 'https://my.meteoblue.com/packages/basic-5min',
          'response_data_key': 'data_xmin',
          'init_time_frequency': '6h',
          'time_resolution': 5  # minutes
      },
      'basic-1h': {
          'api_url': 'https://my.meteoblue.com/packages/basic-1h',
          'response_data_key': 'data_1h',
          'init_time_frequency': '6h',
          'time_resolution': 60  # minutes
      }
  }
  ```
- [ ] Define API authentication: `METEOBLUE_API_KEY` environment variable
- [ ] Define base request parameters structure (format='json', apikey, lat, lon)

### 2.2 Variable and Service Definitions
- [ ] Define `_VARIABLE_NAME` constant: `'precipitation'`
- [ ] Define `_SERVICE_TYPES` list: `['basic-5min', 'basic-1h']`
- [ ] Define grid resolution constraints:
  - Minimum: 100 meters
  - Must be multiple of 100
  - Default: 1000 meters

### 2.3 Data Constraints
- [ ] Define forecast availability window: 7 days from current date (MeteoBlue limit)
- [ ] Define time delta constraints per service:
  - `basic-5min`: multiple of 5 minutes
  - `basic-1h`: multiple of 60 minutes
- [ ] Define coordinate system: EPSG:4326 (WGS84)
- [ ] Define data caching frequency: 6 hours (init_time_frequency)

---
