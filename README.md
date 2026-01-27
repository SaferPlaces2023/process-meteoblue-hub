# process-meteoblue-hub

Python package for ingesting and retrieving meteorological data from Meteoblue via API. The package provides two main commands (`meteoblue-ingestor` and `meteoblue-retriever`) to download Meteoblue data in NetCDF format and convert it to GeoTIFF rasters.

## Description

`process-meteoblue-hub` is a package that implements modules to perform ingest and retrieve operations for meteorological data from Meteoblue. The package is designed to integrate with pygeoapi processes and provides functionality for:

- **Ingest**: Acquisition of data from Meteoblue API and saving in NetCDF format
- **Retrieve**: Recovery and transformation of NetCDF data into georeferenced GeoTIFF rasters

The package currently supports meteorological variables such as precipitation, temperature, wind, humidity and other parameters provided by Meteoblue services (basic-5min, basic-1h).

## Requirements

- Python 3.8+
- Meteoblue API key (to be configured as environment variable `METEOBLUE_API_KEY`)
- Dependencies: `xarray`, `rioxarray`, `netcdf4`, `geopandas`, `boto3`, `click`, `requests`, `aiohttp`

## Installation

### Installation from local repository

```bash
# Clone or download the repository
cd process-meteoblue-hub

# Install the package in editable mode (for development)
pip install -e .

# Or install the package in standard mode
pip install .
```

### Installation with optional dependencies for pygeoapi

```bash
pip install -e ".[pygeoapi]"
```

### Environment variables configuration

Create a `.env` file in the working directory with the API key:

```bash
METEOBLUE_API_KEY=your_api_key_here
```

## Usage

The package provides two main CLI commands:

### 1. meteoblue-ingestor

Acquires data from Meteoblue API and saves it in NetCDF format.

#### Syntax

```bash
meteoblue-ingestor --location_name <name> --lat_range <min,max> --long_range <min,max> [options]
```

#### Example

```bash
meteoblue-ingestor \
  --location_name Milan \
  --variable precipitation \
  --service basic-5min \
  --lat_range 45.0,46.0 \
  --long_range 9.0,10.0 \
  --grid_res 1000 \
  --time_delta 60 \
  --out_dir ./output/netcdf \
  --bucket_destination s3://my-bucket/meteoblue/ingest/
```

#### meteoblue-ingestor parameters

| Parameter | Alias | Type | Required | Default | Description | Example |
|-----------|-------|------|----------|---------|-------------|---------|
| `--variable` | `--var` | str | No | `precipitation` | Variable(s) to ingest. Comma-separated list. Values: `precipitation`, `temperature`, `windspeed`, `winddirection`, `relativehumidity`, `snowfraction`, etc. | `--variable precipitation` |
| `--service` | `--svc` | str | No | `basic-5min` | Meteoblue service to use. Values: `basic-5min`, `basic-1h` | `--service basic-5min` |
| `--location_name` | `--location`, `--loc` | str | **Yes** | - | Location identifier for the ingested data | `--location_name Milan` |
| `--lat_range` | `--lat` | str | No | - | Latitude range as `[min,max]` in EPSG:4326 | `--lat_range 45.0,46.0` |
| `--long_range` | `--lon` | str | No | - | Longitude range as `[min,max]` in EPSG:4326 | `--long_range 9.0,10.0` |
| `--grid_res` | `--res` | int | No | `1000` | Grid resolution in meters (minimum 100, multiple of 100) | `--grid_res 1000` |
| `--time_delta` | `--td` | int | No | Depends on service | Time interval in minutes for output data (must be multiple of service resolution: 5 min for basic-5min, 60 min for basic-1h) | `--time_delta 60` |
| `--out_dir` | `--output_dir`, `--od` | str | No | Temporary directory | Output directory for the ingested data | `--out_dir ./output/netcdf` |
| `--bucket_destination` | `--bucket`, `--s3` | str | No | - | S3 destination bucket for output data | `--bucket_destination s3://my-bucket/path/` |
| `--backend` | - | str | No | - | Backend for sending status updates to the server | `--backend http://backend-url` |
| `--jid` | - | str | No | Auto-generated | Job ID for status tracking | `--jid job-12345` |
| `--version` | - | flag | No | `False` | Show the package version | `--version` |
| `--debug` | - | flag | No | `False` | Debug mode | `--debug` |
| `--verbose` | - | flag | No | `False` | Verbose output | `--verbose` |

### 2. meteoblue-retriever

Retrieves data from NetCDF files and creates georeferenced GeoTIFF outputs.

#### Syntax

```bash
meteoblue-retriever --location_name <name> --time_range <start,end> [options]
```

#### Example

```bash
meteoblue-retriever \
  --location_name Milan \
  --variable precipitation \
  --time_range 2026-01-27T00:00:00,2026-01-28T00:00:00 \
  --lat_range 45.0,46.0 \
  --long_range 9.0,10.0 \
  --out_format tif \
  --out ./output/precipitation_milan.tif \
  --bucket_source s3://my-bucket/meteoblue/ingest/ \
  --bucket_destination s3://my-bucket/meteoblue/retrieve/
```

#### meteoblue-retriever parameters

| Parameter | Alias | Type | Required | Default | Description | Example |
|-----------|-------|------|----------|---------|-------------|---------|
| `--variable` | `--var` | str | No | All variables | Variable to retrieve. Values: `precipitation`, `temperature`, `windspeed`, etc. | `--variable precipitation` |
| `--location_name` | `--location`, `--loc` | str | **Yes** | - | Location identifier for the data | `--location_name Milan` |
| `--lat_range` | `--lat` | str | No | - | Latitude range as `[min,max]` in EPSG:4326 | `--lat_range 45.0,46.0` |
| `--long_range` | `--lon` | str | No | - | Longitude range as `[min,max]` in EPSG:4326 | `--long_range 9.0,10.0` |
| `--time_range` | `--time` | str | **Yes** | - | Time range as `[start,end]` in ISO format. If only start is provided, end = start + 1 day | `--time_range 2026-01-27T00:00:00,2026-01-28T00:00:00` |
| `--out_format` | `--format` | str | No | `tif` | Output format. Currently supported: `tif` | `--out_format tif` |
| `--out` | `--output` | str | No | Temporary directory | Output file path | `--out ./output/result.tif` |
| `--bucket_source` | `--source`, `--s3_source` | str | No | - | Source S3 bucket URI where NetCDF files are stored | `--bucket_source s3://my-bucket/ingest/` |
| `--bucket_destination` | `--bucket`, `--s3` | str | No | - | Destination S3 bucket URI for output | `--bucket_destination s3://my-bucket/retrieve/` |
| `--backend` | - | str | No | - | Backend for sending status updates to the server | `--backend http://backend-url` |
| `--jid` | - | str | No | Auto-generated | Job ID for status tracking | `--jid job-12345` |
| `--version` | - | flag | No | `False` | Show the package version | `--version` |
| `--debug` | - | flag | No | `False` | Debug mode | `--debug` |
| `--verbose` | - | flag | No | `False` | Verbose output | `--verbose` |

## Supported Variables

The package supports the following meteorological variables from Meteoblue:

| Variable Name | Code | Description |
|---------------|------|-------------|
| `PRECIPITATION` | `precipitation` | Precipitation |
| `TEMPERATURE` | `temperature` | Temperature |
| `WINDSPEED` | `windspeed` | Wind speed |
| `WINDDIRECTION` | `winddirection` | Wind direction |
| `RELATIVEHUMIDITY` | `relativehumidity` | Relative humidity |
| `SNOWFRACTION` | `snowfraction` | Snow fraction |
| `PRECIPITATION_PROBABILITY` | `precipitation_probability` | Precipitation probability |
| `CONVECTIVE_PRECIPITATION` | `convective_precipitation` | Convective precipitation |
| `RAINSPOT` | `rainspot` | Rainspot |
| `PICTOCODE` | `pictocode` | Pictocode |
| `FELTTEMPERATURE` | `felttemperature` | Felt temperature |
| `ISDAYLIGHT` | `isdaylight` | Is daylight |
| `UVINDEX` | `uvindex` | UV index |
| `SEALEVELPRESSURE` | `sealevelpressure` | Sea level pressure |

## Typical Workflow

1. **Data acquisition (Ingest)**:
   ```bash
   meteoblue-ingestor \
     --location_name MyLocation \
     --lat_range 45.0,46.0 \
     --long_range 9.0,10.0 \
     --variable precipitation \
     --service basic-1h \
     --bucket_destination s3://my-bucket/ingest/
   ```

2. **Retrieval and conversion (Retrieve)**:
   ```bash
   meteoblue-retriever \
     --location_name MyLocation \
     --time_range 2026-01-27T00:00:00,2026-01-28T00:00:00 \
     --variable precipitation \
     --bucket_source s3://my-bucket/ingest/ \
     --out ./precipitation_output.tif
   ```

## Integration with pygeoapi

The package can be integrated with pygeoapi through the processors:
- `MeteoblueIngestorProcessor` 
- `MeteoblueRetrieverProcessor`

To use the pygeoapi processors, install the package with optional dependencies:

```bash
pip install -e ".[pygeoapi]"
```

## License

Copyright (c) 2025 Gecosistema S.r.l.

## Author

Tommaso Redaelli - tommaso.redaelli@gecosistema.com

## Repository

https://github.com/SaferPlaces2023/process-meteoblue-hub