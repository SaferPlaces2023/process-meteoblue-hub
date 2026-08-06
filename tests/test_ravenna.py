"""
Test script to fetch precipitation data for Ravenna area using process-meteoblue-hub
Ravenna bbox: 44.2445°N-44.5885°N, 11.9544°E-12.4289°E
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

# Import from process-meteoblue-hub
from process_meteoblue_hub.meteoblue import _MeteoblueRetriever, _MeteoblueIngestor

METEOBLUE_API_KEY = "coudqf8PJGiaVT5Z"
S3_SOURCE = "s3://saferplaces.co/packages/process-meteoblue-hub/tests/src/"
S3_DESTINATION = "s3://saferplaces.co/packages/process-meteoblue-hub/tests/out/"

def fetch_ravenna_precipitation():
    """Fetch precipitation forecast for Ravenna area"""
    
    # Set API key in environment (required by Meteoblue ingestor)
    os.environ["METEOBLUE_API_KEY"] = METEOBLUE_API_KEY
    
    # Configuration
    LOCATION = "Ravenna"
    VARIABLE = "precipitation"
    SERVICE = "basic-5min"
    # Ravenna bounding box
    LAT_RANGE = [44.2445, 44.5885]
    LONG_RANGE = [11.9544, 12.4289]
    GRID_RES = 20000  # 20km resolution
    
    # Time range: now to +5 hours (Meteoblue forecast window)
    # Use tz-naive UTC datetimes to match xarray dataset tz-naive time coords
    now = datetime.utcnow().replace(microsecond=0)
    time_start = now.isoformat()
    time_end = (now + timedelta(hours=5)).isoformat()
    
    # Output directory
    output_dir = Path(__file__).parent.parent / "output" / "ravenna"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # S3 configuration (optional)
    s3_source = os.environ.get("S3_SOURCE", S3_SOURCE)
    s3_destination = os.environ.get("S3_DESTINATION", S3_DESTINATION)
    
    print("=" * 60)
    print("Meteoblue Precipitation Retriever - Ravenna")
    print("=" * 60)
    print(f"Location: {LOCATION}")
    print(f"Variable: {VARIABLE}")
    print(f"Bbox: [{LAT_RANGE[0]}, {LAT_RANGE[1]}] x [{LONG_RANGE[0]}, {LONG_RANGE[1]}]")
    print(f"Time: {time_start} to {time_end}")
    print(f"Output: {output_dir}")
    print("=" * 60)
    
    try:
        # Initialize retriever
        retriever = _MeteoblueRetriever()
        
        # Retrieve data
        output_file = output_dir / f"precipitation_{LOCATION.lower()}_{now.strftime('%Y%m%d_%H%M%S')}.tif"
        result = retriever.run(
            variable=VARIABLE,
            location_name=LOCATION,
            lat_range=LAT_RANGE,
            long_range=LONG_RANGE,
            time_range=[time_start, time_end],
            out_format="tif",
            out=str(output_file),
            bucket_source=s3_source,
            bucket_destination=s3_destination,
            debug=True
        )
        
        print("\n" + "=" * 60)
        print("Retrieval completed successfully!")
        print(f"Output file: {output_file}")
        print(f"Result: {result}")
        print("=" * 60)
        
        return result
        
    except Exception as e:
        print(f"\nError during retrieval: {e}")
        raise


def ingest_ravenna_data():
    """Ingest precipitation data for Ravenna area (optional pre-step)"""
    
    # Set API key in environment (required by Meteoblue ingestor)
    os.environ["METEOBLUE_API_KEY"] = METEOBLUE_API_KEY
    
    LOCATION = "Ravenna"
    VARIABLE = "precipitation"
    SERVICE = "basic-5min"
    
    LAT_RANGE = [44.2445, 44.5885]
    LONG_RANGE = [11.9544, 12.4289]
    GRID_RES = 20000
    TIME_DELTA = 60  # 1-hour intervals
    
    output_dir = Path(__file__).parent.parent / "output" / "ravenna" / "netcdf"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    s3_destination = os.environ.get("S3_DESTINATION", S3_DESTINATION)
    
    print(f"Ingesting {VARIABLE} data for {LOCATION}...")
    
    try:
        ingestor = _MeteoblueIngestor()
        result = ingestor.run(
            variable=VARIABLE,
            service=SERVICE,
            location_name=LOCATION,
            lat_range=LAT_RANGE,
            long_range=LONG_RANGE,
            grid_res=GRID_RES,
            time_delta=TIME_DELTA,
            out_dir=str(output_dir),
            bucket_destination=s3_destination,
            debug=True
        )
        print(f"Ingestion completed: {result}")
        return result
        
    except Exception as e:
        print(f"Error during ingestion: {e}")
        raise


if __name__ == "__main__":
    # Fetch precipitation data
    fetch_ravenna_precipitation()
    
    # Optionally ingest data first:
    # ingest_ravenna_data()
    # fetch_ravenna_precipitation()