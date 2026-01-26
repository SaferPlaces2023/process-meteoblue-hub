---
name: gdal-ogr
description: Geospatial Data Abstraction Library for raster (GDAL) and vector (OGR) data format translation, transformation, and processing via command-line tools.
---

# GDAL/OGR Skill

This skill provides expertise in GDAL/OGR command-line tools for geospatial data conversion, reprojection, and processing.

## Purpose

Use this skill when working with:

- Format conversion (GeoJSON, Shapefile, GeoPackage, GeoTIFF, etc.)
- Coordinate system reprojection
- Raster processing (mosaic, clip, resample, translate)
- Vector data manipulation
- Data inspection and validation

## Core Commands

### Vector Tools (OGR)

#### ogr2ogr - Vector Conversion & Transformation

```bash
# Convert Shapefile to GeoPackage
ogr2ogr -f "GPKG" output.gpkg input.shp

# Reproject to EPSG:4326
ogr2ogr -t_srs EPSG:4326 output.geojson input.shp

# Filter by SQL
ogr2ogr -sql "SELECT * FROM layer WHERE population > 10000" output.gpkg input.gpkg

# Clip to bounding box
ogr2ogr -spat xmin ymin xmax ymax output.gpkg input.gpkg

# Append to existing layer
ogr2ogr -append -update output.gpkg input.gpkg
```

#### ogrinfo - Vector Inspection

```bash
# List layers
ogrinfo input.gpkg

# Layer summary
ogrinfo -so input.gpkg layer_name

# Show all features
ogrinfo -al input.gpkg
```

### Raster Tools (GDAL)

#### gdalinfo - Raster Inspection

```bash
# Basic info
gdalinfo input.tif

# Include statistics
gdalinfo -stats input.tif

# JSON output
gdalinfo -json input.tif
```

#### gdal_translate - Raster Conversion

```bash
# Convert format
gdal_translate -of GTiff input.png output.tif

# Extract subset
gdal_translate -srcwin xoff yoff xsize ysize input.tif output.tif

# Scale values
gdal_translate -scale 0 255 0 1 input.tif output.tif

# Add compression
gdal_translate -co COMPRESS=LZW input.tif output.tif
```

#### gdalwarp - Reprojection & Resampling

```bash
# Reproject
gdalwarp -t_srs EPSG:32632 input.tif output.tif

# Resample to new resolution
gdalwarp -tr 10 10 input.tif output.tif

# Clip to shapefile
gdalwarp -cutline mask.shp -crop_to_cutline input.tif output.tif

# Mosaic multiple files
gdalwarp input1.tif input2.tif mosaic.tif
```

#### gdal_merge.py - Mosaic Rasters

```bash
gdal_merge.py -o mosaic.tif input1.tif input2.tif input3.tif
```

## Common Formats

| Format     | Driver         | Extension |
| ---------- | -------------- | --------- |
| GeoPackage | GPKG           | .gpkg     |
| GeoJSON    | GeoJSON        | .geojson  |
| Shapefile  | ESRI Shapefile | .shp      |
| GeoTIFF    | GTiff          | .tif      |
| PostGIS    | PostgreSQL     | -         |
| CSV        | CSV            | .csv      |

## Best Practices

1. **Use GeoPackage**: Prefer `.gpkg` over Shapefile for vector data
2. **Add compression**: Use `-co COMPRESS=LZW` or `DEFLATE` for rasters
3. **Preserve metadata**: Use `-mo` flag to maintain metadata
4. **Check CRS first**: Run `ogrinfo`/`gdalinfo` before transformations
5. **Use VRT for virtual mosaics**: Avoid creating large intermediate files

## Python Bindings

```python
from osgeo import gdal, ogr

# Open raster
ds = gdal.Open("input.tif")
band = ds.GetRasterBand(1)
arr = band.ReadAsArray()

# Open vector
ds = ogr.Open("input.gpkg")
layer = ds.GetLayer(0)
for feature in layer:
    geom = feature.GetGeometryRef()
```

## Documentation

- GDAL: <https://gdal.org/programs/index.html>
- OGR: <https://gdal.org/programs/ogr_programs.html>
- Drivers: <https://gdal.org/drivers/index.html>
