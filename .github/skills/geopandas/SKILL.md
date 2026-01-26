---
name: geopandas
description: Python library extending pandas for geospatial data, enabling spatial operations, file I/O, and integration with shapely geometries.
---

# GeoPandas Skill

This skill provides expertise in GeoPandas for Python-based geospatial data manipulation and analysis.

## Purpose

Use this skill when working with:

- Reading/writing geospatial file formats
- Vector data manipulation in Python
- Spatial operations (buffer, overlay, spatial joins)
- Attribute-based and spatial queries
- Data visualization with matplotlib
- Integration with pandas workflows

## Core Concepts

### GeoDataFrame

A pandas DataFrame with a special `geometry` column containing shapely geometries.

```python
import geopandas as gpd

# Read file
gdf = gpd.read_file("data.gpkg")

# Inspect
print(gdf.head())
print(gdf.crs)
print(gdf.geometry.type.unique())
```

### GeoSeries

A pandas Series of shapely geometries with CRS awareness.

```python
# Access geometry column
gdf.geometry
gdf['geometry']

# Create from WKT
from shapely import wkt
gs = gpd.GeoSeries([wkt.loads("POINT (0 0)")])
```

## File I/O

### Reading Data

```python
# GeoPackage
gdf = gpd.read_file("data.gpkg", layer="buildings")

# Shapefile
gdf = gpd.read_file("data.shp")

# GeoJSON
gdf = gpd.read_file("data.geojson")

# PostGIS
from sqlalchemy import create_engine
engine = create_engine("postgresql://user:pass@localhost/db")
gdf = gpd.read_postgis("SELECT * FROM parcels", engine, geom_col="geom")

# Read only specific bbox
gdf = gpd.read_file("data.gpkg", bbox=(xmin, ymin, xmax, ymax))

# Read with SQL filter (GPKG/PostGIS)
gdf = gpd.read_file("data.gpkg", where="population > 10000")
```

### Writing Data

```python
# GeoPackage (preferred)
gdf.to_file("output.gpkg", driver="GPKG", layer="results")

# GeoJSON
gdf.to_file("output.geojson", driver="GeoJSON")

# PostGIS
gdf.to_postgis("table_name", engine, if_exists="replace")

# Parquet (fast columnar format)
gdf.to_parquet("output.parquet")
```

## Spatial Operations

### Geometry Methods

```python
# Buffer
gdf['buffered'] = gdf.geometry.buffer(100)

# Centroid
gdf['centroid'] = gdf.geometry.centroid

# Simplify
gdf['simple'] = gdf.geometry.simplify(tolerance=10)

# Convex hull
gdf['hull'] = gdf.geometry.convex_hull

# Bounds
gdf[['minx', 'miny', 'maxx', 'maxy']] = gdf.bounds
```

### Measurements

```python
# Area (use projected CRS!)
gdf['area_m2'] = gdf.to_crs(epsg=32632).geometry.area

# Length
gdf['length_m'] = gdf.to_crs(epsg=32632).geometry.length

# Distance to point
from shapely.geometry import Point
ref_point = Point(11.25, 43.77)
gdf['dist'] = gdf.geometry.distance(ref_point)
```

### Overlay Operations

```python
# Intersection
result = gpd.overlay(gdf1, gdf2, how='intersection')

# Union
result = gpd.overlay(gdf1, gdf2, how='union')

# Difference
result = gpd.overlay(gdf1, gdf2, how='difference')

# Symmetric difference
result = gpd.overlay(gdf1, gdf2, how='symmetric_difference')
```

### Spatial Joins

```python
# Points in polygons
points_with_zones = gpd.sjoin(points, zones, how='left', predicate='within')

# Nearest join
nearest = gpd.sjoin_nearest(gdf1, gdf2, how='left', distance_col='dist')
```

### Dissolve (Group by geometry)

```python
# Dissolve by attribute
dissolved = gdf.dissolve(by='region', aggfunc='sum')

# Dissolve all
single_geom = gdf.dissolve()
```

## CRS Handling

```python
# Check CRS
print(gdf.crs)

# Set CRS (if missing)
gdf = gdf.set_crs(epsg=4326)

# Reproject
gdf_utm = gdf.to_crs(epsg=32632)

# Reproject to match another GeoDataFrame
gdf1 = gdf1.to_crs(gdf2.crs)
```

## Visualization

```python
import matplotlib.pyplot as plt

# Basic plot
gdf.plot()

# Choropleth
gdf.plot(column='population', cmap='viridis', legend=True)

# Multiple layers
fig, ax = plt.subplots(figsize=(10, 10))
zones.plot(ax=ax, color='lightgray', edgecolor='black')
points.plot(ax=ax, color='red', markersize=5)
plt.show()

# Interactive (folium)
gdf.explore(column='category', cmap='Set1')
```

## Best Practices

1. **Use GeoPackage**: Prefer over Shapefile for better performance
2. **Project before measuring**: Use appropriate projected CRS for area/length
3. **Use spatial indexing**: `gdf.sindex` for faster spatial queries
4. **Validate geometries**: `gdf.geometry.is_valid`, `gdf.geometry.make_valid()`
5. **Use Parquet for large data**: `gdf.to_parquet()` for fast read/write
6. **Chain operations**: Leverage pandas-style method chaining

## Documentation

- GeoPandas: <https://geopandas.org/>
- User Guide: <https://geopandas.org/en/stable/docs/user_guide.html>
- API Reference: <https://geopandas.org/en/stable/docs/reference.html>
