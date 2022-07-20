# Problem statement

```mermaid
flowchart LR
  A(["Vector file(s)"]) --> B[/Spatially-distributed GeoDataFrame/]
  B[/Spatially-distributed GeoDataFrame/] --> A(["Vector file(s)"])
  B[/Spatially-distributed GeoDataFrame/] --> C[/"Spatially-distributed multi-dimensional array(s)"/]
  C[/"Spatially-distributed multi-dimensional array(s)"/] --> B[/Spatially-distributed GeoDataFrame/]
  C[/"Spatially-distributed multi-dimensional array(s)"/] --> D(["Raster file(s)"])
  D(["Raster file(s)"]) --> C[/"Spatially-distributed multi-dimensional array(s)"/]
```

## Definitions

### Vector file(s)

- One of more vector files, containing one of more layers, that represent a single spatial dataset.
- May be stored on disk either locally or remotelly, though, in reality, they will be stored in an Azure Datalake. 
- May be partitioned spatially or aspatially. For example, [NPD](https://use-land-property-data.service.gov.uk/datasets/nps#polygon) comes as 10 shapefiles that appear to have been partitioned by row number, whereas, PHI is partitioned into rough regions.
- They may or may not share the same schema

## Options

### Spark / Databricks + RasterFrames

- Read vector file(s) from disk into an aspatial Spark DataFrame (i.e. a vanilla Spark DataFrame with the geometry column encoded as WKB or WKT) 
- Use a Spark spartial library (Sedona, GeoMesa, Mosaic, Geode, etc) to convert it to a spatial Spark DataFrame (i.e. geometry column encoded as a UDT)
- Do stuff

### Spark / Databricks + potential Mosaic solution

TODO

### Dask / [Pangeo](https://pangeo.io/)

TODO

### Hybrid

- Read vector file(s) from disk into an aspatial Spark DataFrame (i.e. a vanilla Spark DataFrame with the geometry column encoded as WKB or WKT) 
- Use a Spark spartial library (Sedona, GeoMesa, Mosaic, Geode, etc) to convert it to a spatial Spark DataFrame (i.e. geometry column encoded as a UDT)
- Do stuff
- Write out to (Geo)Parquet
- Read (Geo)Parquet into `sds_data_model`
- Rasterise
- Do stuff
- Write out to zarr   
