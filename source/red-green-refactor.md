# Red-Green-Refactor

I've mentioned the Red-Green-Refactor testing strategy a few times, claiming that it's just a more explicit version of what we're already doing when we develop code, but the discussion has been quite abstract: what does it actually look like in practice?

Well, I'm currently working on [Implement transformation history as a DAG](https://github.com/Defra-Data-Science-Centre-of-Excellence/sds-data-model/issues/56), and I thought this might be as good an opportunity as any to practice what I preach.

## Goal

So, first things first, what am I trying to do?

I want to visualise the process of reading vector data and associated metadata from disc and transforming it into our common raster specification. I've decided that the best way to do this is to generate a Directed Acyclic Graph (DAG) which captures the data states and transformations as nodes with edges indicating the flow from one to another.

I will use flowchart convention of representing the beginning and end of the process with "terminal" nodes (i.e. oval nodes), "process" nodes (i.e. rectangular nodes) to represent transformations, and "input/output" nodes (i.e. rhomboid nodes) to represent data states. For my purposes, the "terminal" nodes will represent data on disk, the "process" nodes will represent functions and methods, and the "input/output" nodes will represent data in memory.

## A simple example for testing

The simplest example I can think of is reading data and metadata into a `VectorLayer`, converting this to a `TiledVectorLayer`, then converting that to a `xarray.DataArray` as a mask. i.e.:

```python
from sds_data_model.vector import VectorLayer

vector_layer = VectorLayer.from_files(
    data_path="tests/data/Ramsar__England__.zip",
    metadata_path="tests/data/Ramsar__England__.xml"
)

tiled_vector_layer = vector_layer.to_tiles()

data_array = tiled_vector_layer.to_data_array_as_mask()
```

So, what would the DAG for this look like?

I came up with the following in `mermaid`:

```mermaid
graph TD;
 data_path([<dl><dt>data input:</dt><dd>tests/data/Ramsar__England__.zip</dd></dl>]) --> from_files[<dl><dt>function:</dt><dd>VectorLayer.from_files</dd></dl>]
 metadata_path([<dl><dt>metadata input:</dt><dd>tests/data/Ramsar__England__.xml</dd></dl>]) --> from_files[<dl><dt>function:</dt><dd>VectorLayer.from_files</dd></dl>]
 from_files[<dl><dt>function:</dt><dd>VectorLayer.from_files</dd></dl>] --> vector_layer[/<dl><dt>output:</dt><dd>VectorLayer</dd></dl>/]
 vector_layer[/<dl><dt>output:</dt><dd>VectorLayer</dd></dl>/] --> to_tiles[<dl><dt>function:</dt><dd>VectorLayer.to_tiles</dd></dl>]
 to_tiles[<dl><dt>function:</dt><dd>VectorLayer.to_tiles</dd></dl>] --> tiled_vector_layer[/<dl><dt>output:</dt><dd>TiledVectorLayer</dd></dl>/]
 tiled_vector_layer[/<dl><dt>output:</dt><dd>TiledVectorLayer</dd></dl>/] --> to_data_array_as_mask[<dl><dt>function:</dt><dd>TiledVectorLayer.to_data_array_as_mask</dd></dl>]
 to_data_array_as_mask[<dl><dt>function:</dt><dd>TiledVectorLayer.to_data_array_as_mask</dd></dl>] --> data_array[/<dl><dt>output:</dt><dd>xarray.DataArray</dd></dl>/]
```
