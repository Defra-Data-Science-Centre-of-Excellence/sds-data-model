# Red-Green-Refactor

I've mentioned the Red-Green-Refactor testing strategy a few times, claiming that it's just a more explicit version of what we're already doing when we develop code, but the discussion has been quite abstract: what does it actually look like in practice?

Well, I'm currently working on [Implement transformation history as a DAG](https://github.com/Defra-Data-Science-Centre-of-Excellence/sds-data-model/issues/56), and I thought this might be as good an opportunity as any to practice what I preach.

## Goal

So, first things first, what am I trying to do?

I want to visualise the process of reading vector data and associated metadata from disc and transforming it into our common raster specification. I've decided that the best way to do this is to generate a Directed Acyclic Graph (DAG) which captures the data states and transformations as nodes with edges indicating the flow from one to another.

I will use flowchart convention of representing the beginning and end of the process with "terminal" nodes (i.e. oval nodes), "process" nodes (i.e. rectangular nodes) to represent transformations, and "input/output" nodes (i.e. rhomboid nodes) to represent data states. For my purposes, the "terminal" nodes will represent data on disk, the "process" nodes will represent functions and methods, and the "input/output" nodes will represent data in memory.

## A simple example for testing

The simplest example I can think of is reading data and metadata into a `VectorLayer`, then convert it to a `TiledVectorLayer`:

```python
from sds_data_model.vector import VectorLayer

vector_layer = VectorLayer.from_files(
    data_path="tests/data/Ramsar__England__.zip",
    metadata_path="tests/data/Ramsar__England__.xml"
)

tiled_vector_layer = vector_layer.to_tiles()
```

## A DAG for this example

So, what would the DAG for this look like?

I came up with the following in `mermaid`:

```mermaid
graph TD;
 data_path([<dl><dt>data input:</dt><dd>tests/data/Ramsar__England__.zip</dd></dl>]) --> from_files[<dl><dt>function:</dt><dd>VectorLayer.from_files</dd></dl>]
 metadata_path([<dl><dt>metadata input:</dt><dd>tests/data/Ramsar__England__.xml</dd></dl>]) --> from_files[<dl><dt>function:</dt><dd>VectorLayer.from_files</dd></dl>]
 from_files[<dl><dt>function:</dt><dd>VectorLayer.from_files</dd></dl>] --> vector_layer[/<dl><dt>output:</dt><dd>VectorLayer</dd></dl>/]
 vector_layer[/<dl><dt>output:</dt><dd>VectorLayer</dd></dl>/] --> to_tiles[<dl><dt>function:</dt><dd>VectorLayer.to_tiles</dd></dl>]
 to_tiles[<dl><dt>function:</dt><dd>VectorLayer.to_tiles</dd></dl>] --> tiled_vector_layer[/<dl><dt>output:</dt><dd>TiledVectorLayer</dd></dl>/]
```

Which I then wrote in python using `graphviz`:

```python
from graphviz import Digraph

dag = Digraph()

dag.node("data_path", label="data input:\ntests/data/Ramsar__England__.zip", shape="oval")
dag.node("metadata_path", label="metadata input:\ntests/data/Ramsar__England__.xml", shape="oval")
dag.node("VectorLayer.from_files", label="function:\nVectorLayer.from_files", shape="box")
dag.node("VectorLayer", label="output:\nVectorLayer", shape="parallelogram")

dag.edge("data_path", "VectorLayer.from_files")
dag.edge("metadata_path", "VectorLayer.from_files")
dag.edge("VectorLayer.from_files", "VectorLayer")

dag.node("VectorLayer.to_tiles", label="function:\nVectorLayer.to_tiles", shape="box")
dag.node("TiledVectorLayer", label="output:\nTiledVectorLayer", shape="parallelogram")

dag.edge("VectorLayer", "VectorLayer.to_tiles")
dag.edge("VectorLayer.to_tiles", "TiledVectorLayer")

dag.save("tests/data/ramsar_dag.dot")
```

![ramsar_dag](../tests/data/ramsar_dag.dot)

## The Red

Now we have an expected output, we can write a test.

First, though, we need to access that expected output.

```python
from pathlib import Path

from graphviz import Digraph, Source
from pytest import fixture


@fixture
def expected_dag(shared_datadir: Path) -> Digraph:
    path_to_dot_file = shared_datadir / "ramsar_dag.dot"
    return Source.from_file(path_to_dot_file)
```

Here, I'm capturing the expected dag in a `pytest.fixture` and I'm using `pytest-datadir`.

Then, I write a test I know will fail:

```diff
from pathlib import Path

from graphviz import Digraph, Source
from pytest import fixture

+from sds_data_model.vector import VectorLayer

@fixture
def expected_dag(shared_datadir: Path) -> Digraph:
    path_to_dot_file = shared_datadir / "ramsar_dag.dot"
    return Source.from_file(path_to_dot_file)


+def test_graph(shared_datadir: Path, expected_dag: Digraph) -> None:
+    data_path = str(shared_datadir / "Ramsar__England__.zip")
+    metadata_path = str(shared_datadir / "Ramsar__England__.xml")
+
+    vector_layer = VectorLayer.from_files(
+        data_path=data_path, metadata_path=metadata_path
+    )
+
+    tiled_vector_layer = vector_layer.to_tiles()
+
+    assert tiled_vector_layer.graph == expected_dag
```

Which it does, with an `AttributeError` as the `'TiledVectorLayer' object has no attribute 'graph'`.

Perfect, now I just have to make this test pass!

## The Green
