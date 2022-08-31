from getpass import getuser
from pathlib import Path
from re import sub

from graphviz import Digraph, Source
from pytest import fixture

from sds_data_model.vector import VectorLayer


@fixture
def expected_dag(shared_datadir: Path) -> str:
    path_to_dot_file = shared_datadir / "ramsar_dag.dot"
    dag: str = Source.from_file(path_to_dot_file).source
    return dag


def test_graph(shared_datadir: Path, expected_dag: Digraph) -> None:
    data_path = str(shared_datadir / "Ramsar__England__.zip")
    metadata_path = str(shared_datadir / "Ramsar__England__.xml")

    vector_layer = VectorLayer.from_files(
        data_path=data_path, metadata_path=metadata_path
    )

    tiled_vector_layer = vector_layer.to_tiles()

    current_user = getuser()

    received_dag = sub(
        pattern=rf"/tmp/pytest-of-{current_user}/pytest-\d+/test_graph\d+/",
        repl=r"tests/",
        string=tiled_vector_layer.graph.source,
    )

    assert received_dag == expected_dag
