from getpass import getuser
from pathlib import Path
from re import sub

from graphviz import Digraph, Source
from pytest import fixture

from sds_data_model.vector import VectorLayer

@fixture
def expected_dag(shared_datadir: Path) -> str:
    """The DAG we expect our function to generate."""
    path_to_dot_file = shared_datadir / "ramsar_dag.dot"
    dag: str = Source.from_file(path_to_dot_file).source
    return dag
