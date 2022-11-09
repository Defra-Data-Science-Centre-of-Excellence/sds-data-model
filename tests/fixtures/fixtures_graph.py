"""Fixtures for graph tests."""

from pathlib import Path

from graphviz import Source
from pytest import fixture


@fixture
def expected_dag(shared_datadir: Path) -> str:
    """The DAG we expect our function to generate."""
    path_to_dot_file = shared_datadir / "ramsar_dag.dot"
    dag: str = Source.from_file(path_to_dot_file).source
    return dag
