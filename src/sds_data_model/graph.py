"""Graph module."""
from functools import partial
from re import search
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from graphviz import Digraph


def _add_node(
    graph: Digraph,
    *args: str,
    **kwargs: Dict[str, Any],
) -> None:
    """Add a node to an exists graph.

    Args:
        graph (Digraph): An exists graph.
        args (Any): arguments to be passed to the underlying `Digraph.node` method.
        kwargs (Any): arguments to be passed to the underlying `Digraph.node` method.
    """
    graph.node(*args, **kwargs)


_add_terminal_node = partial(
    _add_node,
    shape="oval",
)

_add_process_node = partial(
    _add_node,
    shape="box",
)

_add_input_output_node = partial(
    _add_node,
    shape="parallelogram",
)


def _add_data_path_node(
    graph: Digraph,
    data_path: str,
) -> None:
    """Add a data path node to an existing graph.

    Args:
        graph (Digraph): An exists graph.
        data_path (str): filepath or url for data.
    """
    _add_terminal_node(
        graph,
        "data_path",
        label=f"data input:\n{data_path}",
    )


def _add_metadata_path_node(
    graph: Digraph,
    metadata_path: str,
) -> None:
    """Add a metadata path node to an existing graph.

    Args:
        graph (Digraph): An exists graph.
        metadata_path (str): filepath or url for metadata.
    """
    _add_terminal_node(
        graph,
        "metadata_path",
        label=f"metadata input:\n{metadata_path}",
    )


def _add_function_node(
    graph: Digraph,
    function_name: str,
) -> None:
    """Add a function node to an existing graph.

    Args:
        graph (Digraph): An exists graph.
        function_name (str): The name of the function.
    """
    _add_process_node(
        graph,
        function_name,
        label=f"function:\n{function_name}",
    )


def _add_edges(
    graph: Digraph,
    edges: List[Tuple[str, str]],
) -> None:
    """Add edges to an existing graph.

    Args:
        graph (Digraph): An exists graph.
        edges (List[Tuple[str, str]]): List list of edges, i.e. nodes to connect.
    """
    for edge in edges:
        graph.edge(*edge)


def _add_output_node(
    graph: Digraph,
    output_name: str,
    output_label: str,
) -> None:
    """Add an output node to an existing graph.

    Args:
        graph (Digraph): An exists graph.
        output_name (str): The name of the output.
    """
    _add_input_output_node(
        graph,
        output_name,
        label=f"output:\n{output_label}",
    )


def initialise_graph(
    data_path: str,
    metadata_path: Optional[str],
    class_name: str,
) -> Digraph:
    """Initialise a graph.

    Args:
        data_path (str): filepath or url for data.
        metadata_path (str): filepath or url for metadata.
        class_name (str): Name of the class that's created from the data and metadata.

    Returns:
        Digraph: A graph.
    """
    graph = Digraph()

    _add_data_path_node(
        graph=graph,
        data_path=data_path,
    )

    if metadata_path:
        _add_metadata_path_node(
            graph=graph,
            metadata_path=metadata_path,
        )

    constructor = f"{class_name}.from_files"

    _add_function_node(
        graph=graph,
        function_name=constructor,
    )

    _add_output_node(
        graph=graph,
        output_name=class_name,
        output_label=class_name,
    )

    if metadata_path:
        _add_edges(
            graph=graph,
            edges=[
                ("data_path", constructor),
                ("metadata_path", constructor),
                (constructor, class_name),
            ],
        )
    else:
        _add_edges(
            graph=graph,
            edges=[
                ("data_path", constructor),
                (constructor, class_name),
            ],
        )

    return graph


def _get_input_node(
    graph: Digraph,
) -> str:
    """Get the final node in an existing graph.

    Args:
        graph (Digraph): An exists graph.

    Raises:
        ValueError: If it's not possible to find the final node.

    Returns:
        str: The input node.
    """
    match = search(
        pattern=r"-> (\w+)\n$",
        string=graph.body[-1],
    )
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Unable to find final node in {graph.body[-1]}")


def update_graph(
    graph: Digraph,
    method: str,
    args: str,
    output_class_name: str,
) -> Digraph:
    """Update an existing graph.

    Args:
        graph (Digraph): An exists graph.
        method (str): The name of the method to add to the graph.
        output_class_name (str): The name of the class that's created by the method.

    Returns:
        Digraph: An updated graph.
    """
    input_node = _get_input_node(
        graph=graph,
    )
    
    _method = f"{input_node.split('_')[0]}.{method}(\n{args}\n)"

    _add_function_node(
        graph=graph,
        function_name=_method,
    )

    num = sum(
        1 if f"output:\n{output_class_name}" in line else 0 for line in graph.body
    )
    output_name = f"{output_class_name}_{num}"

    _add_output_node(
        graph=graph,
        output_name=output_name,
        output_label=output_class_name,
    )

    _add_edges(
        graph=graph,
        edges=[
            (input_node, _method),
            (_method, output_name),
        ],
    )

    return graph
