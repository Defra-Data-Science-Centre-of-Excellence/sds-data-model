from functools import partial
from re import search
from typing import List, Optional, Tuple

from graphviz import Digraph


def _add_node(
    graph: Digraph,
    *args,
    **kwargs,
) -> None:
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
    _add_terminal_node(
        graph,
        "data_path",
        label=f"data input:\n{data_path}",
    )


def _add_metadata_path_node(
    graph: Digraph,
    metadata_path: str,
) -> None:
    _add_terminal_node(
        graph,
        "metadata_path",
        label=f"metadata input:\n{metadata_path}",
    )


def _add_function_node(
    graph: Digraph,
    function_name: str,
) -> None:
    _add_process_node(
        graph,
        function_name,
        label=f"function:\n{function_name}",
    )


def _add_edges(
    graph: Digraph,
    edges: List[Tuple[str, str]],
) -> None:
    for edge in edges:
        graph.edge(*edge)


def _add_output_node(
    graph: Digraph,
    output_name: str,
) -> None:
    _add_input_output_node(
        graph,
        output_name,
        label=f"output:\n{output_name}",
    )


def initialise_graph(
    data_path: str, metadata_path: Optional[str], class_name: str
) -> Digraph:
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
    output_class_name: str,
) -> Digraph:
    input_node = _get_input_node(
        graph=graph,
    )

    _method = f"{input_node}.{method}"

    _add_function_node(
        graph=graph,
        function_name=_method,
    )

    _add_output_node(
        graph=graph,
        output_name=output_class_name,
    )

    _add_edges(
        graph=graph,
        edges=[
            (input_node, _method),
            (_method, output_class_name),
        ],
    )

    return graph
