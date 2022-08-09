from functools import partial
from typing import List, Tuple
from re import search

from graphviz import Digraph


def add_node(
    graph: Digraph,
    *args,
    **kwargs,
) -> None:
    return graph.node(*args, **kwargs)


add_terminal_node = partial(
    add_node,
    shape="oval",
)

add_process_node = partial(
    add_node,
    shape="box",
)

add_input_output_node = partial(
    add_node,
    shape="parallelogram",
)


def add_data_path_node(
    graph: Digraph,
    data_path: str,
) -> None:
    add_terminal_node(
        graph,
        "data_path",
        label=f"data input:\n{data_path}",
    )


def add_metadata_path_node(
    graph: Digraph,
    metadata_path: str,
) -> None:
    add_terminal_node(
        graph,
        "metadata_path",
        label=f"metadata input:\n{metadata_path}",
    )


def add_function_node(
    graph: Digraph,
    function_name: str,
) -> None:
    add_process_node(
        graph,
        function_name,
        label=f"function:\n{function_name}",
    )


def add_edges(
    graph: Digraph,
    edges: List[Tuple[str, str]],
) -> None:
    for edge in edges:
        graph.edge(*edge)


def add_output_node(
    graph: Digraph,
    output_name: str,
) -> None:
    add_input_output_node(
        graph,
        output_name,
        label=f"output:\n{output_name}",
    )


def initialise_graph(data_path: str, metadata_path: str, class_name: str) -> Digraph:
    graph = Digraph()

    add_data_path_node(
        graph=graph,
        data_path=data_path,
    )

    add_metadata_path_node(
        graph=graph,
        metadata_path=metadata_path,
    )

    constructor = f"{class_name}.from_files"

    add_function_node(
        graph=graph,
        function_name=constructor,
    )

    add_output_node(
        graph=graph,
        output_name=class_name,
    )

    add_edges(
        graph=graph,
        edges=[
            ("data_path", constructor),
            ("metadata_path", constructor),
            (constructor, class_name),
        ],
    )

    return graph


def get_input_node(
    graph: Digraph,
) -> str:
    return search(
        pattern=r"-> (\w+)\n$",
        string=graph.body[-1],
    ).group(1)


from inspect import currentframe, getouterframes, signature
from typing import Tuple
from re import search


# def get_full_signature(
#     context: int = 100,
#     outer_frame_index: int = 2,
# ) -> None:
#     current_frame = currentframe()
#     outer_frames = getouterframes(current_frame, context=context)
#     outer_frame = outer_frames[outer_frame_index]
#     outer_function_name = outer_frame.function
#     outer_frame_code = "".join(outer_frame.code_context)
#     outer_function_string = search(
#         rf"({outer_function_name}\(\s*[\w|\W]+?\)\s*\))", outer_frame_code
#     ).group(1)
#     print(outer_function_string)


def update_graph(
    graph: Digraph,
    method: str,
    output_class_name: str,
) -> Digraph:
    input_node = get_input_node(
        graph=graph,
    )

    _method = f"{input_node}.{method}"

    add_function_node(
        graph=graph,
        function_name=_method,
    )

    add_output_node(
        graph=graph,
        output_name=output_class_name,
    )

    add_edges(
        graph=graph,
        edges=[
            (input_node, _method),
            (_method, output_class_name),
        ],
    )

    return graph
