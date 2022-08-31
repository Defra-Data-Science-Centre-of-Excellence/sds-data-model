"""Logging decorator."""
from functools import wraps
from inspect import currentframe, getouterframes, signature
from logging import INFO, Formatter, StreamHandler, getLogger
from re import search
from types import FrameType, FunctionType
from typing import Any, Callable, Dict, Optional, Tuple

# create logger and set level to info
logger = getLogger("sds")
logger.setLevel(INFO)

# create stream handler and set level to info
stream_handler = StreamHandler()
stream_handler.setLevel(INFO)

# create formatter
formatter = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# add formatter to stream handler
stream_handler.setFormatter(formatter)

# add stream handler to logger
logger.addHandler(stream_handler)


def _get_anonymous_function_string(
    func_name: str,
    frame: Optional[FrameType],
) -> Optional[str]:
    """Get the input code arguments to a function as a string.

    Args:
        func_name (str): _description_
        frame (Optional[FrameType]): _description_

    Raises:
        ValueError: _description_
        ValueError: _description_
        ValueError: _description_

    Returns:
        Optional[str]: _description_
    """
    if not frame:
        raise ValueError("Not possible to return current frame.")
    code_input = getouterframes(frame, 100)
    code_context = code_input[5].code_context
    if not code_context:
        raise ValueError("FrameInfo 5 has no `code_context`")
    code_context_string = "".join(code_context)
    function_call_string_matches = search(
        # "\." =            a literal dot
        # "{func_name}" =   the name of the function
        # "\(" =            a literal opening bracket
        # "\s*" =           0 or more whitespace characters, e.g. space, newline, etc
        # "(" =             begin capture group
        # "[\w|\W]+" =      1 or more word (\w) or non-word (\W) characters
        # "[\)|\"|\']" =    a literal closing bracket, double, or single quote mark
        # ")" =             end capture group
        # "\s*" =           0 or more whitespace characters, e.g. space, newline, etc
        # "\)" =            a literal closing bracket
        # "\s*" =           0 or more whitespace characters, e.g. space, newline, etc
        # "\." =            a literal dot
        rf"\.{func_name}\(\s*([\w|\W]+[\)|\"|\'])\s*\)\s*\.",
        code_context_string,
    )
    if not function_call_string_matches:
        raise ValueError(f"`{func_name}` not found in `code_context`")
    else:
        function_call_strings = function_call_string_matches.group(1)
        return function_call_strings


def _get_parameter_and_argument_tuples(
    func: Callable,
    *args: str,
    **kwargs: Dict[str, Any],
) -> Tuple[Tuple[str, str], ...]:
    """# TODO.

    Args:
        func (Callable): _description_
        *args (str): _description_.
        **kwargs (Dict[str, Any]): _description_.

    Returns:
        Tuple[Tuple[str, str], ...]: _description_
    """
    func_signature = signature(func)
    bound_arguments = func_signature.bind_partial(*args, **kwargs)
    bound_arguments.apply_defaults()
    return tuple(
        (parameter, argument)
        for parameter, argument in bound_arguments.arguments.items()
    )


def _format_parameter_and_argument(
    parameter: str,
    argument: str,
    func: Callable,
) -> str:
    """# TODO.

    Args:
        parameter (str): _description_
        argument (str): _description_
        func (Callable): _description_

    Returns:
        str: _description_
    """
    if isinstance(argument, FunctionType):
        return f"""{parameter}={
            _get_anonymous_function_string(func.__name__, currentframe())
        }"""
    elif parameter in ("self", "cls"):
        return f"{parameter}"
    else:
        return f"{parameter}={argument}"


def _format_parameter_and_argument_tuples(
    parameter_and_argument_tuples: Tuple[Tuple[str, Any], ...],
    func: Callable,
) -> str:
    """# TODO.

    Args:
        parameter_and_argument_tuples (Tuple[Tuple[str, Any], ...]): _description_
        func (Callable): _description_

    Returns:
        str: _description_
    """
    return ",\n\t".join(
        _format_parameter_and_argument(parameter, argument, func)
        for parameter, argument in parameter_and_argument_tuples
    )


def _format_function_string(
    func_name: str,
    parameter_and_argument_string: str,
) -> str:
    """# TODO.

    Args:
        func_name (str): _description_
        parameter_and_argument_string (str): _description_

    Returns:
        str: _description_
    """
    return f"""
    {func_name}(
        {parameter_and_argument_string}
    )
    """


def stringify_callable(
    func: Callable,
    *args: str,
    **kwargs: Dict[str, Any],
) -> str:
    """# TODO.

    Args:
        func (Callable): _description_
        *args (str): _description_.
        **kwargs (Dict[str, Any]): _description_.

    Returns:
        str: _description_
    """
    parameter_and_argument_tuples = _get_parameter_and_argument_tuples(
        func, *args, **kwargs
    )
    parameter_and_argument_string = _format_parameter_and_argument_tuples(
        parameter_and_argument_tuples=parameter_and_argument_tuples,
        func=func,
    )
    return _format_function_string(
        func_name=func.__qualname__,
        parameter_and_argument_string=parameter_and_argument_string,
    )


def log(func: Callable) -> Callable:
    """# TODO.

    Args:
        func (Callable): _description_

    Returns:
        Callable: _description_
    """

    @wraps(func)
    def wrapper(
        *args: str,
        **kwargs: Dict[str, Any],
    ) -> Callable:
        """# TODO.

        Args:
            *args (str): _description_.
            **kwargs (Dict[str, Any]): _description_.

        Raises:
            Exception: _description_

        Returns:
            Callable: _description_
        """
        try:
            result: Callable = func(*args, **kwargs)
            callable_string = stringify_callable(func, *args, **kwargs)
            logger.info(f"{callable_string}")
            return result
        except Exception as e:
            callable_string = stringify_callable(func, *args, **kwargs)
            logger.exception(f"{callable_string} raised exception: {str(e)}")
            raise e

    return wrapper
