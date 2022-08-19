from ctypes import Union
from functools import wraps
from inspect import currentframe, getouterframes, signature, types
from logging import getLogger, INFO, StreamHandler, Formatter
from re import search
from types import FunctionType
from typing import Any, Callable, Tuple

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
    frame: types.FrameType,
) -> str:
    """Get the input code arguments to a function as a string."""
    code_input = getouterframes(frame, 100)
    code_context_string = "".join(code_input[5].code_context)
    function_call_string_matches = search(
        rf"\.{func_name}\(\s*([\w|\W]+\)?)\s*\)", code_context_string
    )
    if not function_call_string_matches:
        print(code_input)
    else:
        function_call_strings = function_call_string_matches.group(1)
        return function_call_strings


def _get_parameter_and_argument_tuples(
    func: Callable,
    *args,
    **kwargs,
) -> Tuple[Tuple[str, Any], ...]:
    func_signature = signature(func)
    bound_arguments = func_signature.bind_partial(*args, **kwargs)
    bound_arguments.apply_defaults()
    return tuple(
        (parameter, argument)
        for parameter, argument in bound_arguments.arguments.items()
    )


def _format_parameter_and_argument(
    parameter: str,
    argument: Any,
    func: Callable,
) -> str:
    if isinstance(argument, FunctionType):
        return f"{parameter}={_get_anonymous_function_string(func.__name__, currentframe())}"
    elif parameter in ("self", "cls"):
        return f"{parameter}"
    else:
        return f"{parameter}={argument}"


def _format_parameter_and_argument_tuples(
    parameter_and_argument_tuples: Tuple[Tuple[str, Any], ...],
    func: Callable,
) -> str:
    return ",\n\t".join(
        _format_parameter_and_argument(parameter, argument, func)
        for parameter, argument in parameter_and_argument_tuples
    )


def _format_function_string(func_name: str, parameter_and_argument_string: str) -> str:
    return f"""
    {func_name}(
        {parameter_and_argument_string}
    )
    """


def stringify_callable(func: Callable, *args, **kwargs) -> str:
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


def log(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            callable_string = stringify_callable(func, *args, **kwargs)
            logger.info(f"{callable_string}")
            return result
        except Exception as e:
            callable_string = stringify_callable(func, *args, **kwargs)
            logger.exception(f"{callable_string} raised exception: {str(e)}")
            raise e

    return wrapper
