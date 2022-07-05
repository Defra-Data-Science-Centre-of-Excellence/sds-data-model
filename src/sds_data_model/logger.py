from functools import wraps
from logging import basicConfig, info, INFO, exception
from inspect import BoundArguments, signature
from typing import Callable

basicConfig(format="%(levelname)s:%(asctime)s:%(message)s", level=INFO)


def _get_arguments(func: Callable, *args, **kwargs) -> BoundArguments:
    func_signature = signature(func)
    bound_arguments = func_signature.bind_partial(*args, **kwargs)
    bound_arguments.apply_defaults()
    return bound_arguments.arguments


def log(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            arguments = _get_arguments(func, *args, **kwargs)
            info(f"{func.__qualname__} called with arguments: {arguments}")
            return result
        except Exception as e:
            arguments = _get_arguments(func, *args, **kwargs)
            exception(
                f"{func.__qualname__} called with arguments: {arguments} raised exception: {str(e)}"
            )
            raise e

    return wrapper
