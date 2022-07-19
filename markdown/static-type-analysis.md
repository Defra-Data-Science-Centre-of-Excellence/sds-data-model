# Static Type Analysis

## What?

Though Python is [dynamically typed](https://en.wikipedia.org/wiki/Type_system#DYNAMIC) language, it's possible to indicate the type of [function arguments and return values](https://peps.python.org/pep-0484/) or [variables](https://peps.python.org/pep-0526/) through type annotations.

For example, the type annotations on the following function indicate that it takes a string argument, called `name`, and returns a string.

```python
def greeting(name: str) -> str:
    return 'Hello ' + name
```

While, the type annotation on the following variable indicates that it is a list of strings.

```python
names: List[str]
```

These annotations can make it easier for other developers (and your future self) to understand what your code is supposed to do. However, they don't have any effect at runtime, so there's nothing to stop someone passing an integer to `greeting` or storing a single integer in `names`. [`mypy`](https://mypy.readthedocs.io/en/stable/#) goes some way towards addressing this problem by analysing your code as if it contained static types.

## Why?

The benefit of this is that `mypy` can catch instances where the declared type doesn't match the actual type or where an object of type A is being treated like type B.

For example, `mypy` would complain if, in your source code, `greeting` returned an integer:

```python
def greeting(name: str) -> str:
    return 1
```

If you passed an integer to `names`:

```python
names: List[str] = 1
```

Or if you passed a list of strings to `names` but then called a dictionary method on it:

```python
names: List[str] = ["Alan", "Betty", "Claude",]
names.keys()
```

However, one of the most useful things that `mypy` does is force you indicate optional values and then deal with the `None` case.

For example, say you wanted to re-write `greeting` so that it returned "Hello world" if no name was given, `mypy` would complain if you didn't make this clear on the function signature:

```python
def greeting(name: str = None) -> str:
    if name:
        return "Hello " + name
    else:
        return "Hello world" 
```

Should really be:

```python
def greeting(name: Optional[str] = None) -> str:
    if name:
        return "Hello " + name
    else:
        return "Hello world" 
```

Similarly, `mypy` will complain if you don't handle the `None` case of Optional objects. The following will be flagged because you can't call `.capitalize` on `None`.

```python
def greeting(name: Optional[str] = None) -> str:
    name = name.capitalize()
    if name:
        return "Hello " + name
    else:
        return "Hello world" 
```

### Do we need `mypy` if we're using other linters?

Yes, though `mypy` could be described as a linter; see, for example [the VS Code docs](https://code.visualstudio.com/docs/python/linting#_mypy). I would argue that `mypy` is more specific and therefore more narrowly useful than a general linter. Where linters highlight common errors and bad practices, mypy focuses on the correct declaration and usage of types. `mypy` therefore augments other linters instead of replacing them.

## How?

There are several ways you can run `mypy`:

1. ad-hoc via the command-line
1. ad-hoc via your IDE
1. After you push to GitHub, as part of a CI pipeline
1. Before you push to GitHub, via git hooks

For now, I'm just going to cover the first three.

### Via the command-line

You can run `mypy` whenever you want from the command line using:

```sh
poetry run mypy .
```

This will run the version of `mypy` installed within your current `poetry virtualenv` on all the files in the current folder and subfolder.

However, we'll generally be running `mypy` via `nox`, a test runner that will provide a consistent interface for all our automated checks and tests:

```sh
poetry run nox -s mypy
```

This will run the `mypy` session defined in the project `noxfile.py` file.

## In your IDE (VS Code)

You can tell VS Code to [use `mypy` to lint python code](https://code.visualstudio.com/docs/python/linting#_mypy) in your setting:[^1]

```json
{
    // other settings
    "python.linting.enabled": true,
    "python.linting.mypyEnabled": true,
    // other settings
}
```

You can then use `mypy` to format python files by opening the command palette by pressing `Ctrl + Shift + P` and selecting "Python: Run Linting".

It's worth adding [`--ignore-missing-imports`](https://mypy.readthedocs.io/en/stable/command_line.html#cmdoption-mypy-ignore-missing-imports) to the `mypyArgs` here to stop `mypy` complaining about missing type annotations of third party libraries.

```json
{
    // other settings
    "python.linting.enabled": true,
    "python.linting.mypyEnabled": true,
    "python.linting.mypyArgs": ["--ignore-missing-imports"]
    // other settings
}
```

## As part of a CI pipeline

This project uses GitHub Actions for Continuous Integration. The CI workflow is defined in `.github/workflows/python-package.yml`. This workflow will run the `mypy` sessions as part of the `static-type-analysis` job, whenever code is pushed to the `develop` branch or pull requests into `develop` are triggered.

[^1]: You can open your settings by selecting File > Preferences > Settings or by using the keyboard shortcut `Ctrl + ,`. You can then switch to the JSON view by clicking the icon of a file with an arrow in the top-right hand corner of the screen.

## `mypy` configuration

This project uses a `pyproject.toml` file for configuration. For information on how to configure `mypy` using this file,
see: [Using a pyproject.toml file](https://mypy.readthedocs.io/en/stable/config_file.html#using-a-pyproject-toml-file) in the `mypy` documentation.
