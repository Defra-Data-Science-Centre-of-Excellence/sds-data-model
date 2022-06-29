# Formatters

## What?

This project uses [`isort`](https://pycqa.github.io/isort/index.html) and [`black`](https://black.readthedocs.io/en/stable/index.html) for formatting.

`isort` is "a Python utility / library to sort imports alphabetically, and automatically separated into sections and by type."[^1]

`black` is "a PEP 8 compliant opinionated formatter with its own style.".[^2]

## Why?

Formatters ensure that code conforms to a specific style. This reduces idiosyncrasies and arguments about style and should lessen the cognitive overhead of involved in diving in to a new code base as the code should always look the same.

There are lots of python formatters out there, such as [`autopep9`](https://pypi.org/project/autopep8/) and [`yapf`](<https://github.com/google/yapf>), but`black` stands out because of it's lack of configurability: the less options there are, the less opportunities there are for disagreements and different approaches.

Similarly, there are quite a few import sorters but `isort` is my choice because it's maintained by the [Python Code Quality Authority](https://meta.pycqa.org/) and it integrates well with `black`.[^3]

### Do we really need a separate formatter for imports?

Yes, as, at the moment at least, the `black` maintainers have no plans to offer import sorting.[^4]

## How?

There are several ways you can run these formatters:

1. ad-hoc via the command-line
1. ad-hoc via your IDE
1. After you push to GitHub, as part of a CI pipeline
1. Before you push to GitHub, via git hooks

For now, I'm just going to cover the first three.

### Via the command-line

You can run `black` whenever you want from the command line using:

```sh
poetry run black .
```

This will run the version of `black` installed within your current `poetry virtualenv` on all the files in the current folder and subfolder.

However, we'll generally be running `black` via `nox`, a test runner that will provide a consistent interface for all our automated checks and tests:

```sh
poetry run nox -s black
```

This will run the `black` session defined in the project `noxfile.py` file.

Similarly, you can run `isort` using:

```sh
poetry run isort .
```

But we'll prefer:

```sh
poetry run nox -s isort
```

## In your IDE (VS Code)

You can tell VS Code to use `black`[^5] to format python code in your setting:

```json
{
    // other settings
    "python.formatting.provider": "black",
    // other settings
}
```

You can then use `black` to format python files by right clicking in the file and selecting "Format Document" or by using the keyboard shortcut `Shift + Alt + F`.

You can set VS Code to format your code with `black` whenever you save a file by adding:

```json
{
    // other settings
    "python.formatting.provider": "black",
    "[python]": {
        "editor.formatOnSave": true
  },
    // other settings
}
```

If you're feeling really brave, you can also set VS Code to autosave so that your files will reformatted automatically:

```json
{
    // other settings
    "files.autoSave": "afterDelay",
    "python.formatting.provider": "black",
    "[python]": {
        "editor.formatOnSave": true
  },
    // other settings
}
```

## As part of a CI pipeline

This project uses GitHub Action for Continuos Integration. The CI workflow is defined
in `.github/workflows/python-package.yml`. This workflow will run the full nox test
suite, include the `black` and `isort` sessions, whenever code is pushed to the
`develop` branch.

[^1]: https://pycqa.github.io/isort/index.html
[^2]: https://black.readthedocs.io/en/stable/the_black_code_style/index.html
[^3]: <https://pycqa.github.io/isort/docs/configuration/black_compatibility.html>
[^4]: <https://github.com/psf/black/issues/333#issuecomment-1153637034>
[^5]: https://code.visualstudio.com/docs/python/editing#_formatting
