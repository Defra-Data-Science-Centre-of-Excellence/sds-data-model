# Testing

## What?

This project uses [`pytest`](https://pycqa.github.io/isort/index.html) and [`coverage`](https://black.readthedocs.io/en/stable/index.html) for formatting.

`pytest` is "a Python utility / library to sort imports alphabetically, and automatically separated into sections and by type."[^1]

`coverage` is a [PEP 8](https://peps.python.org/pep-0008/), Python's official style guide, "compliant opinionated formatter with its own style.".[^2]

## Why?

Testing allows to test different sections of the code in a formalised and systematic manner to reduce errors and bugs. Python comes with a default testing framework called `unittest`. The advantage of using `pytest` over `unittest` is that the test reports are more comprehensive showcasing where the tests have failed and which files were run. It is also easy to setup tests with `pytest` than with `unittest` making it easier to use and less code to write. 



Similarly, there are quite a few import sorters but `isort` is my choice because it's maintained by the [Python Code Quality Authority](https://meta.pycqa.org/) and it integrates well with `black`.[^3]

### Do we really need a separate formatter for imports?

Yes, as, at the moment at least, the `black` maintainers have no plans to offer import sorting.[^4]

## How?

There are several ways you can run these formatters:

1. ad-hoc via the command-line
1. ad-hoc via your IDE
1. After you push to GitHub, as part of a CI pipeline


### Via the command-line

You can run `pytest` whenever you want from the command line using:

```sh
pytest
```
This will run all your tests.

```sh
pytest --fixtures [testpath]
```
This will run tests defined by pytest fixtures allowing to run tests that have been grouped together.

```sh
poetry run pytest .
```

This will run the version of `pytest` installed within your current `poetry virtualenv`. It will also run all of the tests. 

However, we'll generally be running `pytest` via `nox`, a test runner that will provide a consistent interface for all our automated checks and tests:

```sh
poetry run nox -s pytest
```

This will run the `pytest` session defined in the project `noxfile.py` file.


## In VS Code

```Json
{
    "python.testing.pytestArgs": [
        "tests"
    ],
    "python.testing.unittestEnabled": false,
    "python.testing.pytestEnabled": true
}
```

You can use the Testing tab to configure tests you have written which sorts the tests by repository, folder, file and each test function you have written as an individual test that can be run by pressing the "Run Test" button. Allowing to run individual parts, groups or all of the tests easily. 



## As part of a CI pipeline

This project uses GitHub Actions for Continuous Integration. The CI workflow is defined in `.github/workflows/python-package.yml`. This workflow will run  `pytest` and `coverage` sessions as part of the `test` job, whenever code is pushed to the `develop` branch or pull requests into `develop` are triggered.

[^1]: https://pycqa.github.io/isort/index.html
[^2]: https://black.readthedocs.io/en/stable/the_black_code_style/index.html
[^3]: <https://pycqa.github.io/isort/docs/configuration/black_compatibility.html>
[^4]: <https://github.com/psf/black/issues/333#issuecomment-1153637034>
[^5]: https://code.visualstudio.com/docs/python/editing#_formatting
[^6]: You can open your settings by selecting File > Preferences > Settings or by using the keyboard shortcut `Ctrl + ,`. You can then switch to the JSON view by clicking the icon of a file with an arrow in the top-right hand corner of the screen.
