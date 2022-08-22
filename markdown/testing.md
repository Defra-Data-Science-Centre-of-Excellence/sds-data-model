# Testing

## What?

This project uses [`pytest`](https://docs.pytest.org/en/7.1.x/) and [`coverage`](https://coverage.readthedocs.io/en/6.4.2/) for testing.

`pytest` is "a framework that makes it easy to write small, readable tests, and can scale to support complex functional testing for applications and libraries."[^1]

`coverage` is "a tool for measuring code coverage of Python programs. It monitors your program, noting which parts of the code have been executed, then analyzes the source to identify code that could have been executed but was not.".[^2]

## Why?

Testing allows us to test different sections of the code in a formalised and systematic manner to reduce errors and bugs. Python comes with a default testing framework called `unittest`. The advantage of using `pytest` over `unittest` is that the test reports are more comprehensive, showcasing where the tests have failed and which files were run. It is also easier to setup tests with `pytest` than with `unittest` making it easier to use and less code to write. 


## How?

There are several ways you can run these formatters:

1. Via the command-line
1. In VS Code
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
This will run tests defined by pytest fixtures[^3]. "Fixtures define the steps and data that constitute the arrange phase of a test. In pytest, they are functions you define that serve this purpose. They can also be used to define a test’s act phase; this is a powerful technique for designing more complex tests. We can tell pytest that a particular function is a fixture by decorating it with @pytest.fixture"[^4]

```sh
poetry run pytest .
```

This will run the version of `pytest` installed within your current `poetry virtualenv`. It will also run all of the tests. 

However, we'll generally be running `pytest` via `nox`, a test runner that will provide a consistent interface for all our automated checks and tests:

```sh
poetry run nox -s tests
```

This will run the `pytest` session defined in the project `noxfile.py` file. `coverage ` is also run as a part of the nox session. 


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

You can use the Testing[^5] tab to run tests you have written. It sorts the tests by repository, folder, file and test function. This allows you to run individual tests, groups of test, or all of the tests easily.  

## As part of a CI pipeline

This project uses GitHub Actions for Continuous Integration. The CI workflow is defined in `.github/workflows/python-package.yml`. This workflow will run  `pytest` and `coverage` sessions as part of the `test` job, whenever code is pushed to the `develop` branch or pull requests into `develop` are triggered.

[^1]: https://docs.pytest.org/en/7.1.x/
[^2]: https://coverage.readthedocs.io/en/6.4.2/
[^3]: https://docs.pytest.org/en/7.1.x/explanation/fixtures.html#about-fixtures
[^4]: https://docs.pytest.org/en/7.1.x/how-to/fixtures.html
[^5]: https://code.visualstudio.com/docs/python/testing
