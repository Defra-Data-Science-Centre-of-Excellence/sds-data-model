# Linting

This project uses [`flake8`](https://flake8.pycqa.org/en/latest/) for linting. 

`flake8` is a linting tool for "style guide enforcement". It is an aggregator, pulling together multiple tools to flag various bugs, errors and constructs within python code.

## What?

Linting tools are typically used in the development of code/software, in order to catch errors and allow the author(s) to rectify issues as they are introduced to the codebase.

### Plugins/Extensions

There are many 3rd party extensions avaliable for use with flake8 (e.g. [^1],[^2]), each aimed at linting or checking code for a specific type of bug/error. 

A few examples include: 

1. [`flake8-docstrings`](https://github.com/pycqa/flake8-docstrings) - a module to check for code conformity to [pydocstyle](https://github.com/pycqa/pydocstyle).
1. [`flake8-bandit`](https://openbase.com/python/flake8-bandit) - a wrapper around the [bandit](https://pypi.org/project/bandit/) security testing module.
1. [`flake8-bugbear`](https://github.com/PyCQA/flake8-bugbear) - a module to check for likely bugs and design problems in code. 

## Why?

When implemented and configured in a way suited to the developers and the scope of a project, linters can automate time-consuming code checking, and also enforce consistency across a codebase.  

Finding errors/issues with code earlier on in development can also save time and resources in the long-run. 

## How?

There are several ways you can run `flake8`:

1. ad-hoc via the command-line
1. ad-hoc via your IDE
1. After you push to GitHub, as part of a CI pipeline

### Via the command-line

Once installed, flake8 can be run on individual files `flake8 path/to/code/to/check.py` or on directories `flake8 path/to/code`. 

### In your IDE (VS Code)

Linting can be run in VS Code from the command palette, selecting **Python: Run Linting**. Any issues that the linter finds are displayed in the **Problems** tab of the command panel. To set `flake8` as a linter, select **Python: select linter** from the command palette, and choose `flake8` from the list.  

Your `settings.json` file can be configured to select/ignore certain `flake8` error codes. For example the following can be added:

``` json
"python.linting.flake8Args": ["--ignore=E303"]
```
By default, the `settings.json` file will have "Linting in general" and "Linting on file save" set as true.

### As part of a CI pipeline

This project uses GitHub Actions for Continuous Integration. The CI workflow is defined in `.github/workflows/python-package.yml`. This workflow will run the  `lint` job, whenever code is pushed to the `develop` branch or pull requests into `develop` are triggered.

It appears that `flake8` cannot be configured with a `pyproject.toml` configuration file (used by many other developer tools). Workarounds such as [`Flake8-pyproject`](https://pypi.org/project/Flake8-pyproject/) have been developed if this approach is desired. Otherwise a `.flake8` config file within the directory/repo is required.  


[^1]: https://julien.danjou.info/the-best-flake8-extensions/
[^2]: https://github.com/DmytroLitvinov/awesome-flake8-extensions