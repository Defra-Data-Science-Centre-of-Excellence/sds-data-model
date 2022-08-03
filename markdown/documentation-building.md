# Documentation building

## What?

Documentation for the package, including what the package is, how to use it, and more detailed information on each individual function. 

This project uses ['Sphinx'](https://www.sphinx-doc.org/en/master/) to create package documentation. 

Sphinx is a documentation generator that translates a set of plain text source files into various output formats (eg. HTML). 

The documentation process:

* Docstrings and examples are written in the package code. 
* ['autodoc'](https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html), a sphinx extension, imports the modules you are documenting, and pulls in the documentation from the docstrings contained in those modules. 
* ['napoleon'](https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html), another sphinx extension, is a pre-processor that parses the docstrings and converts them to reStructured-Text so that its in a suitable format for Sphinx to parse them. 
* `Sphinx`[^1] will then build HTML docs to a specified directory. For this package, the documentation is written to a separate Github branch, which is hosted on github-pages. 
* Github actions is set up so the documentation re-writes every time any of the documentation within the `/src` folder changes, and is pushed to the `develop` branch or pull requests into `develop` are triggered. It only updates when the `/src` folder changes, as this folder contains all the functions for the package. 


## Why?

Documentation writing is a vital part of writing code. 

Writing documentation allows other coders to quickly understand the package. Collating all documentation in the same place provides a quick overview without having to scroll through each function. This makes the package more accessible, and easy to use. 

It also saves huge amounts of time in the future, as you, or a future coder, can quickly get up to speed with what was written and why. 


## How?

There are several ways you can run the Sphinx documentation build:

1. ad-hoc via the command-line
1. After you push to GitHub, as part of a CI pipeline

### Via the command-line

You can build documentation at any point, to check what it looks like, by running build in the command line. 

```sh
poetry sphinx-build -b html sourcedir builddir
```

Replace `sourcedir` and `builddir` with the relevant folder (`/docs`). This will build html files within the sourcedir (`/docs`) folder specified. 

However, we'll generally be building documentation via `nox`, a test runner that will provide a consistent interface for all our automated checks and tests:

```sh
poetry run nox -s docs-build
```

This will run the `docs-build` session defined in the project `noxfile.py` file. 


## As part of a CI pipeline

This project uses GitHub Action for Continuous Integration. The CI workflow is defined in `.github/workflows/deploy-docs.yml`. This workflow will run the `docs-build` session as part of the  `build-and-deploy`  job, whenever code is edited in the `src` folder, and it is pushed to the `develop` branch or pull requests into `develop` are triggered.

[^1]: https://www.sphinx-doc.org/en/master/usage/quickstart.html

