"""Sphinx configuration."""
from datetime import datetime


project = "SDS Data Model"
author = "Ed Fawcett-Taylor"
copyright = f"{datetime.now().year}, {author}"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
]
autodoc_typehints = "description"
