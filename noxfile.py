"""Nox sessions."""
import os
import sys
from itertools import chain
from pathlib import Path
from shutil import copytree, rmtree
from textwrap import dedent

import nox

try:
    from nox_poetry import Session, session
except ImportError:
    message = f"""\
    Nox failed to import the 'nox-poetry' package.

    Please install it using the following command:

    {sys.executable} -m pip install nox-poetry"""
    raise SystemExit(dedent(message)) from None


package = "sds_data_model"
python_versions = ["3.8"]
locations = "src", "tests", "noxfile.py"

nox.needs_version = ">= 2021.6.6"
nox.options.sessions = (
    "isort",
    "black",
    "blacken-docs",
    "lint",
    "mypy",
    "safety",
    "tests",
    "docs-build",
    # "pre-commit",
    # "typeguard",
    # "xdoctest",
)


# def activate_virtualenv_in_precommit_hooks(session: Session) -> None:
#     """Activate virtualenv in hooks installed by pre-commit.

#     This function patches git hooks installed by pre-commit to activate the
#     session's virtual environment. This allows pre-commit to locate hooks in
#     that environment when invoked from git.

#     Args:
#         session: The Session object.
#     """
#     assert session.bin is not None  # noqa: B101

#     virtualenv = session.env.get("VIRTUAL_ENV")
#     if virtualenv is None:
#         return

#     hookdir = Path(".git") / "hooks"
#     if not hookdir.is_dir():
#         return

#     for hook in hookdir.iterdir():
#         if hook.name.endswith(".sample") or not hook.is_file():
#             continue

#         text = hook.read_text()
#         bindir = repr(session.bin)[1:-1]  # strip quotes
#         if not (
#             Path("A") == Path("a") and bindir.lower() in text.lower() or bindir in text  # noqa: B950
#         ):
#             continue

#         lines = text.splitlines()
#         if not (lines[0].startswith("#!") and "python" in lines[0].lower()):
#             continue

#         header = dedent(
#             f"""\
#             import os
#             os.environ["VIRTUAL_ENV"] = {virtualenv!r}
#             os.environ["PATH"] = os.pathsep.join((
#                 {session.bin!r},
#                 os.environ.get("PATH", ""),
#             ))
#             """
#         )

#         lines.insert(1, header)
#         hook.write_text("\n".join(lines))


@session(python=python_versions, tags=["format", "local"])  # type: ignore[call-overload]  # noqa: B950
def isort(session: Session) -> None:
    """Sort imports with isort."""
    args = session.posargs or locations
    session.install("isort")
    session.run("isort", *args)


@session(python=python_versions, tags=["format", "local"])  # type: ignore[call-overload]  # noqa: B950
def black(session: Session) -> None:
    """Run black code formatter."""
    args = session.posargs or locations
    session.install("black")
    session.run("black", *args)


@session(name="blacken-docs", python=python_versions, tags=["format", "local"])  # type: ignore[call-overload]  # noqa: B950
def blacken_docs(session: Session) -> None:
    """Run black on docstring code blocks."""
    chained = chain.from_iterable(
        Path(location).rglob("*.py") if Path(location).is_dir() else (location,)
        for location in locations
    )
    _locations = tuple(str(path) for path in chained)
    args = session.posargs or _locations
    session.install("blacken-docs")
    session.run("blacken-docs", *args)


# @session(name="pre-commit", python="3.8")
# def precommit(session: Session) -> None:
#     """Lint using pre-commit."""
#     args = session.posargs or ["run", "--all-files", "--show-diff-on-failure"]
#     session.install(
#         "black",
#         "darglint",
#         "flake8",
#         "flake8-bandit",
#         "flake8-bugbear",
#         "flake8-docstrings",
#         "flake8-rst-docstrings",
#         "pep8-naming",
#         "pre-commit",
#         "pre-commit-hooks",
#         "pyupgrade",
#         "reorder-python-imports",
#     )
#     session.run("pre-commit", *args)
#     if args and args[0] == "install":
#         activate_virtualenv_in_precommit_hooks(session)


@session(python=python_versions, tags=["local"])  # type: ignore[call-overload]
def safety(session: Session) -> None:
    """Scan dependencies for insecure packages."""
    requirements = session.poetry.export_requirements()
    session.install("safety")
    session.run("safety", "check", "--full-report", f"--file={requirements}")


@session(python=python_versions, tags=["local"])  # type: ignore[call-overload]
def mypy(session: Session) -> None:
    """Type-check using mypy."""
    args = session.posargs or locations
    session.install(".")
    session.install("mypy", "pytest", "types-requests")
    session.run("mypy", *args)
    if not session.posargs:
        session.run("mypy", f"--python-executable={sys.executable}", "noxfile.py")


@session(python=python_versions, tags=["local"])  # type: ignore[call-overload]
def tests(session: Session) -> None:
    """Run the test suite."""
    session.install(".")
    session.install("coverage[toml]", "pytest", "pygments", "pytest-datadir", "chispa")
    try:
        session.run(
            "coverage", "run", "--parallel", "-m", "pytest", "-vv", *session.posargs
        )
    finally:
        if session.interactive:
            session.notify("coverage", posargs=[])


@session
def coverage(session: Session) -> None:
    """Produce the coverage report."""
    args = session.posargs or ["report"]

    session.install("coverage[toml]")

    if not session.posargs and any(Path().glob(".coverage.*")):
        session.run("coverage", "combine")

    session.run("coverage", *args)


# @session(python=python_versions)
# def typeguard(session: Session) -> None:
#     """Runtime type checking using Typeguard."""
#     session.install(".")
#     session.install("pytest", "typeguard", "pygments")
#     session.run("pytest", f"--typeguard-packages={package}", *session.posargs)


# @session(python=python_versions)
# def xdoctest(session: Session) -> None:
#     """Run examples with xdoctest."""
#     if session.posargs:
#         args = [package, *session.posargs]
#     else:
#         args = [f"--modname={package}", "--command=all"]
#         if "FORCE_COLOR" in os.environ:
#             args.append("--colored=1")

#     session.install(".")
#     session.install("xdoctest[colors]")
#     session.run("python", "-m", "xdoctest", *args)


@session(name="docs-build", python=python_versions)  # type: ignore[call-overload]
def docs_build(session: Session) -> None:
    """Build the documentation."""
    args = session.posargs or ["-M", "html", "source", "_build"]
    if not session.posargs and "FORCE_COLOR" in os.environ:
        args.insert(0, "--color")

    session.install(".")
    session.install("sphinx", "myst-parser", "piccolo_theme")

    build_dir = Path("_build")
    html_dir = Path("_build/html")
    output_dir = Path("docs")
    no_jekyll = output_dir / ".nojekyll"

    session.run("sphinx-build", *args)

    if output_dir.exists():
        rmtree(output_dir)

    copytree(html_dir, output_dir)
    no_jekyll.touch()

    rmtree(build_dir)


@session(python=python_versions, tags=["local"])  # type: ignore[call-overload]
def lint(session: Session) -> None:
    """Lint using flake8."""
    args = session.posargs or locations
    session.install(".")
    deps = [
        "flake8",
        "flake8-pyproject",
        "flake8-annotations",
        "flake8-bandit",
        "flake8-black",
        "flake8-bugbear",
        "flake8-docstrings",
        "flake8-isort",
        "darglint",
    ]
    session.install(*deps)
    session.run("flake8", *args)
