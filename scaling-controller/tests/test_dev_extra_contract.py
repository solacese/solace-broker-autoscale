"""Guards the `dev` extra contract (P2.8).

The assignment-service tests (`test_assignment.py`) depend on FastAPI and guard their import with
`pytest.importorskip("fastapi")`. That is the right degradation for a bare checkout, but it means a
safety-relevant suite would *silently skip* - showing green - if FastAPI ever fell out of the `dev`
extra. Other unit tests construct the native SMF event client. CI installs the full customer package
plus `dev`, so both dependencies must be present and those tests must RUN, not skip.

This module deliberately does NOT use `importorskip`, so it always runs. When it detects that the
`dev` extra is installed (using `ruff`, which ships only in the `dev` extra, as the sentinel), it
asserts FastAPI is importable. A broken `dev` extra therefore turns red here instead of hiding as a
skip in the assignment suite.
"""

from __future__ import annotations

import importlib.util

import pytest

#: Packages that ship ONLY in the `dev` extra. If any is importable, we are in a dev/CI environment
#: and the full `dev` extra - including fastapi - must be present.
_DEV_SENTINELS = ("ruff", "mypy")


def _dev_extra_installed() -> bool:
    return any(importlib.util.find_spec(name) is not None for name in _DEV_SENTINELS)


@pytest.mark.skipif(not _dev_extra_installed(),
                    reason="dev extra not installed (bare checkout); assignment tests degrade via importorskip")
def test_customer_dependencies_present_when_dev_extra_installed() -> None:
    """CI's full customer install must run service and native-SMF tests instead of skipping/failing."""
    for package, extra in (("fastapi", "service"), ("solace", "smf")):
        assert importlib.util.find_spec(package) is not None, (
            f"the `dev` extra is installed but {package} is missing; install the `{extra}` "
            "customer extra alongside `dev` in CI"
        )
