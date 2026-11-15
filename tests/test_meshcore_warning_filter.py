"""Keep the SDK-specific Python 3.14 exception narrow and parseable."""

import sys
import tomllib
import warnings
from pathlib import Path

import pytest
from _pytest.config import parse_warning_filter

_MESSAGE = "'asyncio.iscoroutinefunction' is deprecated and slated for removal"
_CONFIG = Path(__file__).resolve().parents[1] / "pyproject.toml"


def _apply_filters():
    filters = tomllib.loads(_CONFIG.read_text())["tool"]["pytest"]["ini_options"][
        "filterwarnings"
    ]
    for rule in filters:
        warnings.filterwarnings(*parse_warning_filter(rule, escape=False))


def test_sdk_deprecation_is_scoped_to_package():
    with warnings.catch_warnings(record=True) as recorded:
        warnings.resetwarnings()
        _apply_filters()
        warnings.warn_explicit(
            _MESSAGE, DeprecationWarning, "events.py", 1, module="meshcore.events"
        )
        assert not recorded
        for module, message, category in (
            ("medre.adapters.meshcore", _MESSAGE, DeprecationWarning),
            ("meshcore_other.events", _MESSAGE, DeprecationWarning),
            ("meshcore.events", "different deprecated API", DeprecationWarning),
            ("meshcore.events", _MESSAGE, RuntimeWarning),
        ):
            with pytest.raises(category):
                warnings.warn_explicit(message, category, "events.py", 1, module=module)


@pytest.mark.skipif(sys.version_info < (3, 14), reason="requires CPython 3.14 warning")
def test_actual_asyncio_deprecation_at_sdk_callsite():
    source = "import asyncio\nasyncio.iscoroutinefunction(lambda: None)"
    with warnings.catch_warnings(record=True) as recorded:
        warnings.resetwarnings()
        _apply_filters()
        exec(
            source, {"__name__": "meshcore.events"}
        )  # nosec B102 # Literal SDK warning probe.
        assert not recorded
        with pytest.raises(DeprecationWarning):
            exec(
                source, {"__name__": "medre.adapters.meshcore"}
            )  # nosec B102 # Literal warning probe.
