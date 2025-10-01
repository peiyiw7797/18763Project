"""Compatibility helpers for running PySpark on newer Python versions."""

from __future__ import annotations

import sys
import types
from typing import Any


def ensure_typing_submodules() -> None:
    """Create deprecated typing.* submodules required by older PySpark builds."""

    try:
        import typing  # noqa: F401  # pylint: disable=import-outside-toplevel
    except Exception:  # pragma: no cover - typing missing is fatal elsewhere
        return

    # typing.io and typing.re were dropped in Python 3.13; provide shims that expose
    # IO/TextIO/AnyStr and Pattern/Match to satisfy PySpark's imports.
    _install_typing_io()
    _install_typing_re()


def _install_typing_io() -> None:
    try:
        import typing.io  # type: ignore[attr-defined]
        return
    except ModuleNotFoundError:
        pass

    import typing as _typing

    module = types.ModuleType("typing.io")
    for name in ("IO", "TextIO", "BinaryIO", "AnyStr"):
        value = getattr(_typing, name, Any)
        setattr(module, name, value)
    sys.modules["typing.io"] = module


def _install_typing_re() -> None:
    try:
        import typing.re  # type: ignore[attr-defined]
        return
    except ModuleNotFoundError:
        pass

    import typing as _typing

    module = types.ModuleType("typing.re")
    for name in ("Pattern", "Match", "AnyStr"):
        value = getattr(_typing, name, Any)
        setattr(module, name, value)
    sys.modules["typing.re"] = module


__all__ = ["ensure_typing_submodules"]

