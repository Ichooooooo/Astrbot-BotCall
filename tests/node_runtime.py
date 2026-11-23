"""Resolve a Linux Node runtime for WSL page tests."""

from pathlib import Path
import shutil


def node_runtime():
    candidate = shutil.which("node")
    if candidate:
        with Path(candidate).open("rb") as executable:
            if executable.read(4) == b"\x7fELF":
                return candidate
    raise RuntimeError("Add Linux Node.js to PATH for the page tests")
