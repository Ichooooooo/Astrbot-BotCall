"""Resolve development paths for Python tools and browser checks."""

import os
from pathlib import Path

VENV_ROOT = Path(os.environ.get("BOTCALL_VENV_ROOT") or "~/.local/share/bot-call-venv").expanduser()
BROWSER = Path(
    os.environ.get("BOTCALL_BROWSER") or "~/.local/share/bot-call-browser/current/chrome"
).expanduser()
STATE_DIR = Path(os.environ.get("BOTCALL_STATE_DIR") or "~/.local/state/Bot-Call").expanduser()


def venv_python(version):
    return VENV_ROOT.absolute() / f"astrbot-{version}" / "bin/python"
