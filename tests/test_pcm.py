"""Deterministic PCM reordering, deduplication and missing-block deadlines."""

import base64
from pathlib import Path
import subprocess
import unittest
from node_runtime import node_runtime


class PCMPlayerTests(unittest.TestCase):
    def test_order_bounds_and_cleanup(self):
        root = Path(__file__).resolve().parents[1]
        module = base64.b64encode((root / "bot_call/static/pcm.js").read_bytes()).decode()
        script = (
            (root / "tests/test_pcm.mjs")
            .read_text()
            .replace('"PCM_MODULE"', '"data:text/javascript;base64,' + module + '"')
        )
        subprocess.run([node_runtime(), "--input-type=module"], input=script, text=True, check=True)
