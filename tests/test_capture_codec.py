"""Packet clocks, tail trimming and bounded diagnostics, without browser/host audio."""

from pathlib import Path
import subprocess
import unittest
from node_runtime import node_runtime
from bot_call.observability import (
    CAPTURE_CODEC_METRICS,
    client_metric_valid,
    timing_row,
    timing_summary,
)


class CaptureCodecTests(unittest.TestCase):
    def test_packet_variants(self):
        subprocess.run([node_runtime(), "tests/test_capture_worker.cjs"], check=True)

    def test_input_worklet(self):
        subprocess.run([node_runtime(), "tests/test_capture_input.cjs"], check=True)

    def test_diagnostic_bounds_and_summary(self):
        rows = []
        for key, maximum in CAPTURE_CODEC_METRICS.items():
            for value in (0, maximum):
                self.assertTrue(client_metric_valid(key, value), (key, value))
                row = dict(
                    call_id="synthetic",
                    generation_id="capture:x",
                    stage=key,
                    sequence=0,
                    start_ms=0,
                    elapsed_ms=value,
                    recorded_at=1,
                )
                self.assertIsNotNone(timing_row(row))
                rows.append(row)
            for value in (-1, maximum + 1, True, 1.5):
                self.assertFalse(client_metric_valid(key, value))
                self.assertIsNone(
                    timing_row(dict(stage=key, sequence=0, start_ms=0, elapsed_ms=value))
                )
        for stage in timing_summary(rows)["stages"]:
            self.assertIn("latest_value", stage)
            self.assertNotIn("median_ms", stage)
