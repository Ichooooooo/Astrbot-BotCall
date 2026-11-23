"""Run the control panel scenarios with WSL Node: mock plugin-page bridge, no listener, no network."""

import base64
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
from node_runtime import node_runtime

NODE = node_runtime()


def main():
    def encode(path):
        return base64.b64encode((ROOT / path).read_bytes()).decode()

    config = {
        "source": encode("pages/control/control.js"),
        "html": encode("pages/control/index.html"),
        "css": encode("pages/control/control.css"),
        "fixture": encode("tests/control_settings_fixture.json"),
        "tests": encode("tests/test_control_page.mjs"),
    }
    # Pass original module bytes through stdin, with no temporary source tree.
    bootstrap = """
let input = "";
for await (const chunk of process.stdin) input += chunk;
const config = JSON.parse(input);
const text = key => Buffer.from(config[key], "base64").toString("utf8");
globalThis.controlPageSource = text("source");
globalThis.controlPageHtml = text("html");
globalThis.controlPageCss = text("css");
globalThis.controlPageFixture = text("fixture");
await import("data:text/javascript;base64," + config.tests);
"""
    subprocess.run(
        [NODE, "--input-type=module", "-e", bootstrap],
        input=json.dumps(config),
        text=True,
        encoding="utf-8",
        check=True,
    )


if __name__ == "__main__":
    main()
