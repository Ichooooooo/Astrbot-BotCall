"""Build a deterministic, allowlisted native-host candidate using stdlib only."""

import ast
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import stat
import zipfile
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bot_call.astrbot_compat import SUPPORTED_VERSIONS


ALLOWLIST = (
    "README.md",
    "LICENSE",
    "THIRD_PARTY_NOTICES.md",
    "docs/使用手册.md",
    "_conf_schema.json",
    "bot_call/__init__.py",
    "bot_call/access.py",
    "bot_call/benchmark.py",
    "bot_call/benchmark_run.py",
    "bot_call/benchmark_baseline.json",
    "pages/benchmark/index.html",
    "pages/benchmark/benchmark.css",
    "pages/benchmark/benchmark.js",
    ".astrbot-plugin/i18n/zh-CN.json",
    ".astrbot-plugin/i18n/en-US.json",
    "bot_call/astrbot_adapter.py",
    "bot_call/astrbot_compat.py",
    "bot_call/astrbot_audio.py",
    "bot_call/astrbot_tts.py",
    "bot_call/streaming_tts.py",
    "bot_call/speech.py",
    "bot_call/segments.py",
    "bot_call/barge_in.py",
    "bot_call/static/barge.js",
    "bot_call/phone_style.py",
    "bot_call/segment_runtime.py",
    "bot_call/push.py",
    "bot_call/failures.py",
    "bot_call/astrbot_context.py",
    "bot_call/astrbot_reply.py",
    "bot_call/astrbot_agent.py",
    "bot_call/tool_query.py",
    "bot_call/astrbot_review.py",
    "bot_call/audio.py",
    "bot_call/chunk_upload.py",
    "bot_call/series.py",
    "bot_call/backend.py",
    "bot_call/binding.py",
    "bot_call/call_policy.py",
    "bot_call/call_history.py",
    "bot_call/limits.py",
    "bot_call/native_host.py",
    "bot_call/native_provider.py",
    "bot_call/plugin.py",
    "bot_call/settings.py",
    "bot_call/proxy_access.py",
    "bot_call/observability.py",
    "bot_call/prepare_diagnostics.py",
    "bot_call/sdk_privacy.py",
    "bot_call/publication.py",
    "bot_call/quick_tunnel.py",
    "bot_call/review.py",
    "bot_call/review_plan.py",
    "bot_call/review_policy.py",
    "bot_call/review_router.py",
    "bot_call/review_work.py",
    "bot_call/review_retry.py",
    "bot_call/retention.py",
    "bot_call/runtime.py",
    "bot_call/service.py",
    "bot_call/static/call.css",
    "bot_call/static/call.js",
    "bot_call/static/echo.js",
    "bot_call/static/capture.js",
    "bot_call/static/capture-worker.js",
    "bot_call/static/capture-worklet.js",
    "bot_call/static/pcm.js",
    "bot_call/static/capability-worker.js",
    "bot_call/static/index.html",
    "bot_call/static/page.js",
    "bot_call/static/fixed-sample.wav",
    "bot_call/static/vad.js",
    "bot_call/static/vad-worklet.js",
    "bot_call/static/vendor/vad-v5-ort-1.22.0-2623a295/silero_vad_v5.onnx",
    "bot_call/static/vendor/vad-v5-ort-1.22.0-2623a295/ort.wasm.min.js",
    "bot_call/static/vendor/vad-v5-ort-1.22.0-2623a295/ort-wasm-simd-threaded.mjs",
    "bot_call/static/vendor/vad-v5-ort-1.22.0-2623a295/ort-wasm-simd-threaded.wasm",
    "bot_call/static/vendor/vad-v5-ort-1.22.0-2623a295/VAD-SILERO-LICENSE.txt",
    "bot_call/static/vendor/vad-v5-ort-1.22.0-2623a295/ORT-LICENSE.txt",
    "bot_call/static/vendor/vad-v5-ort-1.22.0-2623a295/ORT-ThirdPartyNotices.txt",
    "bot_call/static/vendor/vad-v5-ort-1.22.0-2623a295/sources.json",
    "bot_call/store.py",
    "bot_call/web.py",
    "main.py",
    "metadata.yaml",
)


# Absent only on the backend integration branch; a partial panel is an error.
CONTROL_FILES = (
    "pages/control/index.html",
    "pages/control/control.css",
    "pages/control/control.js",
)


def package_files(root, *, require_control=False):
    present = [(root / name).exists() or (root / name).is_symlink() for name in CONTROL_FILES]
    if (require_control or any(present)) and not all(present):
        raise ValueError("Control panel sources are incomplete")
    return ALLOWLIST + (CONTROL_FILES if all(present) else ())


def _read(root, name):
    relative = PurePosixPath(name)
    if relative.is_absolute() or ".." in relative.parts or "\\" in name:
        raise ValueError("Unsafe package member")
    path = root
    for part in relative.parts:
        path = path / part
        if path.is_symlink():
            raise ValueError("Symbolic links are not package sources")
    if not path.is_file() or not path.resolve().is_relative_to(root):
        raise ValueError("Package source must be a regular in-tree file: " + name)
    return path.read_bytes()


def _save(path, data):
    if path.is_symlink():
        raise ValueError("Artifact target must not be a symbolic link")
    if path.exists():
        if not path.is_file() or path.read_bytes() != data:
            raise ValueError("Existing artifact differs; use a new reviewed version")
        return
    with path.open("xb") as output:
        output.write(data)


def build(destination=None):
    root = Path(__file__).resolve().parents[1]
    metadata = json.loads(_read(root, "metadata.yaml"))
    require_control = tuple(int(part) for part in metadata["version"].split(".")) >= (0, 6, 0)
    sources = {
        name: _read(root, name) for name in package_files(root, require_control=require_control)
    }
    # JSON is a YAML subset. It keeps this build independent of a YAML library.
    metadata = json.loads(sources["metadata.yaml"])
    version = metadata["version"]
    if metadata["name"] != "astrbot_plugin_bot_call" or not re.fullmatch(
        r"[0-9]+\.[0-9]+\.[0-9]+", version
    ):
        raise ValueError("Invalid package identity/version")
    entry_version = None
    for node in ast.parse(sources["main.py"]).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "PLUGIN_VERSION" for t in node.targets
        ):
            entry_version = ast.literal_eval(node.value)
    if entry_version != version:
        raise ValueError("Entry and metadata versions differ")
    if json.loads(sources["_conf_schema.json"])["enabled"]["default"] is not False:
        raise ValueError("The check package must be disabled by default")
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_STORED) as archive:
        for name, data in sources.items():
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o644) << 16
            info.compress_type = zipfile.ZIP_STORED
            archive.writestr(info, data)
    package = stream.getvalue()
    filename = f"astrbot_plugin_bot_call-{version}.zip"
    manifest = {
        "package": filename,
        "entry_version": entry_version,
        "astrbot_version": metadata["astrbot_version"],
        "qualification": f"AstrBot {SUPPORTED_VERSIONS} native main Agent candidate: build_main_agent, native hooks/reset and run_agent use the source configuration, persona and dynamic tools. No business-plugin adapters or direct access to other plugins' databases. Source STT.get_text/TTS.get_audio and review Context.llm_generate remain native. Agent rounds, timeouts, model parameters and internal retries belong to AstrBot. CallPolicy-v3/schema19 record framework-reported main-Agent outcomes; legacy v1/v2 policies and frozen notes remain readable without execution or replay. Background submission and cancellation do not prove business completion, rollback or arbitrary thread exit. Offline validation is not real model, arbitrary-plugin, QQ or mobile acceptance; disabled by default. See CHANGELOG.md.",
        "zip_sha256": hashlib.sha256(package).hexdigest(),
        "zip_bytes": len(package),
        "files": [
            {"name": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            for name, data in sources.items()
        ],
    }
    destination = root / "dist" if destination is None else Path(destination)
    if destination.is_symlink():
        raise ValueError("Artifact directory must not be a symbolic link")
    destination.mkdir(exist_ok=True)
    _save(destination / filename, package)
    _save(
        destination / (filename[:-4] + ".manifest.json"),
        (json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(),
    )
    print(filename, manifest["zip_sha256"])


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, help="Build into an isolated artifact directory")
    build(parser.parse_args().output_dir)
