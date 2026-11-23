"""Install the candidate through native management APIs in fresh loopback instances.

Only fixed status codes and numbers enter evidence. Runtime output, credentials,
API responses and configuration contents are never logged. No models or platforms
are configured. Each run owns a new private directory and process group.
"""

import argparse
import hashlib
import zipfile
from urllib.parse import urlsplit
import json
import os
from pathlib import Path
import secrets
import signal
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import time

import httpx

from dev_paths import STATE_DIR, venv_python

ROOT = Path(__file__).resolve().parents[1]
PLUGIN = "astrbot_plugin_bot_call"
VERSIONS = ("4.29.3", "4.27.2")


class CheckFailed(Exception):
    pass


def check(condition, code):
    if not condition:
        raise CheckFailed(code)


def port():
    while True:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            value = sock.getsockname()[1]
        if value not in {6185, 6191}:
            return value


def listening(number):
    with socket.socket() as sock:
        sock.settimeout(0.2)
        return sock.connect_ex(("127.0.0.1", number)) == 0


def api(client, method, path, **kwargs):
    response = client.request(method, path, **kwargs)
    check(response.status_code == 200, "api.http_status")
    payload = response.json()
    check(payload.get("status") == "ok", "api.not_ok")
    return payload.get("data")


def group_members(group):
    members = []
    for candidate in Path("/proc").iterdir():
        if candidate.name.isdecimal():
            try:
                if os.getpgid(int(candidate.name)) == group:
                    members.append(candidate)
            except ProcessLookupError:
                pass
    return members


def check_loopback(group):
    inodes = set()
    for member in group_members(group):
        try:
            for fd in (member / "fd").iterdir():
                try:
                    link = fd.readlink().as_posix()
                    if link.startswith("socket:["):
                        inodes.add(link[8:-1])
                except FileNotFoundError:
                    pass
        except FileNotFoundError:
            pass
    count = 0
    for table in ("tcp", "tcp6"):
        for line in (Path("/proc/net") / table).read_text().splitlines()[1:]:
            row = line.split()
            if row[3] == "0A" and row[9] in inodes:
                address, number = row[1].split(":")
                check(int(number, 16) not in {6185, 6191}, "runtime.reserved_port")
                check(address == "0100007F", "runtime.non_loopback_listener")
                count += 1
    check(count >= 2, "runtime.listeners_missing")
    return count


def stop(process, ports):
    if process is not None:
        try:
            os.killpg(process.pid, signal.SIGINT)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
        # Also remove any descendants left behind by a CLI exit.
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if not any(listening(p) for p in ports) and (
            process is None or not group_members(process.pid)
        ):
            return True
        time.sleep(0.1)
    if process is not None and group_members(process.pid):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    return False


def settings_checks(client, directory, result):
    base = f"/api/plug/{PLUGIN}/settings"
    data = api(client, "GET", base)
    fields = {f["key"]: f for group in data["groups"] for f in group["fields"]}
    check(
        data["writable"] is True and data["active_calls"] == 0 and not data["reload_required"],
        "settings.get",
    )
    check(
        len(fields) == len(json.loads((ROOT / "_conf_schema.json").read_text())), "settings.fields"
    )
    check("reload_hint" in data, "settings.reload_hint")
    result["settings_fields"] = len(fields)
    result["checks"].append("settings_get_admin_jwt")
    path = directory / "data/config" / f"{PLUGIN}_config.json"
    before = path.read_bytes()
    original = json.loads(before.decode("utf-8-sig"))
    shared = directory / "data/cmd_config.json"
    shared_sha = hashlib.sha256(shared.read_bytes()).digest()
    invalid = client.post(
        base, json=dict(revision=data["revision"], values={"call_history_turns": True})
    ).json()
    check(
        invalid["status"] == "error"
        and invalid["message"].startswith("bot_call.settings_invalid:")
        and json.loads(invalid["message"].split(":", 1)[1]) == invalid["data"],
        "settings.invalid_envelope",
    )
    check(path.read_bytes() == before, "settings.invalid_wrote_config")
    result["checks"].append("settings_validation_atomic")
    edits = dict(review_style="brief", review_voice="bot", review_system_notes="on_issue")
    saved = api(
        client,
        "POST",
        f"/api/v1/plugins/extensions/{PLUGIN}/settings",
        json=dict(revision=data["revision"], values=edits),
    )
    expected = dict(original, **edits)
    check(
        json.loads(path.read_text(encoding="utf-8-sig")) == expected,
        "settings.native_config_content",
    )
    check(
        path.read_bytes() == json.dumps(expected, ensure_ascii=False, indent=2).encode("utf-8-sig"),
        "settings.native_config_encoding",
    )
    check(
        saved["reload_required"] is True and saved["revision"] != data["revision"],
        "settings.saved_state",
    )
    check(
        hashlib.sha256(shared.read_bytes()).digest() == shared_sha, "settings.changed_shared_config"
    )
    result["checks"].append("settings_post_admin_jwt_native_save")
    conflict = client.post(base, json=dict(revision=data["revision"], values=edits)).json()
    check(
        conflict["status"] == "error" and conflict["message"] == "bot_call.settings_conflict",
        "settings.conflict",
    )
    result["checks"].append("settings_revision_conflict")
    anonymous = httpx.post(
        str(client.base_url).rstrip("/") + base,
        json=dict(revision=saved["revision"], values=edits),
        trust_env=False,
    )
    check(anonymous.status_code == 401, "settings.anonymous_post")
    result["checks"].append("settings_post_reject_anonymous")


def control_page_checks(client, archive, result):
    with zipfile.ZipFile(archive) as bundle:
        present = "pages/control/index.html" in bundle.namelist()
    result["control_panel_packaged"] = int(present)
    if not present:
        result["checks"].append("control_panel_deferred_until_frontend_merge")
        return
    entry = api(client, "GET", f"/api/plugin/page/entry?name={PLUGIN}&page=control")
    path = entry["content_path"]
    check(
        path.startswith((f"/api/v1/plugins/{PLUGIN}/", f"/api/plugin/page/content/{PLUGIN}/")),
        "control_page.path",
    )
    response = client.get(path)
    check(response.status_code == 200 and "control.js" in response.text, "control_page.index")
    parts = urlsplit(path)
    for name in ("control.js", "control.css"):
        asset = parts.path.rsplit("/", 1)[0] + "/" + name
        if parts.query:
            asset += "?" + parts.query
        check(client.get(asset).status_code == 200, "control_page.asset")
    result["checks"].append("native_control_panel_page_assets")


def exercise(version, archive, evidence):
    python = venv_python(version)
    check(python.exists(), "venv.missing")
    staging = STATE_DIR / "staging"
    staging.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory = Path(tempfile.mkdtemp(prefix=f"{version}-", dir=staging))
    dashboard_port, call_port = port(), port()
    while call_port == dashboard_port:
        call_port = port()
    password = "Aa1" + secrets.token_urlsafe(30)
    username = "local_" + secrets.token_hex(8)
    env = dict(os.environ)
    # Do not inherit another installation's root, auth or provider environment.
    for key in list(env):
        if (
            key.startswith(("ASTRBOT_", "DASHBOARD_"))
            or key.endswith(("_KEY", "_TOKEN", "_PASSWORD"))
            or key == "SSH_AUTH_SOCK"
        ):
            env.pop(key)
    env.update(ASTRBOT_DASHBOARD_INITIAL_PASSWORD=password, PYTHONUNBUFFERED="1")
    result = {
        "version": version,
        "checks": [],
        "dashboard_port": dashboard_port,
        "call_port": call_port,
        "candidate_bytes": archive.stat().st_size,
    }
    process = None
    stage = "runtime_version"
    started = time.monotonic()
    try:
        runtime = subprocess.run(
            [
                str(python),
                "-c",
                "import sys; from importlib.metadata import version; "
                "assert sys.version_info[:2] == (3, 12); "
                "assert version('astrbot') == sys.argv[1]",
                version,
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
        check(runtime.returncode == 0, "runtime.version_mismatch")
        result["checks"].append("python312_framework_version")
        stage = "init"
        initialized = subprocess.run(
            [str(python), "-m", "astrbot.cli", "init", "--yes"],
            cwd=directory,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=60,
        )
        check(initialized.returncode == 0, "init.failed")
        config_path = directory / "data/cmd_config.json"
        config = json.loads(config_path.read_text(encoding="utf-8-sig"))
        config["dashboard"].update(host="127.0.0.1", port=dashboard_port, username=username)
        config.update(
            platform=[], provider=[], provider_sources=[], log_file_enable=False, trace_enable=False
        )
        config_path.write_text(json.dumps(config, ensure_ascii=False))
        result["checks"].append("fresh_config_no_platforms_or_providers")
        stage = "start"
        process = subprocess.Popen(
            [str(python), "-m", "astrbot.cli", "run", "--port", str(dashboard_port)],
            cwd=directory,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        with httpx.Client(
            base_url=f"http://127.0.0.1:{dashboard_port}", timeout=90, trust_env=False
        ) as client:
            deadline = time.monotonic() + 60
            while not listening(dashboard_port) and time.monotonic() < deadline:
                check(process.poll() is None, "runtime.exited")
                time.sleep(0.25)
            check(listening(dashboard_port), "runtime.start_timeout")
            stage = "login"
            login = api(
                client, "POST", "/api/auth/login", json={"username": username, "password": password}
            )
            client.headers["Authorization"] = "Bearer " + login["token"]
            result["checks"].append("native_login")
            stage = "install"
            with archive.open("rb") as handle:
                api(
                    client,
                    "POST",
                    "/api/plugin/install-upload",
                    files={"file": (archive.name, handle, "application/zip")},
                    data={"ignore_version_check": "false"},
                )
            result["checks"].append("native_zip_install_version_check")
            disabled = client.get(f"/api/plug/{PLUGIN}/diagnostics")
            check(
                (
                    disabled.status_code in {404, 503}
                    or (disabled.status_code == 200 and disabled.json().get("status") == "error")
                )
                and not listening(call_port)
                and not list((directory / "data").rglob("calls.sqlite3")),
                "plugin.default_not_disabled",
            )
            result["checks"].append("installed_disabled_no_call_database")
            stage = "enable"
            api(client, "POST", "/api/plugin/on", json={"name": PLUGIN})
            schema = json.loads((ROOT / "_conf_schema.json").read_text())
            plugin_config = {key: value["default"] for key, value in schema.items()}
            plugin_config.update(
                enabled=True,
                access_mode="fixed_https",
                public_origin="https://staging.invalid",
                listen_port=call_port,
            )
            api(
                client,
                "POST",
                f"/api/config/plugin/update?plugin_name={PLUGIN}",
                json=plugin_config,
            )
            result["checks"].append("native_config_enable_reload")
            stage = "diagnostics"
            data = api(client, "GET", f"/api/plug/{PLUGIN}/diagnostics")
            check(data.get("plugin_ready") is True, "plugin.not_ready")
            check(data.get("source") is None, "plugin.unexpected_source")
            check(listening(call_port), "plugin.not_listening")
            result["checks"].append("diagnostics_ready_without_keys")
            unauthenticated = httpx.get(
                f"http://127.0.0.1:{dashboard_port}/api/plug/{PLUGIN}/diagnostics",
                trust_env=False,
            )
            check(unauthenticated.status_code == 401, "diagnostics.auth_failed")
            result["checks"].append("diagnostics_reject_anonymous")
            # A native route/auth check only: the fresh database has no calls,
            # and preview must not register work or require any provider.
            preview = client.post(
                f"/api/plug/{PLUGIN}/reviews", json={"call_ids": ["nonexistent"], "apply": False}
            )
            check(preview.status_code == 400, "reviews.preview_failed")
            anonymous = httpx.post(
                f"http://127.0.0.1:{dashboard_port}/api/plug/{PLUGIN}/reviews",
                json={"call_ids": ["nonexistent"]},
                trust_env=False,
            )
            check(anonymous.status_code == 401, "reviews.auth_failed")
            result["checks"].extend(
                ["reviews_preview_reject_unknown_call", "reviews_reject_anonymous"]
            )

            stage = "benchmark_snapshot"
            benchmark = api(client, "GET", f"/api/plug/{PLUGIN}/benchmark")
            check(benchmark["status"]["plugin_ready"], "benchmark.ready")
            check(benchmark["reviews"]["calls"] == [], "benchmark.empty_reviews")
            check(benchmark["current_config"]["llm"] is None, "benchmark.unconfigured_model")
            check(
                benchmark["baseline"]["config"]["llm"]["thinking"] == "off",
                "benchmark.config_object",
            )
            result["checks"].append("benchmark_status_reviews_config")

            stage = "benchmark_assets"
            page = api(client, "GET", f"/api/plugin/page/entry?name={PLUGIN}&page=benchmark")
            content_path = page["content_path"]
            check(
                content_path.startswith(
                    (f"/api/v1/plugins/{PLUGIN}/", f"/api/plugin/page/content/{PLUGIN}/")
                ),
                "page.unexpected_path",
            )
            response = client.get(content_path)
            check(
                response.status_code == 200 and "benchmark.js" in response.text,
                "page.assets_unavailable",
            )
            result["checks"].append("native_benchmark_page_asset")
            stage = "settings"
            settings_checks(client, directory, result)
            stage = "reload"
            # Exercise the owned HTTP listener, then reload immediately through
            # AstrBot's native API. No provider, platform, tunnel or real call.
            response = httpx.get(
                f"http://127.0.0.1:{call_port}/bot-call/",
                headers={
                    "Host": "staging.invalid",
                    "Connection": "close",
                    "X-Forwarded-Proto": "https",
                },
                trust_env=False,
                timeout=5,
            )
            check(response.status_code == 200, "reload.http_probe")
            api(client, "POST", "/api/plugin/reload", json={"name": PLUGIN})
            reloaded = api(client, "GET", f"/api/plug/{PLUGIN}/diagnostics")
            check(reloaded.get("plugin_ready") is True and listening(call_port), "reload.not_ready")
            result["checks"].append("native_reload_after_http_connection")
            stage = "control_assets"
            control_page_checks(client, archive, result)
            result["loopback_listeners"] = check_loopback(process.pid)
            stage = "database"
            databases = list((directory / "data").rglob("calls.sqlite3"))
            check(len(databases) == 1, "database.missing")
            with sqlite3.connect(f"file:{databases[0]}?mode=ro", uri=True) as db:
                result["schema"] = db.execute("PRAGMA user_version").fetchone()[0]
                check(result["schema"] == 24, "database.schema")
                result["reviews_by_state"] = {
                    state: db.execute(
                        "SELECT count(*) FROM reviews WHERE state=?", (state,)
                    ).fetchone()[0]
                    for state in (
                        "pending",
                        "running",
                        "failed",
                        "uncertain",
                        "cancelled",
                        "succeeded",
                    )
                }
                check(
                    not any(
                        result["reviews_by_state"][s]
                        for s in ("pending", "running", "failed", "uncertain", "cancelled")
                    ),
                    "reviews.unfinished",
                )
                tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master")}
                check({"speech_segments", "turn_timings"} <= tables, "database.tables")
            result["checks"].append("fresh_database_schema_24")
        result["status"] = "passed"
    except Exception as error:
        result.update(
            status="failed",
            stage=stage,
            error=error.args[0] if isinstance(error, CheckFailed) else type(error).__name__,
        )
    finally:
        result["process_cleanup"] = int(stop(process, (dashboard_port, call_port)))
        if not result["process_cleanup"]:
            result["status"] = "failed"
        if result["process_cleanup"]:
            shutil.rmtree(directory)
            result["data_cleanup"] = 1
        else:
            result["data_cleanup"] = 0
        result["elapsed_ms"] = round((time.monotonic() - started) * 1000)
        (evidence / f"staging-{version}.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result), flush=True)
    return result["status"] == "passed"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, default=STATE_DIR / "evidence" / "staging")
    args = parser.parse_args()
    os.umask(0o077)
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="candidate-", dir=args.evidence_dir) as artifacts:
        built = subprocess.run(
            [
                os.sys.executable,
                "-B",
                str(ROOT / "tools/build_plugin.py"),
                "--output-dir",
                artifacts,
            ],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=60,
        )
        check(built.returncode == 0, "candidate.build_failed")
        (archive,) = Path(artifacts).glob("*.zip")
        results = [exercise(v, archive, args.evidence_dir) for v in VERSIONS]
    raise SystemExit(0 if all(results) else 1)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "stage": "prepare",
                    "error": error.args[0]
                    if isinstance(error, CheckFailed)
                    else type(error).__name__,
                }
            ),
            flush=True,
        )
        raise SystemExit(1) from None
