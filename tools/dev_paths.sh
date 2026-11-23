# Shared development paths and supported test versions.
BOTCALL_VENV_ROOT="${BOTCALL_VENV_ROOT:-$HOME/.local/share/bot-call-venv}"
BOTCALL_BROWSER="${BOTCALL_BROWSER:-$HOME/.local/share/bot-call-browser/current/chrome}"
BOTCALL_STATE_DIR="${BOTCALL_STATE_DIR:-$HOME/.local/state/Bot-Call}"
test_versions=(4.27.2 4.29.3)

select_test_versions() {
    selected_versions=("${test_versions[@]}")
    if (($#)); then
        for version in "${test_versions[@]}"; do
            if [[ "$1" == "$version" ]]; then
                selected_versions=("$1")
                return
            fi
        done
        echo "Unsupported test version: $1 (available: ${test_versions[*]})" >&2
        return 2
    fi
}

venv_python() {
    printf '%s/astrbot-%s/bin/python\n' "$BOTCALL_VENV_ROOT" "$1"
}

check_test_venv() {
    local python
    python="$(venv_python "$1")"
    if [[ ! -x "$python" ]]; then
        echo "Missing test venv: $python; run tools/dev_env.sh $1" >&2
        return 1
    fi
    "$python" -c '
import sys
from importlib.metadata import version
import astrbot

if sys.version_info[:2] != (3, 12) or version("astrbot") != sys.argv[1]:
    sys.exit(f"{sys.executable}: expected Python 3.12 and AstrBot {sys.argv[1]}")
print("Python", sys.version.split()[0], "AstrBot", version("astrbot"), flush=True)
' "$1"
}
