#!/usr/bin/env bash
# Create or check Python 3.12 environments for the AstrBot test matrix.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
source tools/dev_paths.sh

check_only=false
if [[ "${1:-}" == --check ]]; then
    check_only=true
    shift
fi
if (($# > 1)); then
    echo "Usage: tools/dev_env.sh [--check] [${test_versions[*]}]" >&2
    exit 2
fi
select_test_versions "$@"

if [[ -f "$BOTCALL_BROWSER" && -x "$BOTCALL_BROWSER" ]]; then
    echo "Chromium available: $BOTCALL_BROWSER"
else
    echo "Chromium unavailable; set BOTCALL_BROWSER to a Linux Chromium executable for browser checks."
fi
if command -v ffmpeg >/dev/null; then
    echo "ffmpeg available: $(command -v ffmpeg)"
else
    echo "ffmpeg unavailable; add ffmpeg to PATH for browser checks."
fi

status=0
for version in "${selected_versions[@]}"; do
    python="$(venv_python "$version")"
    venv="${python%/bin/python}"
    if ! "$check_only" && [[ ! -e "$venv" ]]; then
        python3.12 -m venv "$venv"
        "$python" -m pip install "astrbot==$version" -r requirements-test.txt
    fi
    check_test_venv "$version" || status=1
done
exit "$status"
