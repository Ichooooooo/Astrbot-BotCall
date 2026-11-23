#!/usr/bin/env bash
# Local-only clean install; never uses an existing AstrBot data directory.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
source tools/dev_paths.sh
exec "$(venv_python "${test_versions[-1]}")" -B tools/staging.py "$@"
