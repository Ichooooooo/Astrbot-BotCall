#!/usr/bin/env bash
# Real installed framework; providers/platforms are synthetic in both environments.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
source tools/dev_paths.sh
if (($# > 1)); then
    echo "Usage: tools/run_tests.sh [${test_versions[*]}]" >&2
    exit 2
fi
select_test_versions "$@"
for selected_version in "${selected_versions[@]}"; do
    check_test_venv "$selected_version"
    python="$(venv_python "$selected_version")"
    "$python" -B -m unittest discover -s tests
    "$python" -B tests/run_call_page.py
    "$python" -B tests/run_benchmark_page.py
done
