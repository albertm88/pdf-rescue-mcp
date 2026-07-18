#!/usr/bin/env sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export PYTHONUTF8=1
export PYTHONIOENCODING=utf-8
cd "$project_dir"
if command -v uv >/dev/null 2>&1; then
    exec uv run --locked python -B -m pdf_rescue_mcp.cli "$@"
fi
export PYTHONPATH="$project_dir/src${PYTHONPATH:+:$PYTHONPATH}"
exec python3 -B -m pdf_rescue_mcp.cli "$@"
