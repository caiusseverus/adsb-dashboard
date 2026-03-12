#!/usr/bin/env bash
# build_pymodes_cython.sh — compile the pyModeS 2.9 C extension (c_common.pyx)
#
# pyModeS 2.9 ships a Cython source (c_common.pyx) but the PyPI wheel is
# pure Python.  This script builds and installs the C extension into the
# project venv, giving ~5-10× speedup on the hot decode path.
#
# Run once after `uv sync`:
#   cd backend && bash build_pymodes_cython.sh
#
# Requirements: gcc (sudo apt install build-essential)
# Cython is installed automatically into the venv.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"

if [[ ! -x "$VENV_PYTHON" ]]; then
  echo "ERROR: venv not found. Run 'uv sync' first." >&2
  exit 1
fi

echo "Installing Cython into venv…"
uv pip install --quiet cython

echo "Downloading pyModeS 2.9 source…"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT
pip download "pymodes==2.9" --no-deps --no-binary pymodes -d "$TMPDIR" -q
tar -xzf "$TMPDIR"/pyModeS-2.9.tar.gz -C "$TMPDIR"

echo "Building c_common extension…"
"$VENV_PYTHON" -m ensurepip -q 2>/dev/null || true
"$VENV_PYTHON" -m pip install setuptools -q
cd "$TMPDIR/pyModeS-2.9"
"$VENV_PYTHON" setup.py build_ext --inplace -q

SITE="$("$VENV_PYTHON" -c "import pyModeS, os; print(os.path.dirname(pyModeS.__file__))")"
cp pyModeS/c_common.cpython-*.so "$SITE/"
echo "Installed c_common extension to $SITE"

"$VENV_PYTHON" -c "
import pyModeS as pms
assert type(pms.df).__name__ == 'cython_function_or_method', 'C extension not active!'
print('OK — pyModeS is using the C extension.')
"
