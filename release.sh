#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")"
rm -rf dist/
if [[ "$(python3 -V)" =~ "Python 3" ]]; then
	PY=python3
else
	PY=python
fi
$PY -m build --sdist --wheel
twine upload dist/*
