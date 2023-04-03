#!/bin/bash

set -euo pipefail

cd "$(dirname "$0")"
rm -rf dist/
python -m build --sdist --wheel
twine upload dist/*
