#!/bin/bash
set -euo pipefail

INPUT_PATH="${1:-/input/data}"
APP_DIR="$(cd "$(dirname "$0")" && pwd)"

bash "$APP_DIR/create_index.sh" "$INPUT_PATH"
bash "$APP_DIR/store_index.sh"