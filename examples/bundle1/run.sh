#!/usr/bin/env bash
set -euo pipefail
UTILS=./utils
bash ./load.sh
source ${UTILS}/cleanup.sh