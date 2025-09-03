#!/usr/bin/env bash
set -euo pipefail
# dynamic variable used to reference utilities
UTILS=./utils

# typical entrypoint: load, then cleanup
bash ./load.sh
source ${UTILS}/cleanup.sh

