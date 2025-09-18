#!/bin/bash

# Legacy wrapper script for backward compatibility
# Calls the new unified build.py script

set -eo pipefail

DBT_PATH="$( cd "$(dirname "$0")/.." ; pwd -P )"

echo "Using legacy build-dist.sh wrapper - consider using scripts/build.py directly"

cd "$DBT_PATH"
python scripts/build.py --skip-tests --build-type both
