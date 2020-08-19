#!/bin/bash
# This script outputs the docker image tag
set -euo pipefail
VERSION=$(git describe --tags --match "v*.*.*" | cut -d '-' -f1)
echo "${XSAMTOOLS_IMAGE_BASE_NAME}-${VERSION}"
