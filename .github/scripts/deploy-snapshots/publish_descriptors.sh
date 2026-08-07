#!/bin/bash
set -euo pipefail

# Publish descriptors to the registry repository
# This script commits and pushes the generated descriptors to the descriptors registry

# Remove old snapshot directories
find . -maxdepth 1 -type d -name '*-SNAPSHOT' -exec rm -rf {} + 2>/dev/null || true

# Create directory for new snapshot version
mkdir -p "${SNAPSHOT_VERSION}"

# Copy descriptors to the snapshot version directory
cp -r "${DESCRIPTORS_OUTPUT_DIR}/." "${SNAPSHOT_VERSION}/"

# Stage changes
git add "${SNAPSHOT_VERSION}"

# Commit changes (or skip if no changes)
git commit -m "[snapshot] ${SNAPSHOT_VERSION} from ${CORE_REPOSITORY}@${DEBEZIUM_COMMIT} at ${BUILD_TIMESTAMP}" || echo "No changes to commit"

# Push to the descriptors branch
git push origin "HEAD:${DESCRIPTORS_BRANCH}"
