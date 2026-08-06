#!/bin/bash
set -eo pipefail

# Build Debezium container images for specific version tags sequentially
#
# This script processes a JSON build list and builds container images for each
# version tag in sequence. It checks out each tag, fetches build scripts from
# the main branch, and executes the build.
#
# Required environment variables:
#   BUILD_LIST: JSON array of build items with 'stream' and 'tag' fields
#   DRY_RUN: Boolean flag for dry-run mode (true/false)
#   SKIP_UI: Boolean flag to skip UI image build (true/false)
#   LATEST_STREAM: The stable stream identifier (e.g., "2.7")
#   PLATFORM_CONDUCTOR_PLATFORM: Platform for conductor image (e.g., "linux/amd64")
#   PLATFORM_STAGE_PLATFORM: Platform for stage image (e.g., "linux/amd64")
#   MULTIPLATFORM_PLATFORMS: Comma-separated list of platforms (e.g., "linux/amd64,linux/arm64")
#   IMAGES_BRANCH: Branch name for fetching build scripts (e.g., "main")

# Validate required environment variables
if [ -z "$BUILD_LIST" ]; then
    echo "Error: BUILD_LIST environment variable is required" >&2
    exit 1
fi

if [ -z "$IMAGES_BRANCH" ]; then
    echo "Error: IMAGES_BRANCH environment variable is required" >&2
    exit 1
fi

# Set defaults for optional variables
DRY_RUN="${DRY_RUN:-false}"
SKIP_UI="${SKIP_UI:-false}"
LATEST_STREAM="${LATEST_STREAM:-}"
PLATFORM_CONDUCTOR_PLATFORM="${PLATFORM_CONDUCTOR_PLATFORM:-linux/amd64}"
PLATFORM_STAGE_PLATFORM="${PLATFORM_STAGE_PLATFORM:-linux/amd64}"
MULTIPLATFORM_PLATFORMS="${MULTIPLATFORM_PLATFORMS:-linux/amd64,linux/arm64}"

echo "=========================================="
echo "Build Configuration"
echo "=========================================="
echo "Dry run: $DRY_RUN"
echo "Skip UI: $SKIP_UI"
echo "Latest stream: $LATEST_STREAM"
echo "Platforms: $MULTIPLATFORM_PLATFORMS"
echo "Conductor platform: $PLATFORM_CONDUCTOR_PLATFORM"
echo "Stage platform: $PLATFORM_STAGE_PLATFORM"
echo "Images branch: $IMAGES_BRANCH"
echo "=========================================="
echo ""

# Disable IPv6 as Node.js has problems downloading dependencies using it
echo "Disabling IPv6 for Node.js compatibility..."
sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1
sudo sysctl -w net.ipv6.conf.default.disable_ipv6=1
echo ""

# Export environment variables for build scripts
export DRY_RUN
export SKIP_UI
export LATEST_STREAM
export PLATFORM_CONDUCTOR_PLATFORM
export PLATFORM_STAGE_PLATFORM
export MULTIPLATFORM_PLATFORMS

# Parse build list and build each tag sequentially
BUILD_COUNT=0
SUCCESS_COUNT=0
FAILED_TAGS=()

echo "$BUILD_LIST" | jq -c '.[]' | while read -r item; do
    STREAM=$(echo "$item" | jq -r '.stream')
    TAG=$(echo "$item" | jq -r '.tag')
    
    BUILD_COUNT=$((BUILD_COUNT + 1))
    
    echo "=========================================="
    echo "Build #$BUILD_COUNT: Tag $TAG (Stream $STREAM)"
    echo "=========================================="
    
    # Checkout the specific tag
    echo "Checking out tag v$TAG..."
    if ! git checkout "v$TAG"; then
        echo "Error: Failed to checkout tag v$TAG" >&2
        FAILED_TAGS+=("$TAG")
        continue
    fi
    
    # Fetch and checkout build scripts from main branch
    echo "Fetching build scripts from $IMAGES_BRANCH branch..."
    if ! git fetch origin "$IMAGES_BRANCH:$IMAGES_BRANCH"; then
        echo "Error: Failed to fetch $IMAGES_BRANCH branch" >&2
        FAILED_TAGS+=("$TAG")
        git reset --hard
        continue
    fi
    
    echo "Checking out build scripts..."
    if ! git checkout "$IMAGES_BRANCH" build-all-multiplatform.sh build-debezium-multiplatform.sh build-postgres-multiplatform.sh script-functions/; then
        echo "Error: Failed to checkout build scripts" >&2
        FAILED_TAGS+=("$TAG")
        git reset --hard
        continue
    fi
    
    # Build images for this tag
    echo ""
    echo "Building images for tag $TAG..."
    echo "Note: UI images only built for linux/amd64 (arm64 not working)"
    echo ""
    
    export RELEASE_TAG="$TAG"
    
    if ./build-debezium-multiplatform.sh "$STREAM" "$MULTIPLATFORM_PLATFORMS"; then
        echo "✅ Successfully built images for tag $TAG"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        echo "❌ Failed to build images for tag $TAG" >&2
        FAILED_TAGS+=("$TAG")
    fi
    
    # Reset to clean state
    echo "Resetting to clean state..."
    git reset --hard
    
    echo ""
    echo "Completed build for tag: $TAG"
    echo "=========================================="
    echo ""
done

# Summary
echo "=========================================="
echo "Build Summary"
echo "=========================================="
echo "Total builds: $BUILD_COUNT"
echo "Successful: $SUCCESS_COUNT"
echo "Failed: $((BUILD_COUNT - SUCCESS_COUNT))"

if [ ${#FAILED_TAGS[@]} -gt 0 ]; then
    echo ""
    echo "Failed tags:"
    for tag in "${FAILED_TAGS[@]}"; do
        echo "  - $tag"
    done
    echo ""
    echo "❌ Some builds failed"
    exit 1
else
    echo ""
    echo "✅ All tagged images built successfully"
fi
