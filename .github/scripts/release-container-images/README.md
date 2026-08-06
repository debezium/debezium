# Release Container Images Scripts

This directory contains helper scripts for the `release-container-images` GitHub Actions workflow.

**Location:** `.github/scripts/release-container-images/`

## Scripts

### build-tagged-images.sh

Builds Debezium container images for specific version tags sequentially.

**Purpose:**
- Processes a JSON build list of version tags
- Checks out each tag in sequence
- Fetches build scripts from main branch
- Executes multi-platform builds for each tag
- Handles errors and provides build summary

**Usage:**

```bash
# Set required environment variables
export BUILD_LIST='[{"stream":"2.7","tag":"2.7.3.Final"},{"stream":"2.6","tag":"2.6.5.Final"}]'
export IMAGES_BRANCH="main"
export DRY_RUN="false"
export LATEST_STREAM="2.7"
export PLATFORM_CONDUCTOR_PLATFORM="linux/amd64"
export PLATFORM_STAGE_PLATFORM="linux/amd64"
export MULTIPLATFORM_PLATFORMS="linux/amd64,linux/arm64"

# Run the script
./build-tagged-images.sh
```

**Required Environment Variables:**
- `BUILD_LIST`: JSON array of build items with 'stream' and 'tag' fields
- `IMAGES_BRANCH`: Branch name for fetching build scripts (e.g., "main")

**Optional Environment Variables:**
- `DRY_RUN`: Boolean flag for dry-run mode (default: false)
- `SKIP_UI`: Boolean flag to skip UI image build (default: false)
- `LATEST_STREAM`: The stable stream identifier (default: empty)
- `PLATFORM_CONDUCTOR_PLATFORM`: Platform for conductor image (default: linux/amd64)
- `PLATFORM_STAGE_PLATFORM`: Platform for stage image (default: linux/amd64)
- `MULTIPLATFORM_PLATFORMS`: Comma-separated list of platforms (default: linux/amd64,linux/arm64)

**Features:**
- Sequential build execution (no parallelization)
- IPv6 disabled for Node.js compatibility
- Automatic cleanup after each build
- Detailed build summary with success/failure counts
- Exit code 1 if any builds fail

### process-versions.py

Processes Debezium version tags and generates build information for GitHub Actions.

**Purpose:**
- Parses semantic version tags from the Debezium repository
- Groups versions by major.minor streams
- Identifies the most recent streams to build
- Determines the stable stream (most recent with 'Final' classifier)
- Generates a build list for sequential image builds

**Usage:**

```bash
# Fetch tags and process them
gh api repos/debezium/debezium/tags --paginate | \
  jq -r '.[].name' | \
  grep -E '^v[0-9]+\.[0-9]+\.[0-9]+' | \
  python3 process-versions.py --streams-count 2 --tags-per-stream 1
```

**Arguments:**
- `--streams-count`: Number of most recent streams to build (default: 2)
- `--tags-per-stream`: Number of most recent tags per stream to build (default: 1)

**Input:**
- Reads version tags from stdin (one per line)
- Expected format: `v2.7.3.Final` or `2.7.3.Final`

**Output:**
- Outputs GitHub Actions output format to stdout:
  - `streams=<space-separated list of streams>`
  - `stable_stream=<stable stream or empty>`
  - `build_list=<JSON array of build items>`

**Example Output:**

```
streams=2.7 2.6
stable_stream=2.7
build_list=[{"stream":"2.7","tag":"2.7.3.Final"},{"stream":"2.6","tag":"2.6.5.Final"}]
```

**Testing:**

```bash
# Test with sample data
echo -e "v2.7.3.Final\nv2.7.2.Final\nv2.6.5.Final\nv2.6.4.Final" | \
  python3 process-versions.py --streams-count 2 --tags-per-stream 1

# Test with actual GitHub data
gh api repos/debezium/debezium/tags --paginate | \
  jq -r '.[].name' | \
  grep -E '^v[0-9]+\.[0-9]+\.[0-9]+' | \
  head -20 | \
  python3 process-versions.py --streams-count 2 --tags-per-stream 2
```

## Version Format

The script expects semantic versioning with the following format:

```
[v]MAJOR.MINOR.MICRO[.CLASSIFIER]
```

Examples:
- `v2.7.3.Final` - Stable release
- `2.7.3.Final` - Stable release (without 'v' prefix)
- `v2.7.0.Alpha1` - Alpha release
- `v2.7.0.Beta1` - Beta release
- `v2.7.0.CR1` - Release candidate

## Stream Selection Logic

1. **Parse all tags** into Version objects
2. **Group by stream** (major.minor)
3. **Sort versions** within each stream (newest first)
4. **Select most recent streams** based on `--streams-count`
5. **Find stable stream** (most recent stream with 'Final' classifier)
6. **Generate build list** with `--tags-per-stream` tags per stream

## Error Handling

The script handles the following error cases:

- **Invalid version format**: Skips tags that don't match the expected format
- **No valid versions**: Exits with error code 1
- **Invalid arguments**: Validates that counts are at least 1

All errors and warnings are written to stderr, while output for GitHub Actions is written to stdout.
