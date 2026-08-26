#!/bin/bash
# pre-release.sh — Debezium pre-release preparation script
#
# Usage:
#   bash pre-release.sh --version VERSION --next-version NEXT_VERSION --token GITHUB_TOKEN [--source-branch BRANCH] [--dry-run]
#
# All repository checkouts must be siblings of the current working directory.
# gh must be authenticated before running this script (gh auth status).
# Use --dry-run to perform all local changes without pushing branches or creating PRs.

set -euo pipefail

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
VERSION=""
NEXT_VERSION=""
SOURCE_BRANCH="main"
GITHUB_TOKEN=""
PROJECT_NUMBER=5
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)        VERSION="$2";        shift 2 ;;
        --next-version)   NEXT_VERSION="$2";   shift 2 ;;
        --source-branch)  SOURCE_BRANCH="$2";  shift 2 ;;
        --token)          GITHUB_TOKEN="$2";   shift 2 ;;
        --dry-run)        DRY_RUN=true;        shift ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Usage: $0 --version VERSION --next-version NEXT_VERSION --token GITHUB_TOKEN [--source-branch BRANCH] [--dry-run]" >&2
            exit 1
            ;;
    esac
done

if [[ -z "$VERSION" || -z "$NEXT_VERSION" || -z "$GITHUB_TOKEN" ]]; then
    echo "ERROR: --version, --next-version, and --token are required." >&2
    echo "Usage: $0 --version VERSION --next-version NEXT_VERSION --token GITHUB_TOKEN [--source-branch BRANCH] [--dry-run]" >&2
    exit 1
fi

if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] No branches will be pushed and no PRs will be created."
fi

MAJOR_MINOR=$(echo "$VERSION" | grep -oP '^\d+\.\d+')

# ---------------------------------------------------------------------------
# Derive PREVIOUS_VERSION
# ---------------------------------------------------------------------------
# Tags have the form vMAJOR.MINOR.MICRO.Qualifier (e.g. v3.7.0.Beta1).
# ltrimstr("refs/tags/v") strips both the ref prefix and the v in one step.
# Sort fields: k1=MAJOR k2=MINOR k3=MICRO k4=Qualifier (lexicographic, Alpha<Beta<Final).
PREVIOUS_VERSION=$(gh api \
  "repos/debezium/debezium/git/refs/tags" \
  --jq '.[].ref | ltrimstr("refs/tags/v")' \
  | grep -P "^${MAJOR_MINOR//./\\.}\." \
  | grep -v "^${VERSION}$" \
  | sort -t. -k1,1n -k2,2n -k3,3n -k4,4 \
  | tail -1)

if [ -z "$PREVIOUS_VERSION" ]; then
    PREVIOUS_VERSION=$(gh api \
      "repos/debezium/debezium/git/refs/tags" \
      --jq '.[].ref | ltrimstr("refs/tags/v")' \
      | grep -v "^${VERSION}$" \
      | sort -t. -k1,1n -k2,2n -k3,3n -k4,4 \
      | tail -1)
fi

echo "Derived PREVIOUS_VERSION=${PREVIOUS_VERSION}"
read -r -p "Press Enter to confirm or Ctrl-C to abort and set manually: "

# ---------------------------------------------------------------------------
# Read POM versions
# ---------------------------------------------------------------------------
pom_property() {
    # Strip leading/trailing whitespace that xmllint may emit
    xmllint --xpath "string(//properties/${2})" "${1}" | tr -d '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'
}

KAFKA_VERSION=$(pom_property debezium/pom.xml version.kafka)
APICURIO_VERSION=$(pom_property debezium/pom.xml version.apicurio)
MYSQL_SERVER_VERSION=$(pom_property debezium/pom.xml version.mysql.server)
DB2_VERSION=$(pom_property debezium/pom.xml version.db2.driver)
OJDBC_VERSION=$(pom_property debezium/pom.xml version.oracle.driver)
INFORMIX_JDBC_VERSION=$(pom_property debezium/pom.xml version.informix.driver)
MAVEN_VERSION=$(pom_property debezium/pom.xml version.maven)
GROOVY_VERSION=$(pom_property debezium/debezium-bom/pom.xml version.groovy)

# ---------------------------------------------------------------------------
# Git operations
# ---------------------------------------------------------------------------
cd debezium
git checkout "$SOURCE_BRANCH" && git pull --rebase upstream "$SOURCE_BRANCH"
git checkout -b "changelog-${VERSION}"
if [[ "$SOURCE_BRANCH" != "main" ]]; then
    git fetch upstream main
fi
cd ..

cd debezium.github.io
git checkout develop && git pull --rebase upstream develop
git checkout -b "changelog-${VERSION}"
cd ..

for REPO in debezium-connector-db2 debezium-connector-cassandra debezium-connector-vitess \
            debezium-connector-spanner debezium-connector-informix debezium-connector-ibmi \
            debezium-connector-cockroachdb debezium-connector-ingres debezium-connector-yashandb \
            debezium-quarkus debezium-server debezium-operator debezium-platform; do
    if [ -d "$REPO" ]; then
        cd "$REPO"
        echo "$REPO"
        git checkout main
        git pull --rebase upstream main
        cd ..
    fi
done

# ---------------------------------------------------------------------------
# GitHub Project operations
# ---------------------------------------------------------------------------
groovy debezium/jenkins-jobs/scripts/dbz-project-tool.groovy \
    -o debezium -t "$GITHUB_TOKEN" -i "$VERSION" -p "$PROJECT_NUMBER" \
    -a new-iteration --new-iteration "$NEXT_VERSION"

groovy debezium/jenkins-jobs/scripts/dbz-project-tool.groovy \
    -o debezium -t "$GITHUB_TOKEN" -i "$VERSION" -p "$PROJECT_NUMBER" \
    -a check-issues-before-release

# ---------------------------------------------------------------------------
# Generate and split release notes
# ---------------------------------------------------------------------------
groovy debezium/jenkins-jobs/scripts/dbz-project-tool.groovy \
    -o debezium -t "$GITHUB_TOKEN" -i "$VERSION" -p "$PROJECT_NUMBER" \
    -a generate-release-notes \
    --previous-iteration "$PREVIOUS_VERSION" \
    --kafka-version "$KAFKA_VERSION" \
    > /tmp/release-notes-raw.txt

awk '/^---CHANGELOG-START---/{found=1; next} /^---CHANGELOG-END---/{found=0} found' \
    /tmp/release-notes-raw.txt > /tmp/changelog-fragment.md

awk '/^---RELEASE-NOTES-START---/{found=1; next} /^---RELEASE-NOTES-END---/{found=0} found' \
    /tmp/release-notes-raw.txt > /tmp/release-notes-fragment.adoc

# ---------------------------------------------------------------------------
# Update CHANGELOG.md
# ---------------------------------------------------------------------------
{ head -n 3 debezium/CHANGELOG.md; cat /tmp/changelog-fragment.md; tail -n +4 debezium/CHANGELOG.md; } \
    > /tmp/CHANGELOG.new && mv /tmp/CHANGELOG.new debezium/CHANGELOG.md

# ---------------------------------------------------------------------------
# Update antora.yml
# ---------------------------------------------------------------------------
cd debezium
sed -i "s|debezium-version: '.*'|debezium-version: '${VERSION}'|" documentation/antora.yml
sed -i "s|debezium-kafka-version: '.*'|debezium-kafka-version: '${KAFKA_VERSION}'|" documentation/antora.yml
sed -i "s|debezium-docker-label: '.*'|debezium-docker-label: '${MAJOR_MINOR}'|" documentation/antora.yml
sed -i "s|apicurio-version: '.*'|apicurio-version: '${APICURIO_VERSION}'|" documentation/antora.yml
sed -i "s|mysql-version: '.*'|mysql-version: '${MYSQL_SERVER_VERSION}'|" documentation/antora.yml
sed -i "s|db2-version: '.*'|db2-version: '${DB2_VERSION}'|" documentation/antora.yml
sed -i "s|ojdbc-version: '.*'|ojdbc-version: '${OJDBC_VERSION}'|" documentation/antora.yml
sed -i "s|informix-jdbc-version: '.*'|informix-jdbc-version: '${INFORMIX_JDBC_VERSION}'|" documentation/antora.yml
sed -i "s|maven-version: '.*'|maven-version: '${MAVEN_VERSION}'|" documentation/antora.yml
sed -i "s|groovy-version: '.*'|groovy-version: '${GROOVY_VERSION}'|" documentation/antora.yml
cd ..

# ---------------------------------------------------------------------------
# Contributor check
# ---------------------------------------------------------------------------
cd debezium
while IFS='|' read -r _tag name email repo commit; do
    real_name=$(gh api "users/$name" --jq '.name // empty' 2>/dev/null | tr -d '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    if [ -n "$real_name" ]; then
        echo "$real_name" >> COPYRIGHT.txt
        # Escape any commas in the display name so the CSV stays valid
        safe_name="${real_name//,/}"
        echo "$name,$safe_name" >> jenkins-jobs/scripts/config/Aliases.txt
    else
        echo "# PLACEHOLDER — verify: $name | $email | $repo | $commit" >> COPYRIGHT.txt
    fi
done < <(bash jenkins-jobs/scripts/check-contributors.sh)
sort -f -o COPYRIGHT.txt COPYRIGHT.txt
cd ..

# ---------------------------------------------------------------------------
# Commit and push debezium repo
# ---------------------------------------------------------------------------
cd debezium
git add CHANGELOG.md documentation/antora.yml COPYRIGHT.txt jenkins-jobs/scripts/config/Aliases.txt
git commit --signoff -m "Changelog for ${VERSION}"
if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Skipping: git push origin changelog-${VERSION}"
    DBZ_PR_URL="(dry-run — no PR created)"
else
    git push origin "changelog-${VERSION}"
    DBZ_PR_URL=$(gh pr create \
        --title "Changelog for ${VERSION}" \
        --body "Pre-release preparation: changelog and antora.yml version updates for ${VERSION}." \
        --base "$SOURCE_BRANCH" \
        --head "changelog-${VERSION}")
fi
cd ..

# ---------------------------------------------------------------------------
# Generate release summary via Bob Shell
# ---------------------------------------------------------------------------
CHANGELOG_CONTENT=$(cat /tmp/changelog-fragment.md)
RELEASE_SUMMARY=$(bob --approval-mode auto_edit --hide-intermediary-output -p \
  "You are preparing release notes for the Debezium ${VERSION} release. \
Given the following changelog fragment, write a concise release summary of the 10 to 20 most \
important new features and fixes. Write it as a single line of plain text where each feature or fix \
is separated by a semicolon and a space ('; '). No bullet points, no markdown formatting, no YAML \
syntax, no newlines — just the raw semicolon-separated text value that will be placed inside a YAML \
'summary:' field. Start directly with the first feature, do not include a preamble. \
Changelog:
${CHANGELOG_CONTENT}")

# Sanitise: collapse any newlines/carriage-returns to a single space, strip
# leading/trailing whitespace, and escape any double-quote characters so the
# value is safe to embed as a quoted YAML scalar.
RELEASE_SUMMARY=$(echo "$RELEASE_SUMMARY" | tr '\r\n' '  ' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//;s/"/\\"/g')

# ---------------------------------------------------------------------------
# Create VERSION.yml
# ---------------------------------------------------------------------------
STABLE=$(echo "$VERSION" | grep -q '\.Final$' && echo "true" || echo "false")
mkdir -p "debezium.github.io/_data/releases/${MAJOR_MINOR}"
cat > "debezium.github.io/_data/releases/${MAJOR_MINOR}/${VERSION}.yml" <<EOF
date: $(date +%Y-%m-%d)
version: "${VERSION}"
stable: ${STABLE}
summary: "${RELEASE_SUMMARY}"
#announcement_url:
EOF

# ---------------------------------------------------------------------------
# Prepend release-notes fragment
# ---------------------------------------------------------------------------
RNFILE="debezium.github.io/releases/${MAJOR_MINOR}/release-notes.asciidoc"
INSERT_LINE=$(grep -n '^\[\[release-' "$RNFILE" | head -1 | cut -d: -f1)
if [[ -z "$INSERT_LINE" ]]; then
    echo "ERROR: could not find an existing [[release-...]] anchor in ${RNFILE}" >&2
    exit 1
fi
{ head -n $((INSERT_LINE - 1)) "$RNFILE"; cat /tmp/release-notes-fragment.adoc; tail -n +"${INSERT_LINE}" "$RNFILE"; } \
    > /tmp/release-notes.new && mv /tmp/release-notes.new "$RNFILE"

# ---------------------------------------------------------------------------
# Commit and push debezium.github.io repo
# ---------------------------------------------------------------------------
cd debezium.github.io
git add "releases/${MAJOR_MINOR}/release-notes.asciidoc" \
        "_data/releases/${MAJOR_MINOR}/${VERSION}.yml"
git commit --signoff -m "Changelog for ${VERSION}"
if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Skipping: git push origin changelog-${VERSION}"
    WEBSITE_PR_URL="(dry-run — no PR created)"
else
    git push origin "changelog-${VERSION}"
    WEBSITE_PR_URL=$(gh pr create \
        --title "Changelog for ${VERSION}" \
        --body "Pre-release preparation: release notes and metadata for ${VERSION}." \
        --base develop \
        --head "changelog-${VERSION}")
fi
cd ..

# ---------------------------------------------------------------------------
# Print remaining manual steps
# ---------------------------------------------------------------------------
echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    echo "Dry run complete. Remaining manual steps:"
else
    echo "Both PRs created. Remaining manual steps:"
fi
echo "1. Review the generated summary in debezium.github.io/_data/releases/${MAJOR_MINOR}/${VERSION}.yml"
echo "2. Review any # PLACEHOLDER lines in debezium/COPYRIGHT.txt"
echo "3. Review PRs and merge after CI passes"
echo ""
echo "PR links:"
echo "  debezium:            ${DBZ_PR_URL}"
echo "  debezium.github.io:  ${WEBSITE_PR_URL}"
