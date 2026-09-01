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
# Progress helper
# ---------------------------------------------------------------------------
STEP=0
step() {
    STEP=$((STEP + 1))
    echo ""
    echo "==> Step ${STEP}: $*"
}

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
step "Fetching tags to derive PREVIOUS_VERSION..."
# Tags have the form vMAJOR.MINOR.MICRO.Qualifier (e.g. v3.7.0.Beta1).
# ltrimstr("refs/tags/v") strips both the ref prefix and the v in one step.
# We only consider tags that are strictly less than VERSION so that
# pre-release tags of the same MICRO (e.g. 3.6.2.Alpha1 when releasing
# 3.6.2.Final) do not shadow the real previous release.
# Sort fields: k1=MAJOR k2=MINOR k3=MICRO k4=Qualifier (lexicographic, Alpha<Beta<Final).
MICRO=$(echo "$VERSION" | grep -oP '^\d+\.\d+\.\d+')
PREVIOUS_VERSION=$(gh api \
  "repos/debezium/debezium/git/refs/tags" --paginate \
  --jq '.[].ref | ltrimstr("refs/tags/v")' \
  | grep -P "^${MAJOR_MINOR//./\\.}\." \
  | grep -v "^${MICRO//./\\.}\." \
  | sort -t. -k1,1n -k2,2n -k3,3n -k4,4 \
  | tail -1)

if [ -z "$PREVIOUS_VERSION" ]; then
    PREVIOUS_VERSION=$(gh api \
      "repos/debezium/debezium/git/refs/tags" --paginate \
      --jq '.[].ref | ltrimstr("refs/tags/v")' \
      | grep -v "^${MICRO//./\\.}\." \
      | sort -t. -k1,1n -k2,2n -k3,3n -k4,4 \
      | tail -1)
fi

# ---------------------------------------------------------------------------
# Read POM versions
# ---------------------------------------------------------------------------
step "Reading POM versions from debezium/pom.xml..."
pom_property() {
    # Use local-name() to ignore the Maven default namespace, and name()='...'
    # for the property child because property names contain dots which XPath
    # would otherwise interpret as the child-axis separator.
    xmllint --xpath "string(//*[local-name()='properties']/*[name()='${2}'])" "${1}" | tr -d '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'
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
# Print all derived versions and confirm before proceeding
# ---------------------------------------------------------------------------
echo ""
echo "Derived versions:"
echo "  VERSION              = ${VERSION}"
echo "  NEXT_VERSION         = ${NEXT_VERSION}"
echo "  PREVIOUS_VERSION     = ${PREVIOUS_VERSION}"
echo "  MAJOR_MINOR          = ${MAJOR_MINOR}"
echo "  SOURCE_BRANCH        = ${SOURCE_BRANCH}"
echo "  KAFKA_VERSION        = ${KAFKA_VERSION}"
echo "  APICURIO_VERSION     = ${APICURIO_VERSION}"
echo "  MYSQL_SERVER_VERSION = ${MYSQL_SERVER_VERSION}"
echo "  DB2_VERSION          = ${DB2_VERSION}"
echo "  OJDBC_VERSION        = ${OJDBC_VERSION}"
echo "  INFORMIX_JDBC_VERSION= ${INFORMIX_JDBC_VERSION}"
echo "  MAVEN_VERSION        = ${MAVEN_VERSION}"
echo "  GROOVY_VERSION       = ${GROOVY_VERSION}"
echo ""
read -r -p "Press Enter to confirm or Ctrl-C to abort: "

# ---------------------------------------------------------------------------
# Git operations
# ---------------------------------------------------------------------------
step "Preparing git branches..."
cd debezium
if [[ "$(git symbolic-ref --short HEAD)" == "changelog-${VERSION}" ]]; then
    echo "Branch changelog-${VERSION} is already active in debezium, skipping checkout."
elif git show-ref --verify --quiet "refs/heads/changelog-${VERSION}"; then
    echo "Branch changelog-${VERSION} exists in debezium but is not active, switching to it."
    git checkout "changelog-${VERSION}"
else
    git checkout "$SOURCE_BRANCH" && git pull --rebase upstream "$SOURCE_BRANCH"
    git checkout -b "changelog-${VERSION}"
fi
# Always fetch main so scripts and config used during the release are available from it.
git fetch upstream main
# Overlay scripts and config from upstream/main so we always use the canonical,
# up-to-date versions regardless of SOURCE_BRANCH.
git checkout upstream/main -- \
    jenkins-jobs/scripts/dbz-project-tool.groovy \
    jenkins-jobs/scripts/check-contributors.sh \
    jenkins-jobs/scripts/config/FilteredNames.txt \
    jenkins-jobs/scripts/config/FilteredCommits.txt \
    jenkins-jobs/scripts/config/Aliases.txt
cd ..

cd debezium.github.io
if [[ "$(git symbolic-ref --short HEAD)" == "changelog-${VERSION}" ]]; then
    echo "Branch changelog-${VERSION} is already active in debezium.github.io, skipping checkout."
elif git show-ref --verify --quiet "refs/heads/changelog-${VERSION}"; then
    echo "Branch changelog-${VERSION} exists in debezium.github.io but is not active, switching to it."
    git checkout "changelog-${VERSION}"
else
    git checkout develop && git pull --rebase upstream develop
    git checkout -b "changelog-${VERSION}"
fi
cd ..

for REPO in debezium-connector-db2 debezium-connector-cassandra debezium-connector-vitess \
            debezium-connector-spanner debezium-connector-informix debezium-connector-ibmi \
            debezium-connector-cockroachdb debezium-connector-ingres debezium-connector-yashandb \
            debezium-quarkus debezium-server debezium-operator debezium-platform; do
    if [ -d "$REPO" ]; then
        cd "$REPO"
        echo "$REPO"
        git checkout "$SOURCE_BRANCH"
        git pull --rebase upstream "$SOURCE_BRANCH"
        cd ..
    fi
done

# ---------------------------------------------------------------------------
# GitHub Project operations
# ---------------------------------------------------------------------------
step "Creating new GitHub project iteration ${NEXT_VERSION}..."
groovy debezium/jenkins-jobs/scripts/dbz-project-tool.groovy \
    -o debezium -t "$GITHUB_TOKEN" -i "$VERSION" -p "$PROJECT_NUMBER" \
    -a new-iteration --new-iteration "$NEXT_VERSION"

step "Checking issues before release..."
groovy debezium/jenkins-jobs/scripts/dbz-project-tool.groovy \
    -o debezium -t "$GITHUB_TOKEN" -i "$VERSION" -p "$PROJECT_NUMBER" \
    -a check-issues-before-release

# ---------------------------------------------------------------------------
# Generate and split release notes
# ---------------------------------------------------------------------------
step "Generating release notes..."
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
step "Updating CHANGELOG.md..."
{ head -n 3 debezium/CHANGELOG.md; cat /tmp/changelog-fragment.md; tail -n +4 debezium/CHANGELOG.md; } \
    > /tmp/CHANGELOG.new && mv /tmp/CHANGELOG.new debezium/CHANGELOG.md

# ---------------------------------------------------------------------------
# Update antora.yml
# ---------------------------------------------------------------------------
step "Updating documentation/antora.yml..."
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
step "Checking new contributors..."
cd debezium
added=0
placeholders=0
placeholder_lines=()
while IFS='|' read -r _tag name email repo commit; do
    # Resolve the GitHub login from the commit email, then fetch the display name.
    login=$(gh api "search/users?q=${email}+in:email" --jq '.items[0].login // empty' 2>/dev/null || true)
    real_name=""
    if [ -n "$login" ]; then
        real_name=$(gh api "users/$login" --jq '.name // empty' 2>/dev/null || true)
        real_name=$(echo "$real_name" | tr -d '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    else
        echo "  [!] Could not resolve GitHub login for: $name <$email> (repo: $repo, commit: $commit)"
    fi
    if [ -n "$real_name" ]; then
        echo "  [+] Added contributor: $real_name (login: $login)"
        echo "$real_name" >> COPYRIGHT.txt
        # Escape any commas in the display name so the CSV stays valid
        safe_name="${real_name//,/}"
        echo "$login,$safe_name" >> jenkins-jobs/scripts/config/Aliases.txt
        added=$((added + 1))
    else
        echo "  [?] Could not resolve display name for: $name <$email> (login: ${login:-unknown}) — left as PLACEHOLDER"
        placeholder_lines+=("# PLACEHOLDER — verify: $name | $email | $repo | $commit")
        placeholders=$((placeholders + 1))
    fi
done < <(bash jenkins-jobs/scripts/check-contributors.sh 2>/dev/null || true)
# Sort the resolved names, then append placeholders at the end so they are easy to find.
sort -f -o COPYRIGHT.txt COPYRIGHT.txt
for line in "${placeholder_lines[@]}"; do
    echo "$line" >> COPYRIGHT.txt
done
echo "  Contributors added: ${added}, placeholders left: ${placeholders}"
if [[ "$placeholders" -gt 0 ]]; then
    echo "  Review # PLACEHOLDER lines in COPYRIGHT.txt before merging."
fi
cd ..

# ---------------------------------------------------------------------------
# Commit debezium repo (local only)
# ---------------------------------------------------------------------------
step "Committing debezium repo (local only)..."
cd debezium
git add CHANGELOG.md documentation/antora.yml COPYRIGHT.txt \
    jenkins-jobs/scripts/dbz-project-tool.groovy \
    jenkins-jobs/scripts/check-contributors.sh \
    jenkins-jobs/scripts/config/Aliases.txt \
    jenkins-jobs/scripts/config/FilteredNames.txt \
    jenkins-jobs/scripts/config/FilteredCommits.txt
git commit --signoff -m "[release] Changelog for ${VERSION}"
cd ..

# ---------------------------------------------------------------------------
# Generate release summary via Bob Shell
# ---------------------------------------------------------------------------
step "Generating release summary with Bob..."
CHANGELOG_CONTENT=$(cat /tmp/changelog-fragment.md)
BOB_OUTPUT=$(bob run --format json \
  "You are preparing release notes for the Debezium ${VERSION} release. \
Given the following changelog fragment, write a concise release summary of the 10 to 20 most \
important new features and fixes. Write it as a single line of plain text where each feature or fix \
is separated by a semicolon and a space ('; '). No bullet points, no markdown formatting, no newlines. \
Start directly with the first feature, do not include a preamble. \
Changelog:
${CHANGELOG_CONTENT}")

# --format json emits a single JSON object; the final answer is in last_message.
RELEASE_SUMMARY=$(echo "$BOB_OUTPUT" | jq -r '.last_message')

# Sanitise: collapse any newlines to a space, strip leading/trailing
# whitespace, and escape double-quote characters for the YAML scalar.
RELEASE_SUMMARY=$(echo "$RELEASE_SUMMARY" | tr '\r\n' '  ' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//;s/"/\\"/g')

# ---------------------------------------------------------------------------
# Create VERSION.yml
# ---------------------------------------------------------------------------
step "Creating ${VERSION}.yml release metadata..."
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
step "Prepending release notes into release-notes.asciidoc..."
RNFILE="debezium.github.io/releases/${MAJOR_MINOR}/release-notes.asciidoc"
INSERT_LINE=$(grep -n '^\[\[release-' "$RNFILE" | head -1 | cut -d: -f1)
if [[ -z "$INSERT_LINE" ]]; then
    echo "ERROR: could not find an existing [[release-...]] anchor in ${RNFILE}" >&2
    exit 1
fi
{ head -n $((INSERT_LINE - 1)) "$RNFILE"; cat /tmp/release-notes-fragment.adoc; tail -n +"${INSERT_LINE}" "$RNFILE"; } \
    > /tmp/release-notes.new && mv /tmp/release-notes.new "$RNFILE"

# ---------------------------------------------------------------------------
# Commit debezium.github.io repo (local only)
# ---------------------------------------------------------------------------
step "Committing debezium.github.io repo (local only)..."
cd debezium.github.io
git add "releases/${MAJOR_MINOR}/release-notes.asciidoc" \
        "_data/releases/${MAJOR_MINOR}/${VERSION}.yml"
git commit --signoff -m "[release] Changelog for ${VERSION}"
cd ..

# ---------------------------------------------------------------------------
# Review pause — both commits are local; nothing has been pushed yet
# ---------------------------------------------------------------------------
echo ""
echo "Both commits are ready locally. Please review:"
echo "  debezium:           git -C debezium show --stat"
echo "  debezium.github.io: git -C debezium.github.io show --stat"
echo "  Release summary:    debezium.github.io/_data/releases/${MAJOR_MINOR}/${VERSION}.yml"
echo "  COPYRIGHT.txt placeholders (if any): debezium/COPYRIGHT.txt"
echo ""
read -r -p "Press Enter to push both branches and create PRs, or Ctrl-C to abort: "

# ---------------------------------------------------------------------------
# Push and create PRs
# ---------------------------------------------------------------------------
FORK_OWNER=$(gh api user --jq .login)

step "Pushing debezium repo and creating PR..."
if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Skipping: git push origin changelog-${VERSION}"
    DBZ_PR_URL="(dry-run — no PR created)"
else
    git -C debezium push origin "changelog-${VERSION}"
    DBZ_PR_URL=$(gh pr create \
        --repo "debezium/debezium" \
        --title "[release] Changelog for ${VERSION}" \
        --body "Pre-release preparation: changelog and antora.yml version updates for ${VERSION}." \
        --base "$SOURCE_BRANCH" \
        --head "${FORK_OWNER}:changelog-${VERSION}")
fi

step "Pushing debezium.github.io repo and creating PR..."
if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Skipping: git push origin changelog-${VERSION}"
    WEBSITE_PR_URL="(dry-run — no PR created)"
else
    git -C debezium.github.io push origin "changelog-${VERSION}"
    WEBSITE_PR_URL=$(gh pr create \
        --repo "debezium/debezium.github.io" \
        --title "[release] Changelog for ${VERSION}" \
        --body "Pre-release preparation: release notes and metadata for ${VERSION}." \
        --base develop \
        --head "${FORK_OWNER}:changelog-${VERSION}")
fi

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
