#!/bin/bash

set -ouo > /dev/null 2>&1

if [ $# -eq 0 ]; then
  echo "No parameters provided."
  echo "Syntax: ./list-missing-commits-by-issue-key.sh <fix-version> <since-tag-name> <to-tag-name>"
  echo ""
  echo "  fix-version    : The Jira version for which issues will be compared against the Git history"
  echo "  since-tag-name : The starting tag to begin inspecting Git history from"
  echo "  to-tag-name    : The ending tag to stop inspecting Git history to"
  echo ""
  echo "An example of comparing the issues in Jira for 1.8.0.CR1 with Git history would be:"
  echo "  ./list-missing-commits-by-issue-key.sh 1.8.0.CR1 v1.8.0.Beta1 v1.8.0.CR1"
  echo ""
  exit 1
fi

FIX_VERSION=$1
SINCE_TAG_NAME=$2
TO_TAG_NAME=$3

DIR="$HOME/debezium-issues"

mkdir -p $DIR

JIRA_URL="https://issues.redhat.com"
PROJECT_NAME="Debezium"
GITHUB_REPO_URL="https://api.github.com/repos/debezium"
EXCLUDED_JIRA_COMPONENTS="examples,website,user-interface-frontend"

ISSUE_KEYS="$DIR/debezium-version-issue-keys.txt"
GIT_HISTORY_PREFIX="$DIR/git-history"
ISSUE_CHECK="$DIR/issue-check.txt"
SCRIPT_OUTPUT_BAD="$DIR/debezium-backport-results-not-exists.txt"
SCRIPT_OUTPUT_OK="$DIR/debezium-backport-results-found.txt"

# Repositories to check for existence of commits
declare -a DEBEZIUM_REPOS=("debezium" "debezium-connector-db2" "debezium-connector-cassandra" "debezium-connector-vitess" "docker-images")

# Obtain all issue keys that are part of the fixed version
echo "Getting issues from Jira for project $PROJECT_NAME and version $FIX_VERSION"
echo "  components excluded: $EXCLUDED_JIRA_COMPONENTS"
echo ""
curl --silent -X "GET" "$JIRA_URL/rest/api/2/search?jql=project=$PROJECT_NAME%20and%20fixVersion=$FIX_VERSION%20and%20component%20not%20in%20($EXCLUDED_JIRA_COMPONENTS)" | jq -r '.issues[].key' > "$ISSUE_KEYS" 2> /dev/null

# Obtain all history between tags for each repository
for REPO in "${DEBEZIUM_REPOS[@]}";
do
  echo "Getting git history for repository $REPO"
  GIT_HISTORY_FILE="$GIT_HISTORY_PREFIX-$REPO.txt"
  curl --silent -X "GET" "${GITHUB_REPO_URL}/$REPO/compare/$SINCE_TAG_NAME...$TO_TAG_NAME" | jq ".commits[] | .commit.message" > "$GIT_HISTORY_FILE"
done

echo ""
echo "Fix Version : $FIX_VERSION"
echo "Comparing   : $SINCE_TAG_NAME ... $TO_TAG_NAME"

# Read each issue key and verify that at least one commit in one repository exists for the key
while IFS=" " read -r ISSUE_KEY
do
  # Iterate each repository history file
  ISSUE_KEY_FOUND=0
  for REPO in "${DEBEZIUM_REPOS[@]}";
  do
    GIT_HISTORY_FILE="$GIT_HISTORY_PREFIX-$REPO.txt"
    cat "$GIT_HISTORY_FILE" | grep "$ISSUE_KEY" > "$ISSUE_CHECK"
    if [ -s "$ISSUE_CHECK" ]; then
      # The file isn't empty, so the issue key was found
      ISSUE_KEY_FOUND=1
    fi
  done

  if [ $ISSUE_KEY_FOUND -eq 0 ]; then
    # Issue key was not found
    echo "$ISSUE_KEY - $JIRA_URL/browse/$ISSUE_KEY" >> "$SCRIPT_OUTPUT_BAD"
  else
    # Issue key was found
    echo "$ISSUE_KEY - $JIRA_URL/browse/$ISSUE_KEY" >> "$SCRIPT_OUTPUT_OK"
  fi

done < $ISSUE_KEYS

echo ""
echo "Issues found: "
echo "--------------------------------------------------------"
if [ -s "$SCRIPT_OUTPUT_OK" ]; then
  cat "$SCRIPT_OUTPUT_OK"
fi

echo ""
echo "Issues not found: "
echo "--------------------------------------------------------"
if [ -s "$SCRIPT_OUTPUT_BAD" ]; then
  cat "$SCRIPT_OUTPUT_BAD"
fi

rm -rf $DIR