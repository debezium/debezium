#!/bin/bash

set -ouo > /dev/null 2>&1

if [ $# -eq 0 ]; then
  echo "No parameters provided."
  echo "Syntax: ./list-missing-commits-by-issue-key.sh <fix-version> <branch-name>"
  echo ""
  echo "  fix-version : The Jira version for which issues will be compared against the Git history"
  echo "  branch-name : The local GitHub branch that should be inspected for the commits"
  echo "                Be sure you have synchronized your local repository!"
  echo ""
  exit 1
fi

FIX_VERSION=$1
BRANCH_NAME=$2

DIR="$HOME/debezium-backports"

mkdir -p $DIR

JIRA_URL="https://issues.redhat.com"
PROJECT_NAME="Debezium"

ISSUE_KEYS="$DIR/debezium-version-issue-keys.txt"
GIT_HISTORY="$DIR/debezium-git-history-issue-key.txt"
SCRIPT_OUTPUT_BAD="$DIR/debezium-backport-results-not-exists.txt"
SCRIPT_OUTPUT_OK="$DIR/debezium-backport-results-found.txt"

# Get all issue keys that are part of the fixed version
echo "Getting issues from Jira for project $PROJECT_NAME and version $FIX_VERSION"
curl --silent -X "GET" "$JIRA_URL/rest/api/2/search?jql=project=$PROJECT_NAME%20and%20fixVersion=$FIX_VERSION" | jq -r '.issues[].key' > "$ISSUE_KEYS" 2> /dev/null

# Read each issue key and verify that at least one commit exists on the specified branch
while IFS=" " read -r ISSUE_KEY
do
  git --no-pager log --all --decorate=short --pretty=oneline --grep="$ISSUE_KEY" "$BRANCH_NAME" > "$GIT_HISTORY"
  if [ -s "$GIT_HISTORY" ]; then
    # The file isn't empty, so the issue key was found
    echo "$ISSUE_KEY - $JIRA_URL/browse/$ISSUE_KEY" >> "$SCRIPT_OUTPUT_OK"
  else
    # The file is empty, backport does not exist
    echo "$ISSUE_KEY - $JIRA_URL/browse/$ISSUE_KEY" >> "$SCRIPT_OUTPUT_BAD"
  fi
done < $ISSUE_KEYS

echo ""
echo ""

echo "Git Branch  : $BRANCH_NAME"
echo "Fix Version : $FIX_VERSION"

echo ""
echo "Issues found:"
echo "-----------------------------------"
if [ -s "$SCRIPT_OUTPUT_OK" ]; then
  cat "$SCRIPT_OUTPUT_OK"
fi

echo ""

echo "Issues not found:"
echo "-----------------------------------"
if [ -s "$SCRIPT_OUTPUT_OK" ]; then
  cat "$SCRIPT_OUTPUT_BAD"
fi
echo ""

rm -rf $DIR