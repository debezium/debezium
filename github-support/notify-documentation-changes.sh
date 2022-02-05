#!/bin/bash

set -ouo > /dev/null 2>&1

GIT_SINCE="1.weeks"
GITHUB_COMMIT_URL="https://github.com/debezium/debezium/commit/"
OUTPUT="documentation_changes.txt"
GIT_OUTPUT_FILE="git_history.txt"
GIT_BRANCH=`git rev-parse --abbrev-ref HEAD`

# Get the history from Git
git log --pretty=oneline --follow --since=$GIT_SINCE -- documentation > $GIT_OUTPUT_FILE

rm -f $OUTPUT
echo "The following Debezium documentation changes have been made in the last 7 days on branch \"$GIT_BRANCH\":" >> $OUTPUT
echo "" >> $OUTPUT

while IFS=" " read -r COMMIT_SHA COMMIT_MSG
do
  echo "* [$COMMIT_SHA]($GITHUB_COMMIT_URL$COMMIT_SHA)" >> $OUTPUT
  echo "$COMMIT_MSG" >> $OUTPUT
done < $GIT_OUTPUT_FILE
rm -f $GIT_OUTPUT_FILE

cat $OUTPUT