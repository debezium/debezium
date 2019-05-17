#!/bin/bash

COPYRIGHT="COPYRIGHT.txt"
CONTRIBUTOR_NAMES="/tmp/repository-CONTRIBUTOR-NAMES.txt"
CONTRIBUTORS="/tmp/repository-CONTRIBUTORS.txt"

FILTERS="jenkins-jobs/scripts/config/FilteredNames.txt"
ALIASES="jenkins-jobs/scripts/config/Aliases.txt"

git log --pretty=format:"%an" . | sort | uniq > $CONTRIBUTOR_NAMES
git log --pretty=format:"%an|%ae" . | sort | uniq > $CONTRIBUTORS

rc=0
while read LINE
do
    # First check whether the contributor name from git history is in the COPYRIGHT file.
    # If the name exists, there is nothing else to do but if it does not we proceed with other checks.
    if ! grep -qi "$LINE" $COPYRIGHT; then

        # Check if the supplied contributor name from git history should be filtered
        # This is where we want users like "Jenkins user" to be ignored.
        # For users that should be skipped entirely, add them to config/FilteredNames.txt
        if ! grep -qi "^$LINE$" $FILTERS; then

            # Check the alias file and transform the git user to an alias if one is defined.
            # If no alias is defined, NAME will be empty.
            NAME=`grep -i "^$LINE," $ALIASES | head -1 | awk '{split($0,a,","); print a[2]}'`

            # Test if the history username was an alias.
            # If it was not a defined alias, write that to the log and return 1
            # If it did resolve as an alias, use the resolved name to see if its in the COPYRIGHT file
            # and return 1 only if the resolved name was not already listed.
            if test -z "$NAME"; then
                echo "Commit $(git log --pretty=format:"%H" --author "$LINE" | head -1) : Did not find [$LINE]."
                rc=1
            else
                if ! grep -qi "$NAME" $COPYRIGHT; then
                    echo "Found [$LINE] (translated to [$NAME]) but the name wasn't in the COPYRIGHT.txt file."
                    rc=1
                fi
            fi
        fi
    fi
done < "$CONTRIBUTOR_NAMES"

exit $rc;