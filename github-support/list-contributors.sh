#!/bin/bash

DIR="/tmp/debezium-contributors"
ALIASES="jenkins-jobs/scripts/config/Aliases.txt"
FILTERS="jenkins-jobs/scripts/config/FilteredNames.txt"

mkdir -p $DIR

CONTRIBUTORS_NAMES="$DIR/$TAG2-DEBEZIUM_CONTRIBUTORS_LIST.txt"
CONTRIBUTORS_LIST_JSON="$DIR/CONTRIBUTORS.json"
CONTRIBUTORS_LIST_TXT="$DIR/DEBEZIUM_CONTRIBUTORS.txt"
CONTRIBUTORS_ALIASES="$DIR/Aliases.txt"
CONTRIBUTORS_FILTERS="$DIR/FilteredNames.txt"

cp $ALIASES $FILTERS $DIR && cd $DIR

declare -a DEBEZIUM_REPOS=("debezium" "debezium-connector-db2" "debezium-connector-cassandra" "debezium-connector-vitess" "docker-images")

for REPO in "${DEBEZIUM_REPOS[@]}";
do
  curl -X "GET" "https://api.github.com/repos/debezium/$REPO/compare/$TAG1...$TAG2" -H "Authorization: token $AUTH_TOKEN" | jq '.commits[] | {name: .commit.author.name, github_url: .author.html_url}' &>> "$CONTRIBUTORS_LIST_JSON"
done

jq -r '.github_url + " " + .name' $CONTRIBUTORS_LIST_JSON > $CONTRIBUTORS_NAMES

while IFS=" " read -r URL NAME;
do
  if grep -qi "^$NAME" $CONTRIBUTORS_ALIASES; then
    REAL_NAME=`grep -i "^$NAME" $CONTRIBUTORS_ALIASES | head -1 | awk '{split($0,a,","); print a[2]}'`
    sed -n -e "s/ $NAME/\[$REAL_NAME\]\,/p" $CONTRIBUTORS_NAMES &>> $CONTRIBUTORS_LIST_TXT
  elif grep -qi "^$NAME" $CONTRIBUTORS_FILTERS; then
    sed -n "/$NAME/d" $CONTRIBUTORS_NAMES &>> $CONTRIBUTORS_LIST_TXT
  else
    sed -n -e "s/ $NAME/\[$NAME\]\,/p" $CONTRIBUTORS_NAMES &>> $CONTRIBUTORS_LIST_TXT
  fi
done < $CONTRIBUTORS_NAMES

sort $CONTRIBUTORS_LIST_TXT | uniq > $CONTRIBUTORS_NAMES && mv $CONTRIBUTORS_NAMES /tmp/

rm -rf $DIR
