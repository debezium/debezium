#!/bin/bash

set -euo

if [ $# -eq 0 ];then
    echo "No release parameters provided"
    exit 1
fi

DIR="$HOME/debezium-contributors"
ALIASES="jenkins-jobs/scripts/config/Aliases.txt"
FILTERS="jenkins-jobs/scripts/config/FilteredNames.txt"

mkdir -p $DIR

CONTRIBUTORS_NAMES="$DIR/$2-DEBEZIUM_CONTRIBUTORS_LIST.txt"
CONTRIBUTORS_LIST_TXT="$DIR/DEBEZIUM_CONTRIBUTORS.txt"
CONTRIBUTORS_ALIASES="$DIR/Aliases.txt"
CONTRIBUTORS_FILTERS="$DIR/FilteredNames.txt"

cp $ALIASES $FILTERS $DIR && cd $DIR

declare -a DEBEZIUM_REPOS=("debezium" "debezium-server" "debezium-operator" "debezium-connector-db2" "debezium-connector-cassandra" "debezium-connector-vitess" "debezium-connector-spanner" "debezium-ui" "container-images" "debezium-connector-informix" "debezium-connector-ibmi" "debezium-connector-cockroachdb" "debezium-operator" "debezium-examples")

for REPO in "${DEBEZIUM_REPOS[@]}";
do
  page_count=$(curl --silent -I "https://api.github.com/repos/debezium/$REPO/compare/$1...$2?page=1&per_page=100" | tr "," $'\n' | grep 'rel="last"' | cut -f1 -d';' | tr "?&" $'\n' | grep -e "^page" | cut -f2 -d=)
  if [[ -z $page_count ]]; then
    curl --silent -X "GET" "https://api.github.com/repos/debezium/$REPO/compare/$1...$2" | jq 'try(.commits[] | {name: .commit.author.name, github_url: .author.html_url}) catch empty' | jq -r '.github_url + " " + .name' >> "$CONTRIBUTORS_NAMES"
  else
    for (( i = 1; i < "$((page_count + 1))"; i++ )); do
      curl --silent -X "GET" "https://api.github.com/repos/debezium/$REPO/compare/$1...$2?page=$i&per_page=100" | jq 'try(.commits[] | {name: .commit.author.name, github_url: .author.html_url}) catch empty' | jq -r '.github_url + " " + .name' >> "$CONTRIBUTORS_NAMES"
    done
  fi
done

while IFS=" " read -r URL NAME;
do
  NAME=`echo "$NAME" | tr "/" " "`
  if [[ -z "$NAME" ]]; then
    if grep -qi "^$URL" $CONTRIBUTORS_ALIASES; then
        REAL_NAME=`grep -i "^$URL" $CONTRIBUTORS_ALIASES | head -1 | awk '{split($0,a,","); print a[2]}'`
        sed -n -e "s/ $URL/$REAL_NAME/p" $CONTRIBUTORS_NAMES >> $CONTRIBUTORS_LIST_TXT
    fi
  else
    if grep -qi "^$NAME" $CONTRIBUTORS_ALIASES; then
        REAL_NAME=`grep -i "^$NAME" $CONTRIBUTORS_ALIASES | head -1 | awk '{split($0,a,","); print a[2]}'`
        sed -n -e "s/ $NAME/\[$REAL_NAME\]/p" $CONTRIBUTORS_NAMES >> $CONTRIBUTORS_LIST_TXT
     elif grep -qi "$NAME" $CONTRIBUTORS_FILTERS; then
        sed -n "/$NAME/d" $CONTRIBUTORS_NAMES >> $CONTRIBUTORS_LIST_TXT
     else
       sed -n -e "s/ $NAME/\[$NAME\]/p" $CONTRIBUTORS_NAMES >> $CONTRIBUTORS_LIST_TXT
     fi
  fi
done < $CONTRIBUTORS_NAMES

sort -t[ -k2 $CONTRIBUTORS_LIST_TXT | uniq > $CONTRIBUTORS_NAMES
sed -e '$!s/$/,/' $CONTRIBUTORS_NAMES

FIX_VERSION=`echo $2 | cut -d "v" -f 2`
echo "List of issues: https://issues.redhat.com/issues/?jql=project%20%3D%20DBZ%20AND%20fixVersion%20%3D%20$FIX_VERSION%20ORDER%20BY%20issuetype%20DESC"

rm -rf $DIR
