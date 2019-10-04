#/bin/bash

if [ ! -f /asncdctools/src/asncdc.nlk ]; then
rc=1
echo "Waiting for db2inst1 to exist ..."
while [ "$rc" -ne 0 ]
do
   sleep 5
   id db2inst1
   rc=$?
   echo '.'
done

su  -c "/asncdctools/src/dbsetup.sh $DBNAME"   - db2inst1
fi
touch /asncdctools/src/asncdc.nlk

echo "done"