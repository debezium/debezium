#!/bin/bash
set -x

docker exec -d oracledb /bin/bash /opt/oracle/setPassword.sh top_secret

docker exec oracledb /bin/bash -l -c  'mkdir /opt/oracle/oradata/recovery_area'

sleep 5

docker exec -i oracledb sqlplus /nolog <<- EOF
    CONNECT sys/top_secret AS SYSDBA
    alter system set db_recovery_file_dest_size = 5G;
    alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
    shutdown immediate
    startup mount
    alter database archivelog;
    alter database open;
    archive log list
    exit;
EOF

pushd debezium-testing/tmt/tests/debezium

sleep 10

sqlplus64 sys/top_secret@//localhost:1521/FREE as sysdba @oracle-init/log-init.sql

sleep 10

sqlplus64 sys/top_secret@//localhost:1521/FREE as sysdba @oracle-init/oracle-free-logminer-init.sql

sleep 10

sqlplus64 sys/top_secret@//localhost:1521/FREEPDB1 as sysdba @oracle-init/oracle-free-logminer-pdb.sql

sleep 10

sqlplus64 sys/top_secret@//localhost:1521/FREE as sysdba @oracle-init/oracle-init.sql

sleep 10

sqlplus64 sys/top_secret@//localhost:1521/FREEPDB1 as sysdba @oracle-init/common-user.sql

popd
