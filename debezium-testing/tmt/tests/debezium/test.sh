#!/bin/sh -eux

cd ../../../..

echo $PWD

if [ "$TEST_PROFILE" = "mysql" ]
then
  mvn clean install ${MAVEN_ARGS},debezium-connector-mysql \
    -Dversion.mysql.server=${MYSQL_VERSION} \
    -Dmysql.port=4301 \
    -Dmysql.replica.port=4301 \
    -Dmysql.gtid.port=4302 \
    -Dmysql.gtid.replica.port=4303 \
    -P${PROFILE}
elif [ "$TEST_PROFILE" = "postgres" ]
then
  mvn clean install ${MAVEN_ARGS},debezium-connector-postgres \
  -Dpostgres.port=55432 \
  -Dversion.postgres.server=${POSTGRESQL_VERSION} \
  -Ddecoder.plugin.name=${DECODER_PLUGIN} \
  -Dtest.argline="-Ddebezium.test.records.waittime=5"
elif [ "$TEST_PROFILE" = "oracle" ]
then
  source ${HOME}/install-oracle-driver.sh
  export LD_LIBRARY_PATH=$ORACLE_ARTIFACT_DIR
  mvn clean install -U-pl debezium-connector-oracle -am -fae \
    -Poracle-tests                              \
    ${ORACLE_ARG:-}                            \
    -Ddatabase.hostname=0.0.0.0                 \
    -Ddatabase.admin.hostname=0.0.0.0           \
    -Dinstantclient.dir=${ORACLE_ARTIFACT_DIR}     \
    -Dinsecure.repositories=WARN                \
    -Ddatabase.user="c##dbzuser"                \
    -Papicurio
elif [ "$TEST_PROFILE" = "sqlserver" ]
then
  if [ "$SQL_SERVER_VERSION" = "2017" ]
  then
    export DATABASE_IMAGE="mcr.microsoft.com/mssql/server:2017-latest"
  else
    export DATABASE_IMAGE="mcr.microsoft.com/mssql/server:2022-latest"
  fi
  mvn clean install ${MAVEN_ARGS},debezium-connector-sqlserver \
  -Ddocker.db="${DATABASE_IMAGE}"
else
  mvn clean install ${MAVEN_ARGS},debezium-connector-mongodb \
  -Dversion.mongo.server=${MONGODB_VERSION}
fi



