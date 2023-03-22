# This docker file is used by the End to End test suite, SourcePipelineInvocationContextProvider
# It's goal is to build a debezium/connect based image that has all the necessary bits in order
# to perform end to end tests.
FROM debezium/connect:nightly

# These should point to the driver version to be used
ENV MAVEN_DEP_DESTINATION=$KAFKA_HOME/libs \
    ORACLE_JDBC_REPO="com/oracle/database/jdbc" \
    ORACLE_JDBC_GROUP="ojdbc8" \
    ORACLE_JDBC_VERSION="21.6.0.0" \
    ORACLE_JDBC_MD5=312e6f4ec9932bbf74a4461669970c4b

# Install the Oracle driver from Maven Central
RUN docker-maven-download central "$ORACLE_JDBC_REPO" "$ORACLE_JDBC_GROUP" "$ORACLE_JDBC_VERSION" "$ORACLE_JDBC_MD5"

USER kafka
