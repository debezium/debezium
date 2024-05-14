# On your terminal, run the following to build the image:
# mvn clean package -Dquick

FROM debezium/connect:2.5.2.Final

WORKDIR $KAFKA_CONNECT_PLUGINS_DIR
RUN rm -f debezium-connector-postgres/debezium-connector-postgres-*.jar
RUN rm -rf debezium-connector-db2
RUN rm -rf debezium-connector-informix
RUN rm -rf debezium-connector-mongodb
RUN rm -rf debezium-connector-jdbc
RUN rm -rf debezium-connector-mysql
RUN rm -rf debezium-connector-oracle
RUN rm -rf debezium-connector-spanner
RUN rm -rf debezium-connector-sqlserver
RUN rm -rf debezium-connector-vitess
RUN rm -f debezium-connector-postgres/debezium-core-2.5.2.Final.jar
WORKDIR /

# Copy the Debezium Connector for Postgres adapted for YugabyteDB
COPY debezium-connector-postgres/target/debezium-connector-postgres-*.jar $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres
COPY debezium-core/target/debezium-core-*.jar $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres

# Set the TLS version to be used by Kafka processes
ENV KAFKA_OPTS="-Djdk.tls.client.protocols=TLSv1.2 -javaagent:/kafka/etc/jmx_prometheus_javaagent-0.17.2.jar=8080:/kafka/etc/jmx-exporter/metrics.yml"

# Package all the Avro related jar files
RUN mkdir -p $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres/avro-supporting-jars/
WORKDIR $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres/avro-supporting-jars
RUN curl -sLo kafka-connect-avro-converter-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.5.3/kafka-connect-avro-converter-7.5.3.jar 
RUN curl -sLo kafka-connect-avro-data-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/7.5.3/kafka-connect-avro-data-7.5.3.jar
RUN curl -sLo kafka-avro-serializer-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.5.3/kafka-avro-serializer-7.5.3.jar
RUN curl -sLo kafka-schema-serializer-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/7.5.3/kafka-schema-serializer-7.5.3.jar 
RUN curl -sLo kafka-schema-converter-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/7.5.3/kafka-schema-converter-7.5.3.jar
RUN curl -sLo kafka-schema-registry-client-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.5.3/kafka-schema-registry-client-7.5.3.jar 
RUN curl -sLo common-config-7.5.3.jar https://packages.confluent.io/maven/io/confluent/common-config/7.5.3/common-config-7.5.3.jar
RUN curl -sLo common-utils-7.5.3.jar https://packages.confluent.io/maven/io/confluent/common-utils/7.5.3/common-utils-7.5.3.jar 

RUN curl -sLo avro-1.11.3.jar https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar
RUN curl -sLo commons-compress-1.21.jar https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.21/commons-compress-1.21.jar
RUN curl -sLo failureaccess-1.0.jar https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0/failureaccess-1.0.jar
RUN curl -sLo guava-33.0.0-jre.jar https://repo1.maven.org/maven2/com/google/guava/guava/33.0.0-jre/guava-33.0.0-jre.jar 
RUN curl -sLo minimal-json-0.9.5.jar https://repo1.maven.org/maven2/com/eclipsesource/minimal-json/minimal-json/0.9.5/minimal-json-0.9.5.jar
RUN curl -sLo re2j-1.6.jar https://repo1.maven.org/maven2/com/google/re2j/re2j/1.6/re2j-1.6.jar 
RUN curl -sLo slf4j-api-1.7.36.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar
RUN curl -sLo snakeyaml-2.0.jar https://repo1.maven.org/maven2/org/yaml/snakeyaml/2.0/snakeyaml-2.0.jar 
RUN curl -sLo swagger-annotations-2.1.10.jar https://repo1.maven.org/maven2/io/swagger/core/v3/swagger-annotations/2.1.10/swagger-annotations-2.1.10.jar 
RUN curl -sLo jackson-databind-2.14.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.14.2/jackson-databind-2.14.2.jar 
RUN curl -sLo jackson-core-2.14.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.14.2/jackson-core-2.14.2.jar 
RUN curl -sLo jackson-annotations-2.14.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.14.2/jackson-annotations-2.14.2.jar 
RUN curl -sLo jackson-dataformat-csv-2.14.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-csv/2.14.2/jackson-dataformat-csv-2.14.2.jar 
RUN curl -sLo logredactor-1.0.12.jar https://repo1.maven.org/maven2/io/confluent/logredactor/1.0.12/logredactor-1.0.12.jar
RUN curl -sLo logredactor-metrics-1.0.12.jar https://repo1.maven.org/maven2/io/confluent/logredactor-metrics/1.0.12/logredactor-metrics-1.0.12.jar
WORKDIR /

# Add the required jar files to be packaged with the base connector
RUN cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && curl -sLo kafka-connect-jdbc-10.6.5.jar https://github.com/yugabyte/kafka-connect-jdbc/releases/download/10.6.5-CUSTOM.4/kafka-connect-jdbc-10.6.5-CUSTOM.4.jar
RUN cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && curl -sLo jdbc-yugabytedb-42.3.5-yb-1.jar https://repo1.maven.org/maven2/com/yugabyte/jdbc-yugabytedb/42.3.5-yb-1/jdbc-yugabytedb-42.3.5-yb-1.jar
RUN cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && curl -sLo transforms-for-apache-kafka-connect-1.5.0.zip https://github.com/Aiven-Open/transforms-for-apache-kafka-connect/releases/download/v1.5.0/transforms-for-apache-kafka-connect-1.5.0.zip
RUN cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && unzip transforms-for-apache-kafka-connect-1.5.0.zip

# Add Jmx agent and metrics pattern file to expose the metrics info
RUN mkdir /kafka/etc && cd /kafka/etc && curl -so jmx_prometheus_javaagent-0.17.2.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.17.2/jmx_prometheus_javaagent-0.17.2.jar

COPY metrics.yml /kafka/etc/jmx-exporter/

ENV CLASSPATH=$KAFKA_HOME
ENV JMXHOST=localhost
ENV JMXPORT=1976

# properties file having instructions to roll over log files in case the size exceeds a given limit
COPY log4j.properties $KAFKA_HOME/config/log4j.properties

