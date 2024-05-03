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

# Add the required jar files to be packaged with the base connector
RUn cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && curl -sLo kafka-connect-avro-converter-7.6.0 https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.6.0/kafka-connect-avro-converter-7.6.0.jar
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

