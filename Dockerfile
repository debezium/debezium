# On your terminal, run the following to build the image:
# mvn clean package -Dquick

FROM debezium/connect:2.5.2.Final

WORKDIR $KAFKA_CONNECT_PLUGINS_DIR
RUN rm -f debezium-connector-postgres/debezium-connector-postgres-*.jar
WORKDIR /

# Copy the Debezium Connector for Postgres adapted for YugabyteDB
COPY debezium-connector-postgres/target/debezium-connector-postgres-*.jar $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres

# Set the TLS version to be used by Kafka processes
ENV KAFKA_OPTS="-Djdk.tls.client.protocols=TLSv1.2"

# Add the required jar files to be packaged with the base connector
RUN cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && curl -sLo kafka-connect-jdbc-10.6.5.jar https://github.com/yugabyte/kafka-connect-jdbc/releases/download/10.6.5-CUSTOM/kafka-connect-jdbc-10.6.5.jar
RUN cd $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres && curl -sLo jdbc-yugabytedb-42.3.5-yb-1.jar https://repo1.maven.org/maven2/com/yugabyte/jdbc-yugabytedb/42.3.5-yb-1/jdbc-yugabytedb-42.3.5-yb-1.jar

# Add Jmx agent and metrics pattern file to expose the metrics info
RUN mkdir /kafka/etc && cd /kafka/etc && curl -so jmx_prometheus_javaagent-0.17.2.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.17.2/jmx_prometheus_javaagent-0.17.2.jar

ADD metrics.yml /etc/jmx-exporter/

ENV CLASSPATH=$KAFKA_HOME

