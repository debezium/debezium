ARG IMAGE=registry.access.redhat.com/ubi8:latest
FROM $IMAGE

LABEL maintainer="Debezium QE"

#
# Set the source path for kafka, debezium connectors and data directories.
#
ARG KAFKA_SOURCE_PATH
ARG DEBEZIUM_CONNECTORS
ARG DEBEZIUM_VERSION=1.7

ENV KAFKA_HOME=/opt/kafka \
    ZK_DATA=/var/lib/zookeeper \
    KAFKA_DATA=/var/lib/kafka \
    KAFKA_CONNECT_PLUGINS=$KAFKA_HOME/new-connector-plugins

#
# Install openjdk-17, iproute and unzip
#
RUN dnf -y install java-17-openjdk
RUN dnf -y install unzip
RUN dnf -y install nmap
RUN dnf -y install iproute && dnf clean all

#
# Create a user and home directory for Kafka
#
USER root

RUN groupadd -r kafka -g 1001 && \
    useradd -u 1001 -r -g kafka -m -d $KAFKA_HOME -s /sbin/nologin -c "Kafka user" kafka && \
    chmod -R 755 $KAFKA_HOME

RUN mkdir $KAFKA_DATA && \
    mkdir $ZK_DATA

#
# Set up new plugins directory
#
RUN mkdir "$KAFKA_CONNECT_PLUGINS"

#
# Change ownership 
#
RUN chown -R 1001:kafka $KAFKA_HOME && \
    chown -R 1001:kafka $KAFKA_DATA && \
    chown -R 1001:kafka $ZK_DATA

#
# Add and unzip Kafka
#
RUN mkdir /tmp/zipped_kafka /tmp/extracted_kafka
ADD ${KAFKA_SOURCE_PATH} /tmp/zipped_kafka
RUN unzip /tmp/zipped_kafka/*.zip -d /tmp/extracted_kafka

#
# Move kafka from directory nested in another directory to $KAFKA_HOME and delete tmp directories
#
RUN mv /tmp/extracted_kafka/*/* $KAFKA_HOME
RUN rm -rf /tmp/zipped_kafka /tmp/extracted_kafka

#
# Add Debezium connectors to connector-plugns
#
RUN mkdir $KAFKA_HOME/connector-plugins
COPY $DEBEZIUM_CONNECTORS $KAFKA_HOME/connector-plugins/

COPY metrics.yaml $KAFKA_HOME/config


#
# Download kafka and kafka-connect start scripts
#
RUN mkdir /scripts
RUN curl -L https://raw.githubusercontent.com/debezium/container-images/main/kafka/${DEBEZIUM_VERSION}/docker-entrypoint.sh -o /scripts/kafka-start.sh
RUN curl -L https://raw.githubusercontent.com/debezium/container-images/main/connect-base/${DEBEZIUM_VERSION}/docker-entrypoint.sh -o /scripts/kafka-connect-start.sh


#
# Setup original config directories
#
RUN mkdir $KAFKA_HOME/config.orig &&\
    mv $KAFKA_HOME/config/* $KAFKA_HOME/config.orig &&\
    chown -R 1001:kafka $KAFKA_HOME/config.orig


# Remove unnecessary files
RUN rm -f $KAFKA_HOME/libs/*-{sources,javadoc,scaladoc}.jar*


#
# Allow random UID to use Kafka
#
RUN chmod -R g+w,o+w $KAFKA_HOME

# Set the working directory to the Kafka home directory
WORKDIR $KAFKA_HOME

#
# Set up volumes for the data and logs directories
#
VOLUME ["${ZK_DATA}","${KAFKA_DATA}","${KAFKA_HOME}/config", "${KAFKA_CONNECT_PLUGINS}"]

#
# Copy entrypoint script and switch user
#
COPY ./docker-entrypoint.sh /
RUN chmod 755 /docker-entrypoint.sh && chmod -R 755 /scripts
USER kafka
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["start"]
