#!/bin/bash

# Exit immediately if a *pipeline* returns a non-zero status. (Add -x for command tracing)
set -e

if [[ -z "$SENSITIVE_PROPERTIES" ]]; then
    SENSITIVE_PROPERTIES="CONNECT_SASL_JAAS_CONFIG,CONNECT_CONSUMER_SASL_JAAS_CONFIG,CONNECT_PRODUCER_SASL_JAAS_CONFIG,CONNECT_SSL_KEYSTORE_PASSWORD,CONNECT_PRODUCER_SSL_KEYSTORE_PASSWORD,CONNECT_SSL_TRUSTSTORE_PASSWORD,CONNECT_PRODUCER_SSL_TRUSTSTORE_PASSWORD,CONNECT_SSL_KEY_PASSWORD,CONNECT_PRODUCER_SSL_KEY_PASSWORD,CONNECT_CONSUMER_SSL_TRUSTSTORE_PASSWORD,CONNECT_CONSUMER_SSL_KEYSTORE_PASSWORD,CONNECT_CONSUMER_SSL_KEY_PASSWORD"
fi

if [[ -z "$BOOTSTRAP_SERVERS" ]]; then
    # Look for any environment variables set by Docker container linking. For example, if the container
    # running Kafka were aliased to 'kafka' in this container, then Docker should have created several envs,
    # such as 'KAFKA_PORT_9092_TCP'. If so, then use that to automatically set the 'bootstrap.servers' property.
    BOOTSTRAP_SERVERS=$(env | grep .*PORT_9092_TCP= | sed -e 's|.*tcp://||' | uniq | paste -sd ,)
fi

if [[ "x$BOOTSTRAP_SERVERS" = "x" ]]; then
    export BOOTSTRAP_SERVERS=0.0.0.0:9092
fi

echo "Using BOOTSTRAP_SERVERS=$BOOTSTRAP_SERVERS"


if [[ -z "$HOST_NAME" ]]; then
    HOST_NAME=$(ip addr | grep 'BROADCAST' -A2 | tail -n1 | awk '{print $2}' | cut -f1  -d'/')
fi

: ${REST_PORT:=8083}
: ${REST_HOST_NAME:=$HOST_NAME}
: ${ADVERTISED_PORT:=8083}
: ${ADVERTISED_HOST_NAME:=$HOST_NAME}
: ${GROUP_ID:=1}
: ${OFFSET_FLUSH_INTERVAL_MS:=60000}
: ${OFFSET_FLUSH_TIMEOUT_MS:=5000}
: ${SHUTDOWN_TIMEOUT:=10000}
: ${KEY_CONVERTER:=org.apache.kafka.connect.json.JsonConverter}
: ${VALUE_CONVERTER:=org.apache.kafka.connect.json.JsonConverter}
: ${INTERNAL_KEY_CONVERTER:=org.apache.kafka.connect.json.JsonConverter}
: ${INTERNAL_VALUE_CONVERTER:=org.apache.kafka.connect.json.JsonConverter}
: ${ENABLE_APICURIO_CONVERTERS:=false}
: ${ENABLE_DEBEZIUM_SCRIPTING:=false}
export CONNECT_REST_ADVERTISED_PORT=$ADVERTISED_PORT
export CONNECT_REST_ADVERTISED_HOST_NAME=$ADVERTISED_HOST_NAME
export CONNECT_REST_PORT=$REST_PORT
export CONNECT_REST_HOST_NAME=$REST_HOST_NAME
export CONNECT_BOOTSTRAP_SERVERS=$BOOTSTRAP_SERVERS
export CONNECT_GROUP_ID=$GROUP_ID
export CONNECT_CONFIG_STORAGE_TOPIC=$CONFIG_STORAGE_TOPIC
export CONNECT_OFFSET_STORAGE_TOPIC=$OFFSET_STORAGE_TOPIC
if [[ -n "$STATUS_STORAGE_TOPIC" ]]; then
    export CONNECT_STATUS_STORAGE_TOPIC=$STATUS_STORAGE_TOPIC
fi
export CONNECT_KEY_CONVERTER=$KEY_CONVERTER
export CONNECT_VALUE_CONVERTER=$VALUE_CONVERTER
export CONNECT_INTERNAL_KEY_CONVERTER=$INTERNAL_KEY_CONVERTER
export CONNECT_INTERNAL_VALUE_CONVERTER=$INTERNAL_VALUE_CONVERTER
export CONNECT_TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS=$SHUTDOWN_TIMEOUT
export CONNECT_OFFSET_FLUSH_INTERVAL_MS=$OFFSET_FLUSH_INTERVAL_MS
export CONNECT_OFFSET_FLUSH_TIMEOUT_MS=$OFFSET_FLUSH_TIMEOUT_MS
if [[ -n "$HEAP_OPTS" ]]; then
    export KAFKA_HEAP_OPTS=$HEAP_OPTS
fi
unset HOST_NAME
unset REST_PORT
unset REST_HOST_NAME
unset ADVERTISED_PORT
unset ADVERTISED_HOST_NAME
unset GROUP_ID
unset OFFSET_FLUSH_INTERVAL_MS
unset OFFSET_FLUSH_TIMEOUT_MS
unset SHUTDOWN_TIMEOUT
unset KEY_CONVERTER
unset VALUE_CONVERTER
unset INTERNAL_KEY_CONVERTER
unset INTERNAL_VALUE_CONVERTER
unset HEAP_OPTS
unset MD5HASH
unset SCALA_VERSION

#
# Set up the classpath with all the plugins ...
#
if [ -z "$CONNECT_PLUGIN_PATH" ]; then
    CONNECT_PLUGIN_PATH=$KAFKA_CONNECT_PLUGINS
fi
echo "Plugins are loaded from $CONNECT_PLUGIN_PATH"

if [[ "${ENABLE_APICURIO_CONVERTERS}" == "true" && ! -z "$EXTERNAL_LIBS_DIR" && -d "$EXTERNAL_LIBS_DIR/apicurio" ]] ; then
    plugin_dirs=(${CONNECT_PLUGIN_PATH//,/ })
    for plugin_dir in $plugin_dirs ; do
        for connector in $plugin_dir/*/ ; do
            ln -snf $EXTERNAL_LIBS_DIR/apicurio/* "$connector"
        done
    done
    echo "Apicurio connectors enabled!"
else
    plugin_dirs=(${CONNECT_PLUGIN_PATH//,/ })
    for plugin_dir in $plugin_dirs ; do
        find $plugin_dir/ -lname "$EXTERNAL_LIBS_DIR/apicurio/*" -exec rm -f {} \;
    done
fi

if [[ "${ENABLE_DEBEZIUM_SCRIPTING}" == "true" && ! -f "$EXTERNAL_LIBS_DIR" && -d "$EXTERNAL_LIBS_DIR/debezium-scripting" ]] ; then
    plugin_dirs=(${CONNECT_PLUGIN_PATH//,/ })
    for plugin_dir in $plugin_dirs ; do
        for connector in $plugin_dir/*/ ; do
            ln -snf $EXTERNAL_LIBS_DIR/debezium-scripting/*.jar "$connector"
        done
    done
    echo "Debezium Scripting enabled!"
else
    plugin_dirs=(${CONNECT_PLUGIN_PATH//,/ })
    for plugin_dir in $plugin_dirs ; do
        find $plugin_dir/ -lname "$EXTERNAL_LIBS_DIR/debezium-scripting/*" -exec rm -f {} \;
    done
fi

#
# Set up the JMX options
#
: ${JMXAUTH:="false"}
: ${JMXSSL:="false"}
if [[ -n "$JMXPORT" && -n "$JMXHOST" ]]; then
    echo "Enabling JMX on ${JMXHOST}:${JMXPORT}"
    export KAFKA_JMX_OPTS="-Djava.rmi.server.hostname=${JMXHOST} -Dcom.sun.management.jmxremote.rmi.port=${JMXPORT} -Dcom.sun.management.jmxremote.port=${JMXPORT} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=${JMXAUTH} -Dcom.sun.management.jmxremote.ssl=${JMXSSL} "
fi

#
# Setup Flight Recorder
#
if [[ "$ENABLE_JFR" == "true" ]]; then
    JFR_OPTS="-XX:StartFlightRecording"
    opt_delimiter="="
    for VAR in $(env); do
      if [[ "$VAR" == JFR_RECORDING_* ]]; then
        opt_name=`echo "$VAR" | sed -r "s/^JFR_RECORDING_([^=]*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ -`
        opt_value=`echo "$VAR" | sed -r "s/^JFR_RECORDING_[^=]*=(.*)/\1/g"`
        JFR_OPTS="${JFR_OPTS}${opt_delimiter}${opt_name}=${opt_value}"
        opt_delimiter=","
      fi
    done
    opt_delimiter=" -XX:FlightRecorderOptions="
    for VAR in $(env); do
      if [[ "$VAR" == JFR_OPT_* ]]; then
        opt_name=`echo "$VAR" | sed -r "s/^JFR_OPT_([^=]*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ -`
        opt_value=`echo "$VAR" | sed -r "s/^JFR_OPT_[^=]*=(.*)/\1/g"`
        JFR_OPTS="${JFR_OPTS}${opt_delimiter}${opt_name}=${opt_value}"
        opt_delimiter=","
      fi
    done
    echo "Java Flight Recorder enabled and configured with options $JFR_OPTS"
    if [[ -n "$KAFKA_OPTS" ]]; then
        export KAFKA_OPTS="$KAFKA_OPTS $JFR_OPTS"
    else
        export KAFKA_OPTS="$JFR_OPTS"
    fi
    unset JFR_OPTS
fi


#
# Setup Kafka Prometheus Metrics
#
if [ "$ENABLE_JMX_EXPORTER" = "true" ]; then
  KAFKA_OPTS="${KAFKA_OPTS} -javaagent:$(ls "$KAFKA_HOME"/libs/jmx_prometheus_javaagent*.jar)=9404:$KAFKA_HOME/config/metrics.yaml"
  export KAFKA_OPTS
fi


#
# Make sure the directory for logs exists ...
#
mkdir -p ${KAFKA_DATA}/$KAFKA_BROKER_ID

# Process the argument to this container ...
case $1 in
    start)
        if [[ "x$CONNECT_BOOTSTRAP_SERVERS" = "x" ]]; then
            echo "The BOOTSTRAP_SERVERS variable must be set, or the container must be linked to one that runs Kafka."
            exit 1
        fi

        if [[ "x$CONNECT_GROUP_ID" = "x" ]]; then
            echo "The GROUP_ID must be set to an ID that uniquely identifies the Kafka Connect cluster these workers belong to."
            echo "Ensure this is unique for all groups that work with a Kafka cluster."
            exit 1
        fi

        if [[ "x$CONNECT_CONFIG_STORAGE_TOPIC" = "x" ]]; then
            echo "The CONFIG_STORAGE_TOPIC variable must be set to the name of the topic where connector configurations will be stored."
            echo "This topic must have a single partition, be highly replicated (e.g., 3x or more) and should be configured for compaction."
            exit 1
        fi

        if [[ "x$CONNECT_OFFSET_STORAGE_TOPIC" = "x" ]]; then
            echo "The OFFSET_STORAGE_TOPIC variable must be set to the name of the topic where connector offsets will be stored."
            echo "This topic should have many partitions (e.g., 25 or 50), be highly replicated (e.g., 3x or more) and be configured for compaction."
            exit 1
        fi

        if [[ "x$CONNECT_STATUS_STORAGE_TOPIC" = "x" ]]; then
            echo "WARNING: it is recommended to specify the STATUS_STORAGE_TOPIC variable for defining the name of the topic where connector statuses will be stored."
            echo "This topic may have multiple partitions, be highly replicated (e.g., 3x or more) and should be configured for compaction."
            echo "As no value is given, the default of 'connect-status' will be used."
        fi

        echo "Using the following environment variables:"
        echo "      GROUP_ID=$CONNECT_GROUP_ID"
        echo "      CONFIG_STORAGE_TOPIC=$CONNECT_CONFIG_STORAGE_TOPIC"
        echo "      OFFSET_STORAGE_TOPIC=$CONNECT_OFFSET_STORAGE_TOPIC"
        if [[ "x$CONNECT_STATUS_STORAGE_TOPIC" != "x" ]]; then
            echo "      STATUS_STORAGE_TOPIC=$CONNECT_STATUS_STORAGE_TOPIC"
        fi
        echo "      BOOTSTRAP_SERVERS=$CONNECT_BOOTSTRAP_SERVERS"
        echo "      REST_HOST_NAME=$CONNECT_REST_HOST_NAME"
        echo "      REST_PORT=$CONNECT_REST_PORT"
        echo "      ADVERTISED_HOST_NAME=$CONNECT_REST_ADVERTISED_HOST_NAME"
        echo "      ADVERTISED_PORT=$CONNECT_REST_ADVERTISED_PORT"
        echo "      KEY_CONVERTER=$CONNECT_KEY_CONVERTER"
        echo "      VALUE_CONVERTER=$CONNECT_VALUE_CONVERTER"
        echo "      INTERNAL_KEY_CONVERTER=$CONNECT_INTERNAL_KEY_CONVERTER"
        echo "      INTERNAL_VALUE_CONVERTER=$CONNECT_INTERNAL_VALUE_CONVERTER"
        echo "      OFFSET_FLUSH_INTERVAL_MS=$CONNECT_OFFSET_FLUSH_INTERVAL_MS"
        echo "      OFFSET_FLUSH_TIMEOUT_MS=$CONNECT_OFFSET_FLUSH_TIMEOUT_MS"
        echo "      SHUTDOWN_TIMEOUT=$CONNECT_TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS"

        # Copy config files if not provided in volume
        cp -rn $KAFKA_HOME/config.orig/* $KAFKA_HOME/config

        #
        # Configure the log files ...
        #
        if [[ -n "$CONNECT_LOG4J_LOGGERS" ]]; then
            sed -i -r -e "s|^(log4j.rootLogger)=.*|\1=${CONNECT_LOG4J_LOGGERS}|g" $KAFKA_HOME/config/log4j.properties
            unset CONNECT_LOG4J_LOGGERS
        fi
        env | grep '^CONNECT_LOG4J' | while read -r VAR;
        do
          env_var=`echo "$VAR" | sed -r "s/([^=]*)=.*/\1/g"`
          prop_name=`echo "$VAR" | sed -r "s/^CONNECT_([^=]*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ .`
          prop_value=`echo "$VAR" | sed -r "s/^CONNECT_[^=]*=(.*)/\1/g"`
          if egrep -q "(^|^#)$prop_name=" $KAFKA_HOME/config/log4j.properties; then
              #note that no config names or values may contain an '@' char
              sed -r -i "s@(^|^#)($prop_name)=(.*)@\2=${prop_value}@g" $KAFKA_HOME/config/log4j.properties
          else
              echo "$prop_name=${prop_value}" >> $KAFKA_HOME/config/log4j.properties
          fi
          if [[ "$SENSITIVE_PROPERTIES" = *"$env_var"* ]]; then
              echo "--- Setting logging property from $env_var: $prop_name=[hidden]"
          else
             echo "--- Setting logging property from $env_var: $prop_name=${prop_value}"
          fi
          unset $env_var
        done
        if [[ -n "$LOG_LEVEL" ]]; then
            sed -i -r -e "s|=INFO, stdout|=$LOG_LEVEL, stdout|g" $KAFKA_HOME/config/log4j.properties
            sed -i -r -e "s|^(log4j.appender.stdout.threshold)=.*|\1=${LOG_LEVEL}|g" $KAFKA_HOME/config/log4j.properties
        fi
        export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/config/log4j.properties"

        #
        # Process all environment variables that start with 'CONNECT_'
        #
        env | while read -r VAR;
        do
          env_var=`echo "$VAR" | sed -r "s/([^=]*)=.*/\1/g"`
          if [[ $env_var =~ ^CONNECT_ ]]; then
            prop_name=`echo "$VAR" | sed -r "s/^CONNECT_([^=]*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ .`
            prop_value=`echo "$VAR" | sed -r "s/^CONNECT_[^=]*=(.*)/\1/g"`
            if egrep -q "(^|^#)$prop_name=" $KAFKA_HOME/config/connect-distributed.properties; then
                #note that no config names or values may contain an '@' char
                sed -r -i "s@(^|^#)($prop_name)=(.*)@\2=${prop_value}@g" $KAFKA_HOME/config/connect-distributed.properties
            else
                # echo "Adding property $prop_name=${prop_value}"
                echo "$prop_name=${prop_value}" >> $KAFKA_HOME/config/connect-distributed.properties
            fi
            if [[ "$SENSITIVE_PROPERTIES" = *"$env_var"* ]]; then
                echo "--- Setting property from $env_var: $prop_name=[hidden]"
            else
                echo "--- Setting property from $env_var: $prop_name=${prop_value}"
            fi
          fi
        done

        #
        # Execute the Kafka Connect distributed service, replacing this shell process with the specified program ...
        #
        exec $KAFKA_HOME/bin/connect-distributed.sh $KAFKA_HOME/config/connect-distributed.properties
        ;;
esac

# Otherwise just run the specified command
exec "$@"
