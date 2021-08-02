#!/bin/bash

# Exit immediately if a *pipeline* returns a non-zero status. (Add -x for command tracing)
set -e

# Copy config files if not provided in volume
cp -rn $KAFKA_HOME/config.orig/* $KAFKA_HOME/config

case $1 in
    zookeeper)
        shift
        # Change the Zookeeper snapshot directory in zookeeper.properities file
        sed -i "s|dataDir=.*|dataDir=${ZK_DATA}|" $KAFKA_HOME/config/zookeeper.properties
        if [ -z "$1" ]; then
            exec $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
        else
            echo "Zookeeper can not have any arguments"        
        fi
        ;;
    kafka)
        shift
        # Set the diretory where the logs for kafka will be stored
        sed -i "s|log.dirs=.*|log.dirs=${KAFKA_DATA}|" $KAFKA_HOME/config/server.properties
        if [ -z "$1" ]; then
            exec /scripts/kafka-start.sh start
        else
            exec /scripts/kafka-start.sh "$@"        
        fi
        ;;
    kafka-connect)
        shift
        # Set the directory with debezium connectors in config file
        echo "plugin.path=$KAFKA_HOME/connector-plugins" >> $KAFKA_HOME/config/connect-distributed.properties

        # If volume with plugins is not empty then use the contents of volume with plugins as connector plugins. Otherwise use default set of plugins 
        if find $KAFKA_CONNECT_PLUGINS -mindepth 1 -maxdepth 1 | read; then
            rm -rf $KAFKA_HOME/connector-plugins/*;
            ls ${KAFKA_CONNECT_PLUGINS}
            cp -R ${KAFKA_CONNECT_PLUGINS}/* $KAFKA_HOME/connector-plugins/ 
        fi    
        export CONNECT_PLUGIN_PATH=$KAFKA_HOME/connector-plugins
        if [ -z "$1" ]; then
            exec /scripts/kafka-connect-start.sh start
        else
            exec /scripts/kafka-connect-start.sh "$@"        
        fi
        ;;
    *)    
        echo "First argument must be either zookeeper, kafka or kafka-connect."
        exit 1
        ;;
esac
