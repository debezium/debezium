/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

public interface ConfigurationNames {

    String TOPIC_PREFIX_PROPERTY_NAME = "topic.prefix";
    String DATABASE_CONFIG_PREFIX = "database.";
    String DATABASE_HOSTNAME_PROPERTY_NAME = "hostname";
    String DATABASE_PORT_PROPERTY_NAME = "port";
    String MONGODB_CONNECTION_STRING_PROPERTY_NAME = "mongodb.connection.string";
    String TASK_ID_PROPERTY_NAME = "task.id";
}
