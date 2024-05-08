/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;

/**
 * Converts the connector SourceInfo into publicly visible source field of the message.
 * It is expected that when the connector SourceInfo schema changes then a legacy class will be created
 * that could be enable in connector config to provide old format of the source.
 *
 * @author Jiri Pechanec
 *
 * @param <T> SourceInfo specific for the connector
 */
public interface SourceInfoStructMaker<T extends AbstractSourceInfo> {

    void init(String connector, String version, CommonConnectorConfig connectorConfig);

    /**
     * Returns the schema of the source info.
     */
    Schema schema();

    /**
     * Converts the connector's source info into the struct to be included in the message as the source field.
     *
     * @param sourceInfo
     * @return the converted struct
     */
    Struct struct(T sourceInfo);
}
