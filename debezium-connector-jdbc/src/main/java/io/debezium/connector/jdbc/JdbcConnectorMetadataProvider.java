/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import io.debezium.config.Field;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;
import io.debezium.metadata.ComponentMetadataProvider;

public class JdbcConnectorMetadataProvider implements ComponentMetadataProvider {

    @Override
    public ComponentMetadata getConnectorMetadata() {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                return new ComponentDescriptor(JdbcSinkConnector.class.getName(), Module.version());
            }

            @Override
            public Field.Set getComponentFields() {
                return JdbcSinkConnectorConfig.ALL_FIELDS;
            }
        };
    }
}
