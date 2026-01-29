/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.metadata;

import io.debezium.config.Field;
import io.debezium.connector.mariadb.MariaDbConnector;
import io.debezium.connector.mariadb.MariaDbConnectorConfig;
import io.debezium.connector.mariadb.Module;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;

/**
 * @author Chris Cranford
 */
public class MariaDbConnectorMetadata implements ComponentMetadata {
    @Override
    public ComponentDescriptor getComponentDescriptor() {
        return new ComponentDescriptor(MariaDbConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getComponentFields() {
        return MariaDbConnectorConfig.ALL_FIELDS;
    }
}
