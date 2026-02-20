/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.metadata;

import io.debezium.config.Field;
import io.debezium.connector.oracle.Module;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.metadata.ComponentDescriptor;
import io.debezium.metadata.ComponentMetadata;

public class OracleConnectorMetadata implements ComponentMetadata {

    @Override
    public ComponentDescriptor getComponentDescriptor() {
        return new ComponentDescriptor(OracleConnector.class.getName(), Module.version());
    }

    @Override
    public Field.Set getComponentFields() {
        return OracleConnectorConfig.ALL_FIELDS;
    }

}
