/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metadata;

import io.debezium.config.Field;

public interface ConnectorMetadata {

    ConnectorDescriptor getConnectorDescriptor();

    Field.Set getConnectorFields();
}
