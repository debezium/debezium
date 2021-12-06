/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metadata;

import java.util.Collections;
import java.util.List;

import io.debezium.config.Field;

public abstract class AbstractConnectorMetadata {

    public abstract ConnectorDescriptor getConnectorDescriptor();

    public abstract Field.Set getAllConnectorFields();

    public List<String> deprecatedFieldNames() {
        return Collections.emptyList();
    }
}
