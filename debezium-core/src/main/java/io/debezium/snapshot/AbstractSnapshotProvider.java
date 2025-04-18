/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceConnector;

public class AbstractSnapshotProvider {

    protected boolean isForCurrentConnector(Configuration configuration, Class<?> implementationClass) {

        ConnectorSpecific annotation = implementationClass.getAnnotation(ConnectorSpecific.class);
        if (annotation == null) {
            return false;
        }
        Class<? extends BaseSourceConnector> connectorClass = annotation.connector();

        return connectorClass == getConnectorClass(configuration);
    }

    private Class<?> getConnectorClass(Configuration config) {

        try {
            return Class.forName(config.getString("connector.class"));
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
