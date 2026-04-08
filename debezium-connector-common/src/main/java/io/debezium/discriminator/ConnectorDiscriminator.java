/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.discriminator;

public class ConnectorDiscriminator {

    public ConnectorDiscriminator() {
    }

    public static boolean isForCurrentConnector(io.debezium.config.Configuration configuration, Class<?> implementationClass) {

        io.debezium.annotation.ConnectorSpecific annotation = implementationClass.getAnnotation(io.debezium.annotation.ConnectorSpecific.class);
        if (annotation == null) {
            return false;
        }
        Class<? extends io.debezium.connector.common.BaseSourceConnector> connectorClass = annotation.connector();

        return connectorClass == getConnectorClass(configuration);
    }

    public static Class<?> getConnectorClass(io.debezium.config.Configuration config) {

        try {
            return Class.forName(config.getString("connector.class"));
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}