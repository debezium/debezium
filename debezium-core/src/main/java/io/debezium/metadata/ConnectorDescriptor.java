/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metadata;

public class ConnectorDescriptor {
    public final String id;
    public final String name;
    public final String version;

    public ConnectorDescriptor(String id, String name, String version) {
        this.id = id;
        this.name = name;
        this.version = version;
    }

}