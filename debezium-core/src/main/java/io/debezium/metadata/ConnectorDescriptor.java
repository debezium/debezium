/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metadata;

public class ConnectorDescriptor {
    private final String id;
    private final String name;
    private final String className;
    private final String version;

    public ConnectorDescriptor(String id, String name, String className, String version) {
        this.id = id;
        this.name = name;
        this.className = className;
        this.version = version;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getClassName() {
        return className;
    }

    public String getVersion() {
        return version;
    }
}
