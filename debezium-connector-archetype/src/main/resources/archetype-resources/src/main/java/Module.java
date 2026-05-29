/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

/**
 * Information about the ${connectorName} connector module.
 */
public class Module {

    private static final String VERSION = "${version}";

    public static String version() {
        return VERSION;
    }

    public static String name() {
        return "${connectorName.toLowerCase()}";
    }

    public static String contextName() {
        return "${connectorName}";
    }
}
