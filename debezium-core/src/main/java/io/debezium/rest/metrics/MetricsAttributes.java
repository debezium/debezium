/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest.metrics;

import java.util.List;

/**
 * Class containing the attributes names for the metrics exposed via REST API
 */
public class MetricsAttributes {
    private static final List<String> CONNECTION_ATTRIBUTES = List.of("Connected");
    private static final List<String> CONNECTOR_ATTRIBUTES = List.of("MilliSecondsSinceLastEvent", "TotalNumberOfEventsSeen");

    public static List<String> getConnectionAttributes() {
        return CONNECTION_ATTRIBUTES;
    }

    public static List<String> getConnectorAttributes() {
        return CONNECTOR_ATTRIBUTES;
    }
}
