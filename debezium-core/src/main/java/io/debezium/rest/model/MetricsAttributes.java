/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.rest.model;

import java.util.List;

/**
 * Interface containing the attributes names for the metrics exposed via REST API
 */
public interface MetricsAttributes {
    List<String> CONNECTION_ATTRIBUTES = List.of("Connected");
    List<String> CONNECTOR_ATTRIBUTES = List.of("MilliSecondsSinceLastEvent", "TotalNumberOfEventsSeen");

    List<String> getConnectionAttributes();

    List<String> getConnectorAttributes();
}
