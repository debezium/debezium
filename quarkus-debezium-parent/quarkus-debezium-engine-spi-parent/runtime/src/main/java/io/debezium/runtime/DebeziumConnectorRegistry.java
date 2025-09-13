/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.runtime;

import java.util.List;

public interface DebeziumConnectorRegistry {
    /**
     *
     * @return the {@link Connector} type for this registry
     */
    Connector connector();

    /**
     *
     * @param group
     * @return the {@link Debezium} engine instance assigned to a {@link CaptureGroup}
     */
    Debezium get(CaptureGroup group);

    /**
     *
     * @return the {@link Debezium} engines inside the registry
     */
    List<Debezium> engines();
}
