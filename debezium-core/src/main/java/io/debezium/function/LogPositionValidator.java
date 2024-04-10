/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.function;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

@FunctionalInterface
public interface LogPositionValidator {

    /**
     * Validate the stored offset with the position available in the db log.
     * @param partition The current stored partition.
     * @param offsetContext The current stored offset.
     * @param config Connector configuration.
     */
    boolean validate(Partition partition, OffsetContext offsetContext, CommonConnectorConfig config);
}
