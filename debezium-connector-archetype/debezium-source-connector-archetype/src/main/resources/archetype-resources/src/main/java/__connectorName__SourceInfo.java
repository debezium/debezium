/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.time.Instant;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.BaseSourceInfo;

/**
 * Carries the {@code source} metadata block included in every change event.
 *
 * <p>Add connector-specific fields here (e.g. file name, position, sequence number).
 * Exposed fields must also be registered in {@link ${connectorName}SourceInfoStructMaker}.
 */
public class ${connectorName}SourceInfo extends BaseSourceInfo {

    private final CommonConnectorConfig config;

    public ${connectorName}SourceInfo(CommonConnectorConfig config) {
        super(config);
        this.config = config;
    }

    @Override
    protected Instant timestamp() {
        return Instant.now();
    }

    @Override
    protected String database() {
        return config.getLogicalName();
    }
}
