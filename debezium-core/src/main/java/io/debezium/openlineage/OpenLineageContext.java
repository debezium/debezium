/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import java.util.UUID;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.UUIDUtils;

public class OpenLineageContext {

    private final UUID runUuid;
    private final OpenLineage openLineage;
    private final DebeziumOpenLineageConfiguration configuration;
    private final OpenLineageJobIdentifier jobIdentifier;

    public OpenLineageContext(OpenLineage openLineage, DebeziumOpenLineageConfiguration configuration, OpenLineageJobIdentifier jobIdentifier) {
        this.openLineage = openLineage;
        this.configuration = configuration;
        this.jobIdentifier = jobIdentifier;
        runUuid = UUIDUtils.generateNewUUID();
    }

    public OpenLineage getOpenLineage() {
        return openLineage;
    }

    public DebeziumOpenLineageConfiguration getConfiguration() {
        return configuration;
    }

    public UUID getRunUuid() {
        return runUuid;
    }

    public OpenLineageJobIdentifier getJobIdentifier() {
        return jobIdentifier;
    }
}
