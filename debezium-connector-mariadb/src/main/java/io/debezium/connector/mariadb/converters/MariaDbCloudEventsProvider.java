/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.converters;

import io.debezium.connector.mariadb.Module;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.SerializerType;

/**
 * An implementation of {@link CloudEventsProvider} for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbCloudEventsProvider implements CloudEventsProvider {
    @Override
    public String getName() {
        return Module.name();
    }

    @Override
    public CloudEventsMaker createMaker(RecordAndMetadata recordAndMetadata, SerializerType contentType, String dataSchemaUriBase, String cloudEventsSchemaName) {
        return new MariaDbCloudEventsMaker(recordAndMetadata, contentType, dataSchemaUriBase, cloudEventsSchemaName);
    }
}
