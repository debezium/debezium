/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.converters;

import io.debezium.connector.mysql.Module;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.SerializerType;

/**
 * An implementation of {@link CloudEventsProvider} for MySQL.
 *
 * @author Chris Cranford
 */
public class MySqlCloudEventsProvider implements CloudEventsProvider {
    @Override
    public String getName() {
        return Module.name();
    }

    @Override
    public CloudEventsMaker createMaker(RecordAndMetadata recordAndMetadata, SerializerType dataContentType, String dataSchemaUriBase,
                                        String cloudEventsSchemaName) {
        return new MySqlCloudEventsMaker(recordAndMetadata, dataContentType, dataSchemaUriBase, cloudEventsSchemaName);
    }
}
