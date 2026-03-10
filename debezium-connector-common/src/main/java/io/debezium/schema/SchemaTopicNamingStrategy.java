/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import java.util.Properties;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Determine data event topic names using {@link DataCollectionId#schemaParts()}.
 *
 * @author Harvey Yue
 */
@Incubating
public class SchemaTopicNamingStrategy extends AbstractTopicNamingStrategy<DataCollectionId> {

    public SchemaTopicNamingStrategy(Properties props) {
        super(props);
    }

    public SchemaTopicNamingStrategy(Properties props, boolean multiPartitionMode) {
        super(props);
        this.multiPartitionMode = multiPartitionMode;
    }

    public static SchemaTopicNamingStrategy create(CommonConnectorConfig config) {
        return create(config, false);
    }

    public static SchemaTopicNamingStrategy create(CommonConnectorConfig config, boolean multiPartitionMode) {
        return new SchemaTopicNamingStrategy(config.getConfig().asProperties(), multiPartitionMode);
    }

    @Override
    public String dataChangeTopic(DataCollectionId id) {
        return topicNames.computeIfAbsent(id, t -> sanitizedTopicName(getSchemaPartsTopicName(id)));
    }
}
