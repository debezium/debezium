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
import io.debezium.util.Collect;

/**
 * Determine data event topic names using {@link DataCollectionId#schemaParts()}.
 *
 * @author Harvey Yue
 */
@Incubating
public class SchemaTopicNamingStrategy extends AbstractTopicNamingStrategy<DataCollectionId> {

    private final boolean multiPartitionMode;

    public SchemaTopicNamingStrategy(Properties props) {
        super(props);
        this.multiPartitionMode = props.get(CommonConnectorConfig.MULTI_PARTITION_MODE) == null ? false
                : Boolean.parseBoolean(props.get(CommonConnectorConfig.MULTI_PARTITION_MODE).toString());
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
        String topicName;
        if (multiPartitionMode) {
            topicName = mkString(Collect.arrayListOf(prefix, id.parts()), delimiter);
        }
        else {
            topicName = mkString(Collect.arrayListOf(prefix, id.schemaParts()), delimiter);
        }
        return topicNames.computeIfAbsent(id, t -> sanitizedTopicName(topicName));
    }
}
