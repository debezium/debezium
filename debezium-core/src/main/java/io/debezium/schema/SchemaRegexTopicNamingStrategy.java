/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import java.util.Properties;

import io.debezium.common.annotation.Incubating;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Implement a regex expression strategy to determine data event topic names using {@link DataCollectionId#schemaParts()}.
 *
 * @author Harvey Yue
 */
@Incubating
public class SchemaRegexTopicNamingStrategy extends AbstractRegexTopicNamingStrategy {

    public SchemaRegexTopicNamingStrategy(Properties props) {
        super(props);
    }

    @Override
    public String getOriginTopic(DataCollectionId id) {
        return getSchemaPartsTopicName(id);
    }
}
