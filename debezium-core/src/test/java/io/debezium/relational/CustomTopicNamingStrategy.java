/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Properties;

import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.spi.schema.DataCollectionId;

public class CustomTopicNamingStrategy extends SchemaTopicNamingStrategy {

    private String dataChangeTopic;
    private String recordSchemaPrefix;

    public CustomTopicNamingStrategy(Properties props) {
        super(props);
    }

    public CustomTopicNamingStrategy(Properties props, String dataChangeTopic, String recordSchemaPrefix) {
        super(props);
        this.dataChangeTopic = dataChangeTopic;
        this.recordSchemaPrefix = recordSchemaPrefix;
    }

    @Override
    public String dataChangeTopic(DataCollectionId id) {
        return null != dataChangeTopic ? dataChangeTopic : super.dataChangeTopic(id);
    }

    @Override
    public String recordSchemaPrefix(DataCollectionId id) {
        return null != recordSchemaPrefix ? recordSchemaPrefix : super.recordSchemaPrefix(id);
    }
}
