/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * @author David Leibovic
 */
public class ByDatabaseTopicMapper extends TopicMapper {

    public String getTopicName() {
        return topicPrefix + table.id().catalog();
    }

    public void enhanceKeySchema(SchemaBuilder keySchemaBuilder) {
        // Just add the table name as a field ...
        keySchemaBuilder.field("tableName", Schema.STRING_SCHEMA);
    }

    public Map<String, Object> getNonRowFieldsToAddToKey(Schema schema) {
        // Just add the table name as a field ...
        Map<String, Object> nonRowFields = new HashMap<>();
        nonRowFields.put("tableName", table.id().table());
        return nonRowFields;
    }

}
