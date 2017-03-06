/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.relational.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * @author David Leibovic
 */
public class ByDatabaseTopicMapper extends TopicMapper {

    public String getTopicName(String topicPrefix, Table table) {
        return topicPrefix + table.id().catalog();
    }

    public void enhanceKeySchema(SchemaBuilder keySchemaBuilder) {
        // Now that multiple tables can share a topic, the Key Schema can no longer consist of solely the record's
        // primary / unique key fields, since they are not guaranteed to be unique across tables.
        keySchemaBuilder.field("__dbz__tableName", Schema.STRING_SCHEMA);
    }

    public Map<String, Object> getNonRowFieldsToAddToKey(Schema schema, Table table) {
        // Just add the table name as a field ...
        Map<String, Object> nonRowFields = new HashMap<>();
        nonRowFields.put("__dbz__tableName", table.id().table());
        return nonRowFields;
    }

}
