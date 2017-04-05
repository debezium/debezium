/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import io.debezium.config.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.relational.Table;

import java.util.Map;

/**
 * @author David Leibovic
 */
public class ByTableTopicMapper extends TopicMapper {

    public Field.Set configFields() {
        return null;
    }

    public String getTopicName(String topicPrefix, Table table) {
        return topicPrefix + table.id().toString();
    }

    public void enhanceKeySchema(SchemaBuilder keySchemaBuilder) {
        // do nothing ...
    }

    public Map<String, Object> getNonRowFieldsToAddToKey(Schema schema, String topicPrefix, Table table) {
        // do nothing ...
        return null;
    }

}
