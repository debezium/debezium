/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * @author David Leibovic
 */
public class ByTableTopicMapper extends TopicMapper {

    public String getTopicName() {
        return topicPrefix + table.id().toString();
    }

    public void enhanceKeySchema(SchemaBuilder keySchemaBuilder) {
        // do nothing ...
    }

    public void addNonRowFieldsToKey(Schema schema, Struct rowBasedKey) {
        // do nothing ...
    }

}
