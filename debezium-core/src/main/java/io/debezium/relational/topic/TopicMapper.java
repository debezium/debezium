/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.relational.Table;

import java.util.Map;

/**
 * @author David Leibovic
 */
public abstract class TopicMapper {

    protected String topicPrefix;
    protected Table table;

    public TopicMapper setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
        return this;
    }

    public TopicMapper setTable(Table table) {
        this.table = table;
        return this;
    }

    /**
     * Get the name of the topic given for the table.
     *
     * @return the topic name; may be null if this strategy could not be applied
     */
    abstract public String getTopicName();

    /**
     * Depending on your TopicMapper implementation and which rows in a database may occupy the same topic,
     * it may be necessary to enhance the key schema for the events to ensure each distinct record in the topic
     * has a unique key.
     *
     * @param keySchemaBuilder the {@link SchemaBuilder} for the key, pre-populated with the table's primary/unique key
     */
    abstract public void enhanceKeySchema(SchemaBuilder keySchemaBuilder);

    /**
     * Get the extra key-value pairs necessary to add to the event's key.
     * @param schema the schema for the key; never null
     * @return the extra key-value pairs, or null if none are necessary.
     */
    abstract public Map<String, Object> getNonRowFieldsToAddToKey(Schema schema);

}
