/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.relational.Table;

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
     * Get the schema of the keys for all messages produced from the table.
     *
     * @param keySchemaBuilder the {@link SchemaBuilder} for the key, pre-populated with the table's primary/unique key
     */
    abstract public void enhanceKeySchema(SchemaBuilder keySchemaBuilder);

    /**
     * Get the key for the row defined by the specified
     *
     * @param schema the schema for the key; never null
     * @param rowBasedKey the {@link Struct} for the key whose row-based fields have already been set; never null
     */
    abstract public void addNonRowFieldsToKey(Schema schema, Struct rowBasedKey);

}
