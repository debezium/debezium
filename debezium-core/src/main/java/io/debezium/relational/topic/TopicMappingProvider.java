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
 * @author Randall Hauch
 *
 */
public interface TopicMappingProvider {

    /**
     * A mapping strategy for a table.
     */
    public static interface TopicMapping {
        /**
         * Get the name of the topic given for the table.
         * 
         * @return the topic name; may be null if this strategy could not be applied
         */
        String getTopicName();

        /**
         * Get the name of the schema for the key.
         * 
         * @return the key's schema name; may not be null
         */
        default String getKeySchemaName() {
            return getTopicName() + ".Key";
        }

        /**
         * Get the name of the schema for the key.
         * 
         * @return the key's schema name; may not be null
         */
        default String getValueSchemaName() {
            return getTopicName() + ".Value";
        }

        /**
         * Get the schema of the keys for all messages produced from the table.
         * 
         * @param keySchemaBuilder the {@link SchemaBuilder} for the key, pre-populated with the table's primary/unique key
         */
        void enhanceKeySchema(SchemaBuilder keySchemaBuilder);

        /**
         * Get the key for the row defined by the specified
         * 
         * @param schema the schema for the key; never null
         * @param rowBasedKey the {@link Struct} for the key whose row-based fields have already been set; never null
         */
        void addNonRowFieldsToKey(Schema schema, Struct rowBasedKey);
    }

    /**
     * Get the name of the topic given the specified topic name prefix, database name, and table name.
     * 
     * @param prefix the topic name prefix; may not be null
     * @param table the table definition; may not be null
     * @return the topic name; may be null if this strategy could not be applied
     */
    TopicMapping getMapper(String prefix, Table table);

}
