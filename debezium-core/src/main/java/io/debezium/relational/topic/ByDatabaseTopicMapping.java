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
import io.debezium.relational.TableId;

/**
 * @author Randall Hauch
 *
 */
public class ByDatabaseTopicMapping implements TopicMappingProvider {

    @Override
    public TopicMapping getMapper(String prefix, Table table) {
        return new ByDatabaseMapper(prefix,table.id());
    }
    
    protected static class ByDatabaseMapper implements TopicMapping {
        private final String topicName;
        private final String tableName;
        private final String keySchemaName;
        private final String valueSchemaName;
        protected ByDatabaseMapper(String prefix, TableId tableId) {
            this.topicName = prefix + tableId.catalog();
            this.tableName = tableId.table();
            this.keySchemaName = prefix + tableId.toString() + ".Key";
            this.valueSchemaName = prefix + tableId.toString() + ".Value";
        }
        
        @Override
        public String getTopicName() {
            return topicName;
        }
        
        @Override
        public String getKeySchemaName() {
            return keySchemaName;
        }
        
        @Override
        public String getValueSchemaName() {
            return valueSchemaName;
        }
        
        @Override
        public void enhanceKeySchema(SchemaBuilder keySchemaBuilder) {
            // Just add the table name as a field ...
            keySchemaBuilder.field("tableName", Schema.STRING_SCHEMA);
        }
        
        @Override
        public void addNonRowFieldsToKey(Schema schema, Struct rowBasedKey) {
            // Just add the table name as a field ...
            rowBasedKey.put("tableName", tableName);
        }
    }

}
